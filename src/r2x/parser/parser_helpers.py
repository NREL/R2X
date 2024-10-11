# ruff: noqa
"""Set of helper functions for parsers."""

from enum import Enum
from functools import singledispatch
from os import PathLike
from pathlib import Path, PureWindowsPath
from loguru import logger
import polars as pl
import pandas as pd
from datetime import datetime
import numpy as np
import cvxpy as cp
from typing import Literal, NamedTuple, List

from infrasys.function_data import LinearFunctionData, QuadraticFunctionData, PiecewiseLinearData, XYCoords

PLEXOS_OUTPUT_COLUMNS = ["year", "month", "day", "hour", "value"]


def pl_filter_year(df, year: int | None = None, year_columns=["t", "year"], **kwargs):
    if year is None and kwargs.get("solve_year"):
        year = kwargs["solve_year"]

    if year is None:
        return df
    matching_names = list(set(year_columns).intersection(df.collect_schema()))
    if not matching_names:
        return df

    if len(matching_names) > 1:
        raise KeyError(f"More than one column identified as year. {matching_names=}")
    logger.trace("Filtering data for year {}", year)
    return df.filter(pl.col(matching_names[0]) == year)


def field_filter(property_fields, eligible_fields):
    valid = {k: v for k, v in property_fields.items() if k in eligible_fields if v is not None}
    extra = {k: v for k, v in property_fields.items() if k not in eligible_fields if v is not None}

    return valid, extra


def prepare_ext_field(valid_fields, extra_fields):
    """Cleanses the extra fields by removing any timeseries data"""
    if extra_fields:
        # Implement any filtering of ext_data here
        # logger.debug("Extra fields: {}", extra_fields)
        # remove any non eligible datatypes from extra fields
        eligible_datatypes = [str, int, float, bool]
        extra_fields = {k: v for k, v in extra_fields.items() if type(v) in eligible_datatypes}
        valid_fields["ext"] = extra_fields
    else:
        valid_fields["ext"] = {}
    return valid_fields


def filter_property_dates(system_data: pl.DataFrame, study_year: int):
    """filters query by date_from and date_to"""
    # Convert date_from and date_to to datetime
    system_data = system_data.with_columns(
        [
            pl.col("date_from").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S").cast(pl.Date),
            pl.col("date_to").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S").cast(pl.Date),
        ]
    )

    # Create two new columns year_from and year_to
    system_data = system_data.with_columns(
        [
            pl.col("date_from").dt.year().alias("year_from"),
            pl.col("date_to").dt.year().alias("year_to"),
        ]
    )

    # Remove Property by study year & date_from/to
    date_filter = ((pl.col("date_from").is_null()) | (pl.col("year_from") <= study_year)) & (
        (pl.col("date_to").is_null()) | (pl.col("year_to") >= study_year)
    )
    system_data = system_data.filter(date_filter)
    return system_data


def pl_lowercase(df: pl.DataFrame, **kwargs):
    logger.trace("Lowercase columns: {}", df.collect_schema().names())
    result = df.with_columns(pl.col(pl.String).str.to_lowercase()).rename(
        {column: column.lower() for column in df.collect_schema().names()}
    )
    logger.trace("New columns: {}", df.collect_schema().names())
    return result


def pl_rename(
    df: pl.DataFrame,
    column_mapping: dict[str, str] | None = None,
    default_mapping: dict[str, str] | None = None,
    **kwargs,
):
    if not column_mapping:
        logger.trace("No column mapping provided")
        return df
    logger.trace("Apply column mapping {}", column_mapping)
    return df.select(pl.col(column_mapping.keys())).select(
        pl.all().name.map(lambda col_name: column_mapping.get(col_name, col_name))
    )


def pl_left_multi_join(l_df: pl.LazyFrame, *r_dfs: pl.LazyFrame, **kwargs):
    original_keys = set(l_df.collect_schema().names())
    # output_df = pl.LazyFrame(l_df)
    output_df = l_df
    for r_df in r_dfs:
        current_keys = set(r_df.collect_schema().names())
        current_keys = original_keys.intersection(current_keys)
        output_df = output_df.join(r_df, on=list(current_keys), how="left", coalesce=True)

    output_df = output_df.collect()
    assert (
        output_df.shape[0] == l_df.collect().shape[0]
    ), f"Merge resulted in less rows. Check the shared keys. {original_keys=} vs {current_keys=}"
    return output_df


def handle_leap_year_adjustment(data_file: pl.DataFrame):
    """Duplicate feb 28th to feb 29th for leap years."""
    feb_28 = data_file.slice(1392, 24)
    before_feb_29 = data_file.slice(0, 1416)
    after_feb_29 = data_file.slice(1416, len(data_file) - 1440)
    return pl.concat([before_feb_29, feb_28, after_feb_29])


def fill_missing_timestamps(data_file: pl.DataFrame, hourly_time_index: pl.DataFrame):
    """Add missing timestamps to data and forward fill nulls to complete a year"""

    if "hour" in data_file.columns:
        data_file = data_file.with_columns(
            pl.datetime(pl.col("year"), pl.col("month"), pl.col("day"), pl.col("hour"))
        )
    if len(data_file) <= 13:
        data_file = data_file.with_columns(pl.datetime(pl.col("year"), pl.col("month"), pl.col("day")))
    else:
        data_file = data_file.with_columns(pl.datetime(pl.col("year"), pl.col("month"), pl.col("day")))

    upsample_data = hourly_time_index.join(data_file, on="datetime", how="left")
    upsample_data = upsample_data.fill_null(strategy="forward")
    return upsample_data


def resample_data_to_hourly(data_file: pl.DataFrame):
    """Resample data to hourly frequency from 30 minute data."""
    data_file = data_file.with_columns((pl.col("hour") % 48).alias("hour"))
    data_file = (
        data_file.with_columns(
            (
                pl.datetime(
                    data_file["year"],
                    data_file["month"],
                    data_file["day"],
                    hour=data_file["hour"] // 2,
                    minute=(data_file["hour"] % 2) * 30,
                )
            ).alias("timestamp")
        )
        .sort("timestamp")
        .filter(pl.col("timestamp").is_not_null())
    )

    return (
        data_file.group_by_dynamic("timestamp", every="1h")
        .agg([pl.col("value").mean().alias("value")])
        .with_columns(
            pl.col("timestamp").dt.year().alias("year"),
            pl.col("timestamp").dt.month().alias("month"),
            pl.col("timestamp").dt.day().alias("day"),
            pl.col("timestamp").dt.hour().alias("hour"),
            pl.col("value").alias("value"),
        )
        .select(["year", "month", "day", "hour", "value"])
    )


def construct_pwl_from_quadtratic(fn, mapped_records, num_tranches=6):
    """
    Given function data of quadratic curve, construct piecewise linear curve with num_tranches tranches.
    """
    assert isinstance(fn, QuadraticFunctionData), "Input function data must be of type QuadraticFunctionData"
    if isinstance(num_tranches, str):
        num_tranches = int(num_tranches)

    a = fn.quadratic_term
    b = fn.proportional_term
    c = fn.constant_term
    x_min = mapped_records["min_rated_capacity"].magnitude
    x_max = mapped_records["active_power_limits_max"].magnitude

    # Use evenly spaced X values for the tranches
    # Future iteration should accept custom X values for Bid Cost Markup
    x_vals, y_vals = optimize_pwl(a, b, c, x_min, x_max, num_tranches)

    pwl_fn = PiecewiseLinearData(points=[XYCoords(x, y) for x, y in zip(x_vals, y_vals)])

    # # Plot the results
    # import matplotlib.pyplot as plt
    # plt.figure(figsize=(8, 6))
    # # Plot the quadratic function
    # x_plot = np.linspace(x_min, x_max, 100)
    # y_quad = a * x_plot**2 + b * x_plot + c
    # plt.plot(x_plot, y_quad, label='Quadratic Function', color='blue', lw=2)
    # plt.plot(x_vals, y_vals, label='Optimized PWL Function', color='red', linestyle='--', marker='o')
    # plt.xlabel('MW')
    # plt.ylabel('MMbtu/hr')
    # plt.title(f'Generator {mapped_records.get("name")} PWL to Quadratic Cost Functions')
    # plt.legend()
    # plt.grid(True)
    # plt.savefig(f'pwl_curves/pwl_optimization_{mapped_records.get("name")}.png')
    # plt.close()

    return pwl_fn


def optimize_pwl(a, b, c, min, max, n_tranches=6):
    y = cp.Variable(n_tranches)
    x_target = np.linspace(min, max, n_tranches)
    y_quad = a * x_target**2 + b * x_target + c
    mse = cp.sum_squares(y - y_quad)

    # Monotonicity constraints
    constraints = [y[i] <= y[i + 1] for i in range(n_tranches - 1)]

    objective = cp.Minimize(mse)
    problem = cp.Problem(objective, constraints)
    problem.solve()

    return x_target, y.value


def bid_cost_mark_up(fn, mapped_records):
    # TODO(ktehranchi): Implement bid-cost markup
    # First we need to convert whichever type of function we have to a piecewise linear function
    # This PWL function must have X values definted at the mark-up points
    # We can easily modify the mark-up prices by changing the Y values of the PWL function
    # Issue right now is we need to do this for time-varying data but market bid cost isnt implemented
    pass


def csv_handler(fpath: Path, csv_file_encoding="utf8", **kwargs) -> pl.DataFrame:
    """Parse CSV files and return a Polars DataFrame with all column names in lowercase.

    Parameters
    ----------
    fpath : str
        The file path of the CSV file to read.
    csv_file_encoding : str, optional
        The encoding format of the CSV file, by default "utf8".
    **kwargs : dict, optional
        Additional keyword arguments passed to the `pl.read_csv` function.

    Returns
    -------
    pl.DataFrame or None
        The parsed CSV file as a Polars DataFrame with lowercase column names if successful,
        or `None` if the file was not found.

    Raises
    ------
    pl.exceptions.ComputeError
        Raised if there are issues with the data types in the CSV file.
    FileNotFoundError
        Raised if the file is not found.

    See Also
    --------
    pl_lowercase : Function to convert all column names of a Polars DataFrame to lowercase.

    Example
    -------
    >>> df = csv_handler("data/example.csv")
    >>> print(df)
    shape: (2, 3)
    ┌─────┬────────┬──────┐
    │ id  │ name   │ age  │
    │ --- │ ---    │ ---  │
    │ i64 │ str    │ i64  │
    ╞═════╪════════╪══════╡
    │ 1   │ Alice  │ 30   │
    │ 2   │ Bob    │ 24   │
    └─────┴────────┴──────┘
    """
    logger.trace("Attempting reading file {}", fpath)
    logger.trace("Parsing file {}", fpath)
    try:
        data_file = pl.read_csv(
            fpath.as_posix(),
            infer_schema_length=10_000_000,
            encoding=csv_file_encoding,
        )
    except FileNotFoundError:
        msg = f"File {fpath} not found."
        logger.error(msg)
        raise FileNotFoundError(msg)
    except pl.exceptions.PolarsError:
        logger.warning("File {} could not be parse due to dtype problems. See error.", fpath)
        raise

    data_file = pl_lowercase(data_file)

    return data_file


class DATA_FILE_COLUMNS(Enum):
    BASIC = ["name", "value"]
    TS_NPV = ["name", "pattern", "value"]
    TS_NYV = ["name", "year", "value"]
    TS_NDV = ["name", "DateTime", "value"]
    TS_YMDP = ["year", "month", "day", "period"]
    TS_YMDPV = ["year", "month", "day", "period", "value"]
    TS_NYMDV = ["name", "year", "month", "day", "value"]
    TS_YM = ["year", "month"]
    TS_MDP = ["month", "day", "period"]
    TS_NYMDH = [
        "name",
        "year",
        "month",
        "day",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "23",
        "24",
    ]
    TS_NMDH = [
        "name",
        "month",
        "day",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "23",
        "24",
    ]
    TS_NM = [
        "name",
        "m01",
        "m02",
        "m03",
        "m04",
        "m05",
        "m06",
        "m07",
        "m08",
        "m09",
        "m10",
        "m11",
        "m12",
    ]


def get_column_enum(columns: List[str]) -> DATA_FILE_COLUMNS | None:
    """Identify the corresponding PropertyColumns enum based on the given columns.

    Parameters
    ----------
    columns : List[str]
        The list of columns to inspect.

    Returns
    -------
    Optional[PropertyColumns]
        The corresponding enum if a match is found; otherwise, None.
    """
    for property_type in DATA_FILE_COLUMNS:
        if set(property_type.value) == set(columns):
            return property_type
    return None


def create_date_range(year: int, interval: str = "1h"):
    dt = pl.datetime_range(
        datetime(year, 1, 1), datetime(year + 1, 1, 1), interval, eager=True, closed="left"
    ).alias("datetime")
    date_df = pl.DataFrame({"datetime": dt})
    date_df = date_df.with_columns(
        [
            date_df["datetime"].dt.year().alias("year").cast(pl.Int64),
            date_df["datetime"].dt.month().alias("month").cast(pl.Int64),
            date_df["datetime"].dt.day().alias("day").cast(pl.Int64),
            date_df["datetime"].dt.hour().alias("hour").cast(pl.Int64),
        ]
    )
    return date_df


def parse_data_file(column_type: DATA_FILE_COLUMNS, data_file):
    match column_type:
        case column_type.BASIC:
            data_file = parse_basic(data_file)
        case column_type.TS_NYMDV:
            data_file = parse_ts_nymdv(data_file)
        case column_type.TS_NMDH:
            data_file = parse_ts_nmdh(data_file)
        case column_type.TS_NYMDH:
            data_file = parse_ts_nymdh(data_file)
        case _:
            raise NotImplementedError
    return data_file


def parse_basic(data_file):
    data_file = data_file.with_columns(
        month=pl.col("pattern").str.extract(r"(\d{2})$").cast(pl.Int8)
    )  # If other patterns exist, this will need to change.
    data_file = date_df.join(data_file.select("month", "value"), on="month", how="inner").select(
        output_columns
    )
    return data_file


def parse_ts_nymdv(data_file):
    return data_file


def parse_ts_nmdh(data_file):
    data_file = data_file.melt(id_vars=["name", "month", "day"], variable_name="hour")
    data_file = data_file.with_columns(pl.col("hour").cast(pl.Int8))
    return data_file


def parse_ts_nymdh(data_file):
    data_file = data_file.melt(id_vars=["name", "year", "month", "day"], variable_name="hour")
    data_file = data_file.with_columns(pl.col("hour").cast(pl.Int8))
    return data_file
