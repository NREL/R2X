"""Compilation of functions used on the PLEXOS parser."""

# ruff: noqa
import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import polars as pl
import numpy as np
from loguru import logger
from infrasys import SingleTimeSeries

PLEXOS_ACTION_MAP = {
    "×": np.multiply,  # noqa
    "+": np.add,
    "-": np.subtract,
    "/": np.divide,
    "=": lambda x, y: y,
}


class DATAFILE_COLUMNS(Enum):
    """Enum of possible Data file columns in Plexos."""

    NV = ["name", "value"]
    Y = ["year"]
    PV = ["pattern", "value"]
    TS_NPV = ["name", "pattern", "value"]
    TS_NYV = ["name", "year", "value"]
    TS_NDV = ["name", "DateTime", "value"]
    TS_YMDP = ["year", "month", "day", "period"]
    TS_YMDPV = ["year", "month", "day", "period", "value"]
    TS_NYMDV = ["name", "year", "month", "day", "value"]
    TS_NYMDPV = ["name", "year", "month", "day", "period", "value"]
    TS_YM = ["year", "month"]
    TS_MDP = ["month", "day", "period"]
    TS_NMDP = ["name", "month", "day", "period"]
    TS_YMDH = [
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


def get_column_enum(columns: list[str]) -> DATAFILE_COLUMNS | None:
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
    best_match = None
    max_columns_matched = 0

    columns_set = set(columns)
    for property_type in DATAFILE_COLUMNS:
        enum_columns_set = set(property_type.value)

        if enum_columns_set.issubset(columns_set):
            if len(enum_columns_set) > max_columns_matched:
                best_match = property_type
                max_columns_matched = len(enum_columns_set)

    msg = "Matched columns = {} to property_type = {}"
    logger.trace(msg, columns, best_match)
    return best_match


def filter_property_dates(system_data: pl.DataFrame, study_year: int):
    """Filter query by date_from and date_to."""
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


def parse_data_file(column_type: DATAFILE_COLUMNS, data_file):
    match column_type:
        case column_type.Y:
            data_file = parse_y(data_file)
        case column_type.PV:
            data_file = parse_pv(data_file)
        case column_type.NV:
            data_file = parse_nv(data_file)
        case column_type.TS_MDP:
            data_file = parse_ts_mdp(data_file)
        case column_type.TS_NMDP:
            data_file = parse_ts_nmdp(data_file)
        case column_type.TS_NM:
            data_file = parse_ts_nm(data_file)
        case column_type.TS_YM:
            data_file = parse_ts_ym(data_file)
        case column_type.TS_YMDPV:
            data_file = parse_ts_ymdpv(data_file)
        case column_type.TS_YMDH:
            data_file = parse_ts_ymdh(data_file)
        case column_type.TS_NYMDV:
            data_file = parse_ts_nymdv(data_file)
        case column_type.TS_NYMDPV:
            data_file = parse_ts_nymdpv(data_file)
        case column_type.TS_NMDH:
            data_file = parse_ts_nmdh(data_file)
        case column_type.TS_NYMDH:
            data_file = parse_ts_nymdh(data_file)
        case _:
            msg = f"Time series format {column_type.value} not yet supported."
            raise NotImplementedError(msg)
    return data_file


def parse_y(data_file):
    data_file = data_file.melt(id_vars="year", variable_name="name")
    return data_file


def parse_pv(data_file):
    data_file = data_file.with_columns(
        month=pl.col("pattern").str.extract(r"(\d{2})$").cast(pl.Int8)
    )  # If other patterns exist, this will need to change.
    return data_file


def parse_ts_nm(data_file):
    data_file = data_file.melt(id_vars="name", variable_name="month")
    data_file = data_file.with_columns(month=pl.col("month").str.extract(r"(\d{2})$").cast(pl.Int8))
    return data_file


def parse_nv(data_file):
    return data_file


def parse_ts_nymdv(data_file):
    return data_file


def parse_ts_nymdpv(data_file):
    data_file = data_file.with_columns(hour=pl.col("period"))
    data_file = data_file.with_columns(pl.col("hour").cast(pl.Int8))
    return data_file


def parse_ts_nmdh(data_file):
    data_file = data_file.melt(id_vars=["name", "month", "day"], variable_name="hour")
    return data_file


def parse_ts_ym(data_file):
    data_file = data_file.melt(id_vars=["year", "month"], variable_name="name")
    return data_file


def parse_ts_ymdpv(data_file):
    data_file = data_file.with_columns(hour=pl.col("period"))
    data_file = data_file.with_columns(pl.col("hour").cast(pl.Int8))
    return data_file


def parse_ts_nmdp(data_file):
    data_file = data_file.with_columns(hour=pl.col("period"))
    data_file = data_file.with_columns(pl.col("hour").cast(pl.Int8))
    return data_file


def parse_ts_mdp(data_file):
    data_file = data_file.melt(id_vars=["month", "day", "period"], variable_name="name")
    # Valid PLEXOS period ranges:
    # - 1-24 periods is 1 hour data for full day
    # - 1-48 periods is 30 minute data for full day
    # - 1-289 periods is 5 minute data for full day
    if not all(data_file["period"].unique().is_in(range(0, 289))):
        msg = (
            "Periods should range 1-24 (if hourly), 1-48 (if 30min) or 1-289 (if 5min).",
            f"Found {max(data_file.unique())}",
        )
        raise NotImplementedError(msg)
    data_file = data_file.with_columns(hour=pl.col("period"))
    data_file = data_file.with_columns(pl.col("hour").cast(pl.Int8))
    return data_file


def parse_ts_ymdh(data_file):
    data_file = data_file.melt(id_vars=["year", "month", "day"], variable_name="hour")
    data_file = data_file.with_columns(pl.col("hour").cast(pl.Int8))
    return data_file


def parse_ts_nymdh(data_file):
    data_file = data_file.melt(id_vars=["name", "year", "month", "day"], variable_name="hour")
    data_file = data_file.with_columns(pl.col("hour").cast(pl.Int8))
    return data_file


def parse_patterns(key):
    ranges = key.split(";")
    month_list = []
    for rng in ranges:
        # Match ranges like 'M5-10' and single months like 'M1'
        match = re.match(r"M(\d+)(?:-(\d+))?", rng)
        if match:
            start_month = int(match.group(1))
            end_month = int(match.group(2)) if match.group(2) else start_month
            # Generate the list of months from the range
            month_list.extend(range(start_month, end_month + 1))
    return month_list


def time_slice_handler(
    records: list[dict[str, Any]],
    hourly_time_index: pl.DataFrame,
    pattern_key: str = "pattern",
) -> np.ndarray:
    """Deconstruct a dict of time slices and return a NumPy array representing a time series.

    Parameters
    ----------
    records : List[dict[str, Any]]
        A list of dictionaries containing timeslice records.
    hourly_time_index : pl.DataFrame
        Dataframe containing a 'datetime' column for hourly time index.
    pattern_key : str, optional
        Key used to extract patterns from records (default is 'pattern').

    Returns
    -------
    np.ndarray
        A NumPy array representing the time series based on the input patterns.

    Raises
    ------
    TypeError
        If records are not a list or hourly_time_index is not a polars DataFrame.
    ValueError
        If the 'datetime' column is missing from hourly_time_index.
    NotImplementedError
        If records do not contain valid month patterns starting with 'M'.

    Examples
    --------
    >>> records = [{"pattern": "M1-2", "value": np.array([5])}, {"pattern": "M3", "value": np.array([10])}]
    >>> datetime_values = [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1)]
    >>> hourly_time_index = pl.DataFrame({"datetime": datetime_values})
    >>> time_slice_handler(records, hourly_time_index)
    array([5., 5., 10.])

    >>> hourly_time_index = pl.DataFrame({"wrong_column": [1, 2, 3]})  # Raises ValueError
    """
    if not isinstance(hourly_time_index, pl.DataFrame):
        raise TypeError(
            f"Expected 'hourly_time_index' to be a polars DataFrame, got {type(hourly_time_index).__name__}"
        )
    if not all(isinstance(record, dict) for record in records):
        raise TypeError("All records must be dictionaries")

    if not all(record[pattern_key].startswith("M") for record in records if pattern_key in record):
        raise NotImplementedError("All records must contain valid month patterns starting with 'M'")

    if "datetime" not in hourly_time_index.columns:
        raise ValueError("Hourly time index does not have 'datetime' column")

    hourly_time_index = hourly_time_index["datetime"]

    months = np.array([dt.month for dt in hourly_time_index])
    month_datetime_series = np.zeros(len(hourly_time_index), dtype=float)

    for record in records:
        months_in_key = parse_patterns(record[pattern_key])
        for month in months_in_key:
            month_datetime_series[months == month] = record["value"].magnitude

    return month_datetime_series