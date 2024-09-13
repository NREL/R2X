# ruff: noqa
"""Set of helper functions for parsers."""

from loguru import logger
import polars as pl
import pandas as pd
from datetime import datetime


def pl_filter_year(df, year: int | None = None, year_columns=["t", "year"], **kwargs):
    if year is None and kwargs.get("solve_year"):
        year = kwargs["solve_year"]

    if year is None:
        return df
    matching_names = list(set(year_columns).intersection(df.collect_schema()))
    if not matching_names:
        return df

    if len(matching_names) > 1:
        return KeyError(f"More than one column identified as year. {matching_names=}")
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


def fill_missing_timestamps(data_file: pl.DataFrame, date_time_column: list[str]):
    """Add missing timestamps to data and forward fill nulls"""
    data_file = data_file.with_columns(
        (
            pl.col("year").cast(pl.Int32).cast(pl.Utf8)
            + "-"
            + pl.col("month").cast(pl.Int32).cast(pl.Utf8).str.zfill(2)
            + "-"
            + pl.col("day").cast(pl.Int32).cast(pl.Utf8).str.zfill(2)
            + " "
            + pl.col("hour").cast(pl.Int32).cast(pl.Utf8).str.zfill(2)
            + ":00:00"
        )
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
        .alias("timestamp")
    ).with_columns(pl.col("timestamp").dt.cast_time_unit("ns"))

    data_file = data_file.with_columns(
        pl.col("year").cast(pl.Int32),
        pl.col("month").cast(pl.Int8),
        pl.col("day").cast(pl.Int8),
        pl.col("hour").cast(pl.Int8),
    )

    complete_timestamps = pl.from_pandas(pd.DataFrame({"timestamp": date_time_column}))
    missing_timestamps = complete_timestamps.join(data_file, on="timestamp", how="anti")

    missing_timestamps = missing_timestamps.with_columns(
        pl.col("timestamp").dt.year().alias("year"),
        pl.col("timestamp").dt.month().alias("month"),
        pl.col("timestamp").dt.day().alias("day"),
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.lit(None).alias("value"),
    ).select(["year", "month", "day", "hour", "value", "timestamp"])

    complete_df = pl.concat([data_file, missing_timestamps]).sort("timestamp").fill_null(strategy="forward")
    complete_df.drop_in_place("timestamp")
    return complete_df


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
