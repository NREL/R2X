# ruff: noqa
"""Set of helper functions for parsers."""

from loguru import logger
import polars as pl
import pandas as pd


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


def handle_leap_year_adjustment(data_file):
    # Adjust for non-leap year with leap-year data
    feb_28 = data_file.slice(1392, 24)
    before_feb_29 = data_file.slice(0, 1416)
    after_feb_29 = data_file.slice(1416, len(data_file) - 1440)
    return pl.concat([before_feb_29, feb_28, after_feb_29])


def fill_missing_timestamps(data_file, date_time_column):
    # Add missing timestamps and fill nulls
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

    complete_timestamps_df = pl.from_pandas(pd.DataFrame({"timestamp": date_time_column}))
    missing_timestamps_df = complete_timestamps_df.join(data_file, on="timestamp", how="anti")

    missing_timestamps_df = missing_timestamps_df.with_columns(
        pl.col("timestamp").dt.year().alias("year"),
        pl.col("timestamp").dt.month().alias("month"),
        pl.col("timestamp").dt.day().alias("day"),
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.lit(None).alias("value"),
    ).select(["year", "month", "day", "hour", "value", "timestamp"])

    complete_df = (
        pl.concat([data_file, missing_timestamps_df]).sort("timestamp").fill_null(strategy="forward")
    )
    complete_df.drop_in_place("timestamp")
    return complete_df


def resample_data_to_hourly(data_file):
    # Resample data to hourly frequency
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
