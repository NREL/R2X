"""Compilation of polars function that we use in the parsing process."""

from datetime import datetime
from typing import Any

import polars as pl
from loguru import logger
from polars.lazyframe import LazyFrame

from r2x.parser.plexos_utils import DATAFILE_COLUMNS


def pl_filter_year(
    data: pl.DataFrame, year: int | None = None, year_columns: list[str] = ["t", "year"], **kwargs
) -> pl.DataFrame:
    """Filter the DataFrame by a specific year.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to filter.
    year : int | None, optional
        The year to filter by, default is None.
    year_columns : list[str], optional
        The columns to check for year filtering, by default ['t', 'year'].
    **kwargs : dict, optional
        Additional arguments, can contain 'solve_year' to override the year.

    Returns
    -------
    pl.DataFrame
        The filtered DataFrame.

    Raises
    ------
    KeyError
        If more than one column is identified as year.
    """
    if year is None and kwargs.get("solve_year"):
        year = kwargs["solve_year"]

    if year is None:
        return data

    matching_names = list(set(year_columns).intersection(data.collect_schema()))
    if not matching_names:
        return data

    if len(matching_names) > 1:
        raise KeyError(f"More than one column identified as year. {matching_names=}")
    logger.trace("Filtering data for year {}", year)
    return data.filter(pl.col(matching_names[0]) == year)


def pl_remove_duplicates(data: pl.DataFrame, columns: DATAFILE_COLUMNS | list[str]) -> pl.DataFrame:
    """Remove duplicate rows from the DataFrame based on certain columns.

    Parameters
    ----------
    data : pl.DataFrame
        The DataFrame from which to remove duplicates.
    columns : DATAFILE_COLUMNS | list[str]
        The column type enumeration or list of strings used to determine columns for duplication check.

    Returns
    -------
    pl.DataFrame
        DataFrame without duplicate rows.
    """
    columns = columns.value if isinstance(columns, DATAFILE_COLUMNS) else columns
    columns_to_check = [
        col
        for col in columns
        if col in ["name", "pattern", "year", "DateTime", "month", "day", "period", "hour"]
    ]

    if not data.filter(data.select(columns_to_check).is_duplicated()).is_empty():
        logger.warning("File has duplicated rows. Removing duplicates.")
        data = data.unique(subset=columns_to_check).sort(pl.all())

    return data


def pl_lowercase(data: pl.DataFrame, **kwargs):
    """Convert all string columns to lowercase.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame with columns to be lowercased.
    **kwargs : dict, optional
        Additional arguments.

    Returns
    -------
    pl.DataFrame
        The DataFrame with lowercase string columns.
    """
    logger.trace("Lowercase columns: {}", data.collect_schema().names())
    result = data.with_columns(pl.col(pl.String).str.to_lowercase()).rename(
        {column: column.lower() for column in data.collect_schema().names()}
    )
    logger.trace("New columns: {}", data.collect_schema().names())
    return result


def pl_rename(
    data: pl.DataFrame,
    column_mapping: dict[str, str] | None = None,
    **kwargs,
):
    """Rename columns in the DataFrame using a provided column mapping.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to rename columns.
    column_mapping : dict[str, str] | None, optional
        Mapping of original column names to new column names, by default None.
    **kwargs : dict, optional
        Additional arguments.

    Returns
    -------
    pl.DataFrame
        The DataFrame with renamed columns.
    """
    if not column_mapping:
        logger.trace("No column mapping provided")
        return data
    logger.trace("Apply column mapping {}", column_mapping)
    return data.select(pl.col(column_mapping.keys())).select(
        pl.all().name.map(lambda col_name: column_mapping.get(col_name, col_name))
    )


def pl_left_multi_join(l_df: pl.LazyFrame, *r_dfs: pl.DataFrame, **kwargs):
    """Perform a left join on multiple DataFrames.

    Parameters
    ----------
    l_df : pl.LazyFrame
        The left DataFrame for joining.
    *r_dfs : pl.LazyFrame
        The right DataFrames to join.
    **kwargs : dict, optional
        Additional arguments.

    Returns
    -------
    pl.DataFrame
        The resulting DataFrame after the join.

    Raises
    ------
    AssertionError
        If the merge results in fewer rows than the left DataFrame.
    """
    original_keys = set(l_df.collect_schema().names())
    output_df = l_df
    if not r_dfs:
        return l_df

    current_keys: set[Any] = set()
    for r_df in r_dfs:
        current_keys = set(r_df.collect_schema().names())
        current_keys = original_keys.intersection(current_keys)
        if isinstance(r_df, LazyFrame):
            r_df = r_df.collect()
        output_df = output_df.join(r_df, on=list(current_keys), how="left", coalesce=True)

    if isinstance(output_df, pl.LazyFrame):
        output_df = output_df.collect()

    l_df_shape = l_df.collect().shape[0] if isinstance(l_df, pl.LazyFrame) else l_df.shape[0]
    assert output_df.shape[0] == l_df_shape, (
        f"Merge resulted in less rows. Check the shared keys. {original_keys=} vs {current_keys=}"
    )
    return output_df


def pl_create_date_range(year: int, interval: str = "1h"):
    """Create a DataFrame with a date range for the given year.

    Parameters
    ----------
    year : int
        The year to create the date range for.
    interval : str, optional
        Time interval for the date range, by default '1h'.

    Returns
    -------
    pl.DataFrame
        DataFrame with datetime, year, month, day, and hour columns.
    """
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
