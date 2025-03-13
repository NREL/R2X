"""Compilation of polars function that we use in the parsing process."""

from datetime import datetime
from typing import Any

import polars as pl
from loguru import logger
from polars.lazyframe import LazyFrame

from r2x.exceptions import R2XParserError
from r2x.parser.plexos_utils import DATAFILE_COLUMNS


def pl_filter_by_year(
    data: pl.DataFrame | pl.LazyFrame, year: int | None = None, year_column: str = "year", **kwargs
) -> pl.DataFrame:
    """Filter the DataFrame by a specific year.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame to filter.
    year : int | None, optional
        The year to filter by, default is None.
    year_column : str, optional
        The columns to filter by year
    **kwargs : dict, optional
        Additional arguments, can contain 'solve_year' to override the year.

    Returns
    -------
    pl.DataFrame
        The filtered DataFrame.

    Raises
    ------
    R2XParserError
        If the year provided is not in the set of available years.

    See Also
    --------
    pl_filter_by_year
    """
    if isinstance(data, pl.DataFrame) and data.is_empty():
        return data

    if year_column not in data.collect_schema():
        return data

    if kwargs.get("solve_year"):
        year = kwargs["solve_year"]

    filter_data = data.clone()

    available_years = filter_data.select(pl.col(year_column)).unique()

    if isinstance(data, pl.LazyFrame):
        available_years = available_years.collect()

    assert len(available_years) >= 1
    assert isinstance(available_years, pl.DataFrame)

    if year not in available_years[year_column].to_list():
        logger.debug("{} not in available years {}. Returning unfiltered file.", year, available_years)
        return data

    return filter_data.filter(pl.col(year_column) == year)


def pl_filter_by_weather_year(
    data: pl.LazyFrame,
    weather_year: int | None = None,
    year_column: str = "datetime",
    **kwargs,
) -> pl.DataFrame:
    """Filter the DataFrame by a specific year.

    This function is tailored for datasets that are big, hecen why we use a LazyFrame. Examples of this
    dataframes are h5 files that were parsed to polars or any other big csv file.

    Parameters
    ----------
    data : pl.LazyFrame
        The DataFrame to filter.
    weather_year : int | None, optional
        The year to filter by, default is None.
    year_column : Literal[str] = 'year', optional
        The columns to filter by year
    **kwargs : dict, optional
        Additional arguments

    Returns
    -------
    pl.LazyFrame
        The filtered DataFrame.

    Raises
    ------
    R2XParserError
        If the year provided is not in the set of available years.

    See Also
    --------
    pl_filter_by_year
    """
    if not isinstance(data, pl.LazyFrame):
        return data

    if year_column not in data.collect_schema():
        return data

    available_years = data.select(pl.col(year_column).dt.year()).unique().collect()[year_column].to_list()
    assert len(available_years) >= 1

    if weather_year not in available_years:
        msg = f"{weather_year=} not in dataset. Select one of the available years {available_years=}"
        raise R2XParserError(msg)

    return data.filter(pl.col(year_column).dt.year() == weather_year)


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
