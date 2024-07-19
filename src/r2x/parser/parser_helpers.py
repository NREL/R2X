# ruff: noqa
"""Set of helper functions for parsers."""

from loguru import logger
import polars as pl


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
