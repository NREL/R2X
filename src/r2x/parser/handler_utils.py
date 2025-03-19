"""Helper functions for base parser."""

import os
from pathlib import Path

import h5py
import pandas as pd
import polars as pl
from loguru import logger

from .polars_helpers import pl_lowercase


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

    if kwargs.get("keep_case") is None:
        data_file = pl_lowercase(data_file)

    return data_file


def h5_handler(fpath, parser_class: str, **kwargs) -> pl.LazyFrame:
    """Parse H5 files and return a Polars DataFrame.

    Currently, the only exception we handle is for ReEDS since it is formatting differently. If new parsers or
    existing parsers required h5 reading, we will need to add new handlers.

    Parameters
    ----------
    fpath : str
        The file path of the CSV file to read.
    csv_file_encoding : str, optional
        The encoding format of the CSV file, by default "utf8".

    Raises
    ------
    NotImplementedError
        Raised if a non supported parser request a h5 file.
    """
    match parser_class:
        case "ReEDSParser":
            with h5py.File(fpath, "r") as f:
                logger.info(f"Parsing h5 File: {fpath}")
                # unique structure for recf versus load
                if os.path.basename(fpath) == "recf.h5":
                    index_datetime = (
                        f["index_datetime"][:] if "index_datetime" in f.keys() else f["index_0"][:]
                    )
                    pd_df = pd.DataFrame(
                        f["data"],
                        index=pd.Index(
                            pd.to_datetime(
                                index_datetime.astype(str),
                            ),
                            name="datetime",
                        ),
                        columns=[col.decode("utf-8") for col in f["columns"]],
                    )
                    pd_df = pd_df.reset_index()
                elif os.path.basename(fpath) == "load.h5":
                    # get index data
                    index_year = f["index_year"][:] if "index_year" in f.keys() else f["index_0"][:]
                    index_datetime = (
                        f["index_datetime"][:] if "index_datetime" in f.keys() else f["index_1"][:]
                    )

                    # get data in pandas format
                    pd_df = pd.DataFrame(f["data"], columns=[col.decode("utf-8") for col in f["columns"]])

                    # creating multi level index
                    unique_year_sorted = sorted(set(index_year))
                    unique_datetime_sorted = sorted(set(index_datetime))
                    multilevel_index = pd.MultiIndex.from_product(
                        [unique_year_sorted, unique_datetime_sorted], names=["year", "datetime"]
                    )

                    # setting index then reseting
                    pd_df = pd_df.set_index(multilevel_index).reset_index()

                    # correctly formating datetime col
                    pd_df["datetime"] = pd.to_datetime(pd_df["datetime"].astype(str))
                pl_df = pl.LazyFrame(pd_df)
        case _:
            msg = f"H5 file parsing is not implemented for {parser_class=}."
            raise NotImplementedError(msg)

    if kwargs.get("keep_case") is None:
        pl_df = pl_lowercase(pl_df)
    return pl_df
