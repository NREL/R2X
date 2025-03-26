"""Useful check functions for upgrader."""

from pathlib import Path

import h5py
import pandas as pd
from loguru import logger


def check_if_h5_is_pandas_format(fpath: Path) -> bool:
    """Check if an h5 file is formatted using pandas."""
    try:
        _ = pd.read_hdf(fpath)
        return True
    except ValueError:
        return False


def check_if_h5_has_correct_index_names(fpath: Path) -> bool:
    """Check if has correct index names."""
    required_columns_1 = {"index_datetime", "index_year", "columns", "index_names", "data"}
    required_columns_2 = {"index_datetime", "columns", "index_names", "data"}
    with h5py.File(fpath, "r") as f:
        file_keys = set(f.keys())
    return required_columns_1.issubset(file_keys) or required_columns_2.issubset(file_keys)


def check_if_columm_is_datetime(fpath: Path, column: str = "index_datetime") -> bool:
    """Check if a h5 file has a datetime column."""
    with h5py.File(fpath, "r") as f:
        file_keys = set(f.keys())
        if column not in file_keys:
            logger.debug("`index_datetime` column not found on {}", fpath)
            return False
        first_row = f[column][:1]

    try:
        _ = pd.to_datetime(first_row.astype("U"))
    except:  # noqa: E722
        return False
    return True
