"""Helper functions for upgrader."""

import datetime
import inspect
from collections.abc import Callable
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
from loguru import logger

from r2x.utils import get_timeindex, validate_string


def add_datetime_index(fpath, datetime_column: str = "index_datetime") -> bool:
    """Add datetime index for h5 file w/o it."""
    time_index = get_timeindex()
    time_index = time_index.to_series().apply(datetime.datetime.isoformat).reset_index(drop=True)
    time_index = np.char.encode(time_index.to_numpy().astype(str), "utf-8")

    with h5py.File(fpath, "r+") as f:
        year_multiplier = len(set(f["index_year"][:])) if "index_year" in f.keys() else 1
        time_index = np.tile(time_index, year_multiplier)
        del f[datetime_column]
        f.create_dataset(datetime_column, data=time_index, dtype="S30")
    return True


def rename_index_names_from_h5(fpath) -> bool:
    """Rename index for h5 file."""
    multi_index = {"index_0", "index_1"}
    single_index = {"index_0"}

    with h5py.File(fpath, "r+") as f:
        keys = set(f.keys())

        match keys:
            case set() if multi_index.issubset(keys):
                column_mapping = {"index_0": "index_year", "index_1": "index_datetime"}
            case set() if single_index.issubset(keys):
                column_mapping = {"index_0": "index_datetime"}
        for old_name, new_name in column_mapping.items():
            f.move(old_name, new_name)
    return True


def pandas_to_h5py(pandas_dataframe: pd.DataFrame, output_fpath: Path, compression_opts: int = 4) -> bool:
    """Convert a pandas dataframe to an h5py."""
    logger.debug("Converting pandas style H5 {} to h5py compatible", output_fpath)
    timeindex = get_timeindex()
    timeindex = timeindex.to_series().apply(datetime.datetime.isoformat).reset_index(drop=True)
    with h5py.File(output_fpath, "w") as f:
        match pandas_dataframe.index:
            case pd.MultiIndex():
                for level in pandas_dataframe.index.levels:
                    if (len(level) == 7 * 8760) and (isinstance(level.to_numpy()[0], np.int64)):
                        assert len(level) == len(timeindex), (
                            f"H5 file {output_fpath} has more weather year data."
                        )
                        f.create_dataset("index_datetime", data=timeindex.str.encode("utf-8"), dtype="S30")
                    else:
                        f.create_dataset(f"index_{level.name}", data=level.values, dtype=level.dtype)

            case pd.Index():
                f.create_dataset("index_datetime", data=timeindex.str.encode("utf-8"), dtype="S30")

        index_names = pd.Index(pandas_dataframe.index.names)
        f.create_dataset("index_names", data=index_names, dtype=f"S{index_names.map(len).max()}")

        f.create_dataset(
            "columns", data=pandas_dataframe.columns, dtype=f"S{pandas_dataframe.columns.map(len).max()}"
        )

        if len(pandas_dataframe.dtypes.unique()) > 1:
            msg = f"Multiple data types detected in {output_fpath.name}, "
            msg += "unclear which one to use for re-saving h5."
            raise Exception(msg)
        pandas_dataframe = pandas_dataframe.astype(np.float32)
        dftype_out = pandas_dataframe.dtypes.unique()[0]
        f.create_dataset(
            "data",
            data=pandas_dataframe.values,
            dtype=dftype_out,
            compression="gzip",
            compression_opts=compression_opts,
        )
    return True


def get_function_arguments(argument_input: dict, function: Callable) -> dict:
    """Get arguments to pass to a function based on its signature.

    This function processes the `argument_input` and returns a dictionary of argument
    values that are valid for the given `function`, using the function's signature
    as a filter. String values are validated, nested dictionaries are flattened,
    and only the valid argument keys (as defined in the function signature) are included.

    Parameters
    ----------
    data_dict : dict
        A dictionary containing potential argument values, which may include
        strings, dictionaries, and other types of data.

    function : str
        The name of the function whose signature is used to filter the arguments.

    Returns
    -------
    dict
        A dictionary of filtered arguments that match the function's signature.
        Only arguments that exist in the function's signature will be included.

    Example
    -------
    >>> def example_function(a, b, c=None):
    >>>     pass
    >>> data = {"a": 1, "b": 2, "c": 3, "extra": 4}
    >>> prepare_function_arguments(data, "example_function")
    {'a': 1, 'b': 2, 'c': 3}
    """
    arguments = {}
    for key, value in argument_input.items():
        if isinstance(value, str):
            value = validate_string(value)
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                arguments[sub_key] = sub_value
        else:
            arguments[key] = value

    return {key: value for key, value in arguments.items() if key in inspect.getfullargspec(function).args}
