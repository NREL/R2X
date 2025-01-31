"""Compilation of functions that handle upgrades.

This functions apply an update function to certain raw files file before using them for creating the System.
"""

import os
import pathlib
import shutil
import zipfile
from collections import OrderedDict

import pandas as pd
from loguru import logger

from r2x.upgrader.helpers import get_function_arguments
from r2x.utils import read_csv


def rename(fpath: pathlib.Path, new_fname: str) -> pathlib.Path:
    """Apply a new filename to a file.

    This function renames the specified file to a new filename while preserving
    its directory structure. The original file path is resolved to its absolute
    path before renaming.

    Parameters
    ----------
    fpath : pathlib.Path
        The path of the file to be renamed. It should be an existing file.

    new_fname : str
        The new filename to be applied to the file. This should not include any
        directory path; only the name of the file (and optionally the extension).

    Returns
    -------
    pathlib.Path
        The new path of the renamed file.

    Raises
    ------
    FileNotFoundError
        If the file specified by `fpath` does not exist.

    FileExistsError
        If a file with the new name already exists in the same directory.

    Examples
    --------
    >>> import pathlib
    >>> from r2x.upgrader.functions import rename  # Assuming the function is in my_module

    # Rename an existing file
    >>> old_file = pathlib.Path("/path/to/old_file.txt")
    >>> new_file = rename(old_file, "new_file.txt")
    >>> new_file
    PosixPath('/path/to/new_file.txt')

    # Attempt to rename a non-existent file (will raise FileNotFoundError)
    >>> non_existent_file = pathlib.Path("/path/to/non_existent.txt")
    >>> rename(non_existent_file, "new_name.txt")
    FileNotFoundError: [Errno 2] No such file or directory: '/path/to/non_existent.txt'

    # Attempt to rename to a name that already exists (will raise FileExistsError)
    >>> existing_file = pathlib.Path("/path/to/existing_file.txt")
    >>> rename(existing_file, "existing_file.txt")
    FileExistsError: [Errno 17] File exists: '/path/to/existing_file.txt' -> '/path/to/existing_file.txt'
    """
    fpath_new = fpath.resolve().parent.joinpath(new_fname)
    if fpath_new.exists():
        logger.debug(f"File {(fpath_new.name)=} already exist. Skipping it.")
        return fpath_new
    new_fpath = fpath.replace(fpath_new)
    logger.debug("Renaming {} to {}", fpath.name, fpath_new.name)
    return new_fpath


def move_file(fpath: pathlib.Path, new_fpath: str | pathlib.Path) -> pathlib.Path | None:
    """Move a file to a different location.

    This function moves a file from its current location to a new specified
    location using `shutil.move`. If the new path is a string, it will be
    interpreted relative to the parent directory of the original file.

    Parameters
    ----------
    fpath : pathlib.Path
        The path of the file to be moved.
    new_fpath : str or pathlib.Path
        The destination path where the file should be moved. If a string is
        provided, it is interpreted relative to the parent directory of the
        original file.

    Returns
    -------
    pathlib.Path | None
        The new path of the moved file if the move was successful. Returns
        None if the file already exists at the destination.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    ValueError
        If the new path resolves to a location that already contains a file
        with the same name.

    Examples
    --------
    >>> from r2x.upgrader.functions import move_file
    >>> import pathlib
    >>> move_file(pathlib.Path("/path/to/file.txt"), "new_location/file.txt")

    >>> move_file(pathlib.Path("/path/to/non_existent_file.txt"), "new_location/file.txt")
    Traceback (most recent call last):
        ...
    FileNotFoundError: /path/to/non_existent_file.txt does not exist.

    >>> move_file(pathlib.Path("/path/to/existing_file.txt"), "existing_file.txt")
    DEBUG:__main__:File existing_file.txt already exists in the right place.
    """
    logger.debug(f"Moving {fpath} to {new_fpath}")

    if not fpath.exists():
        raise FileNotFoundError(f"{fpath} does not exist.")

    if not isinstance(new_fpath, pathlib.Path):
        new_fpath = fpath.parent.parent / new_fpath

    if os.path.exists(new_fpath):
        logger.debug(f"File {fpath.name} already exists in the right place.")
        return None

    shutil.move(str(fpath), str(new_fpath))
    return new_fpath


def melt(fpath: pathlib.Path, melt_id_vars: list | None = None, var_name="quarter") -> pd.DataFrame:
    """Apply melt operation to data.

    This function reads a CSV file, performs a melt operation on the specified
    columns, and saves the modified data back to the same file. If no identifier
    variables are provided, default values are used.

    Parameters
    ----------
    fpath : pathlib.Path
        The path to the CSV file to be melted.
    melt_id_vars : list, optional
        A list of columns to use as identifier variables. If None, defaults to
        ["i", "r"].
    melt_var_name : str, optional
        The name to assign to the variable column in the melted DataFrame.
        Default is "quarter".

    Returns
    -------
    pd.DataFrame
        The melted DataFrame

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    ValueError
        If the melt operation fails due to missing identifier variables.

    Examples
    --------
    >>> import pandas as pd
    >>> import tempfile
    >>> import pathlib

    # Create a temporary CSV file for testing
    >>> temp_file = pathlib.Path(tempfile.gettempdir()) / "test_data.csv"
    >>> pd.DataFrame({"i": [1, 2], "r": [3, 4], "Q1": [10, 20], "Q2": [30, 40]}).to_csv(
    ...     temp_file, index=False
    ... )

    >>> melt_season(temp_file)
    >>> pd.read_csv(temp_file)
       i  r quarter  value
    0  1  3      Q1     10
    1  1  3      Q2     30
    2  2  4      Q1     20
    3  2  4      Q2     40

    >>> temp_file.unlink()  # Clean up the temporary file
    """
    if not melt_id_vars:
        melt_id_vars = ["i", "r"]

    if not fpath.exists():
        raise FileNotFoundError(f"{fpath} does not exist.")

    data = pd.read_csv(fpath)

    if var_name in data.columns:
        logger.debug("File {} has been already melted. Skipping.", fpath.name)
        return data

    logger.debug(f"Melting columns for {fpath.name}")
    data = pd.melt(data, id_vars=melt_id_vars, var_name=var_name)

    data.to_csv(fpath, index=False)
    return data


def apply_header(fpath: pathlib.Path, header: str) -> pd.DataFrame | None:
    """Apply a header to a CSV file.

    This function reads the first row of a CSV file and compares it to the
    specified header. If the headers match (case-insensitively), the function
    does nothing. If they don't match, it replaces the existing header with the
    new header and saves the updated data back to the file.

    Parameters
    ----------
    fpath : pathlib.Path
        The path to the CSV file to which the header will be applied.
    header : str
        A comma-separated string representing the new header.

    Returns
    -------
    pd.DataFrame | None
        The modified DataFrame if the header was changed; None if no change was made.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    ValueError
        If the CSV file is empty or does not have a valid first row.

    Examples
    --------
    >>> import pandas as pd
    >>> import tempfile
    >>> import pathlib

    # Create a temporary CSV file for testing
    >>> temp_file = pathlib.Path(tempfile.gettempdir()) / "test_data.csv"
    >>> pd.DataFrame({"A": [1, 2], "B": [3, 4]}).to_csv(temp_file, index=False, header=None)

    >>> df = apply_header(temp_file, "a,b")
    >>> df
       a  b
    0  1  3
    1  2  4

    >>> df = apply_header(temp_file, "A,B")
    >>> df is None
    True

    >>> temp_file.unlink()  # Clean up the temporary file
    """
    logger.debug(f"Attempting to add header to {fpath.name}.")

    if not fpath.exists():
        raise FileNotFoundError(f"{fpath} does not exist.")

    first_row = pd.read_csv(fpath, header=None, nrows=1).iloc[0].to_list()

    # Lowercase the columns
    first_row = list(map(lambda x: x.lower() if isinstance(x, str) else x, first_row))

    header_row = header.split(",")
    if first_row == header_row:
        logger.debug(f"File {fpath} has appropriate columns")
        return None

    logger.debug(f"Adding columns {header_row} to {fpath.name}")
    data = pd.read_csv(fpath, header=None, names=header_row, skiprows=1)
    data.to_csv(fpath, index=False)
    return data


def set_index(fpath: pathlib.Path, index: str) -> pd.DataFrame | None:
    """Apply a new name for the index of the CSV file at fpath.

    This function reads a CSV file, checks if the index already has a name,
    and if not, assigns a new name to the index and saves the file.

    Parameters
    ----------
    fpath : pathlib.Path
        The path to the CSV file.
    index_rename : str
        The new name to assign to the index.

    Returns
    -------
    pd.DataFrame | None
        The modified DataFrame if the index name was changed, or None if
        the index already had a name or an error occurred.

    Examples
    --------
    >>> import pathlib
    >>> import pandas as pd
    >>> # Assuming a CSV file 'data.csv' with no index name
    >>> index_rename(pathlib.Path("data.csv"), "NewIndexName")
    >>> df = pd.read_csv("data.csv", index_col=0)
    >>> df.index.name
    'NewIndexName'

    >>> # If 'data.csv' already has an index name
    >>> index_rename(pathlib.Path("data.csv"), "AnotherIndexName")
    data.csv already has an index name: 'NewIndexName'
    """
    if not fpath.exists():
        raise FileNotFoundError(f"{fpath} does not exist.")
    data = pd.read_csv(fpath, index_col=0)

    if data.index.name is not None:
        logger.debug(f"{fpath.name} has index name already.")
        return None

    logger.debug(f"Adding index name {index} to {fpath.name}.")
    data.index.name = index
    data.to_csv(fpath)
    return data


def upgrade_handler(run_folder: str | pathlib.Path):
    """Entry point to call the different upgrade functions."""
    logger.info("Starting upgrader")

    # The file tracker has all the information of what update to perform for each data file.
    file_tracker = read_csv("file_tracker.csv", package_data="r2x.upgrader").collect().to_pandas()
    files_to_modify = file_tracker["fname"].unique()

    # This might actually not be safe for the nas.
    f_dict = OrderedDict(
        {
            f.name: f
            for f in pathlib.Path(run_folder).glob("*[inputs_case|outputs]/*")
            if f.name in files_to_modify
        }
    )

    # Backup inputs_case_files for safety
    backup_fpath = pathlib.Path(run_folder).joinpath("backup_files.zip")
    if not backup_fpath.exists():
        logger.info("Creating backup of files.")
        with zipfile.ZipFile(backup_fpath, mode="w") as archive:
            for fname, fpath_name in f_dict.items():
                archive.write(fpath_name, arcname=fname)

    for fname, f_group in file_tracker.groupby("fname", sort=False):
        if fname not in f_dict:
            logger.debug(f"{fname} not in inputs_case_list. Skipping it.")
            continue

        fpath_name = f_dict[fname]
        assert fpath_name
        functions_to_apply = f_group["method"].iloc[0].split(",")  # List of functions
        f_group_dict = f_group.to_dict(orient="records")[
            0
        ]  # Records return a list of dicts. We jsut get the first element
        f_group_dict["fpath"] = fpath_name

        for function in functions_to_apply:
            function_callable = globals()[function]
            function_arguments = get_function_arguments(f_group_dict, function_callable)
            function_callable(**function_arguments)
            # Update f_dict if we renamed a file to apply additional functions to it
            if function == "rename":
                fpath_new = fpath_name.parent.joinpath(f_group_dict["new_fname"][0])
                f_dict[fpath_new.name] = fpath_new
