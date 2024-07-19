"""Helper function to validate data."""

# System packages
import os
from collections.abc import Iterable
from typing import Any

# Third-party packages
import pandas as pd
from loguru import logger

# Module packages
from .utils import DEFAULT_COLUMN_MAP


def check_input_files(run_folder: str, fmap: dict[str, Any]):
    """Validate ReEDS input folder.

    This function wraps some of the test we apply to validate the input files
    from the ReEDS model.

    Args:
        run_folder: Folder to look for the files,
        fmap: File project mapping.

    TODO: We can probably create this function to take a callable function that
    could be used to test inputs from other models as well.
    """
    # Get a list of mandatory files.
    file_list = [value["fname"] for _, value in fmap.items() if value.get("mandatory", False)]
    file_column_dict = {
        value["fname"]: value.get("column_mapping").keys()
        for _, value in fmap.items()
        if value.get("column_mapping")  # Only process files with column_mapping
        if value.get("mandatory", False)
    }

    # Verify files first.
    missing_files = get_missing_files(run_folder, file_list)
    assert len(missing_files) == 0, f"The following files are missing: {missing_files}"

    # Verify columns needed.

    missing_columns = []
    for fname, expected_columns in file_column_dict.items():
        for path_prefix in ["outputs", "inputs_case", "outputs/inputs_params"]:
            fpath = os.path.join(run_folder, path_prefix, fname)
            if os.path.isfile(fpath):
                missing_columns = get_missing_columns(fpath, expected_columns)
        assert len(missing_columns) == 0, f"Missing columns in {fpath}: {missing_columns}"

    logger.debug("Validation completed!")


def get_missing_columns(fpath: str, column_names: list) -> list:
    """List of missing columns from a csv file.

    We just read the first row of a CSV to check the name of the columns

    Args:
        fpath: Path to the csv file
        column_names: list of columns to verify

    Returns
    -------
        A list of missing columns or empty list
    """
    try:
        _ = pd.read_csv(fpath, nrows=0).rename(columns=DEFAULT_COLUMN_MAP)
    except pd.errors.EmptyDataError:
        logger.error(f"Required file for R2X:{fpath} is empty!")
        raise

    return [col for col in column_names if col not in _.columns.str.lower()]


def get_missing_files(project_folder: str, file_list: Iterable, max_depth: int = 2) -> list:
    """List missing required files from project folder.

    This function looks recursively in the project folder. For safety we only
    look 2 levels of folders

    Args:
        project_folder: Folder to look for the files
        file_list: Iterable of files to check
        max_depth: Level of subfolders to look.

    Returns
    -------
        A list with the missing files or empty list
    """
    all_files = set()

    # Initialize stack with the project folder and depth 0
    input_folder = os.path.join(project_folder, "inputs_case")
    output_folder = os.path.join(project_folder, "outputs")
    stack: list[tuple[str, int]] = [(input_folder, 0), (output_folder, 0)]

    while stack:
        current_folder, current_depth = stack.pop()

        if current_depth > max_depth:
            continue

        for root, dirs, dir_files in os.walk(current_folder):
            for file_name in dir_files:
                file_path = os.path.join(root, file_name)
                all_files.add(os.path.basename(file_path))

            for folder in dirs:
                next_folder = os.path.join(root, folder)
                stack.append((next_folder, current_depth + 1))
    missing_files = [f for f in file_list if os.path.basename(f) not in all_files]
    return missing_files
