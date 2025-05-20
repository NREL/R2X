"""Utility functions for R2X plugin management."""

import importlib.metadata
import os
from pathlib import Path
from loguru import logger


def register_functions_from_folder(folder_path: str | Path):
    """
    Dynamically register specific functions from Python files in a folder.

    Looks for functions with the @register_function decorator and registers them.

    Internal Use Only

    Args:
        folder_path (str): Path to the folder containing Python files
    """
    folder_path = Path(folder_path)
    # Ensure folder path exists
    if not os.path.exists(folder_path):
        msg = f"Folder not found: {folder_path}"
        raise FileNotFoundError(msg)

    # Loop through files in the folder
    for filename in folder_path.glob("*.py"):  # Use glob to safely get .py files
        if filename.name != "__init__.py":  # Skip __init__.py
            module_name = f"r2x.plugins.{filename.stem}"  # e.g., r2x.plugins.break_gens
            try:
                _ = importlib.import_module(module_name)
                logger.debug(f"Successfully imported and registered functions from {module_name}")
            except ImportError as e:
                logger.error(f"Error importing {module_name}: {e!s}")
            except Exception as e:
                logger.error(f"Unexpected error with {module_name}: {e!s}")
