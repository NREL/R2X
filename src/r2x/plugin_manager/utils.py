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
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    # Loop through files in the folder
    for filename in os.listdir(folder_path):
        # Check if file is a Python file (ends with .py) and not __init__.py
        if filename.endswith(".py") and filename != "__init__.py":
            # Remove .py extension to get module name
            module_name = str(folder_path).replace("src/", "").replace("/", ".") + "." + filename[:-3]

            try:
                # Dynamically import the module. Functions will be registered automatically
                _ = importlib.import_module(module_name)
                # logger.debug(f"Successfully registered {module.__name__}")

            except ImportError as e:
                logger.error(f"Error importing {module_name}: {e!s}")
            except Exception as e:
                logger.error(f"Unexpected error with {module_name}: {e!s}")
