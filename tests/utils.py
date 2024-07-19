"""Some util functions for the testing."""

from pathlib import Path
import shutil


def clean_folder(path: Path):
    """Remove all files from tmp folder."""
    if path.exists():
        shutil.rmtree(path)
    path.mkdir()
