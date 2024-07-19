"""Concrete class to handler parser models.

This module provides the abstract class to create parser objects.
"""

# System packages
from copy import deepcopy
import inspect
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Any, TypeVar
from collections.abc import Callable, Sequence
from pathlib import Path

# Third-party packages
from loguru import logger
import polars as pl
import pandas as pd

# Local packages
from r2x.api import System
from r2x.config import Scenario
from plexosdb import XMLHandler
from .parser_helpers import pl_filter_year, pl_lowercase, pl_rename
from ..utils import check_file_exists


@dataclass
class BaseParser(ABC):
    """Class that defines the shared methods of parsers.

    Note
    ----
    This class is meant to be use for developing new parsers. Do not use it directly.

    Attributes
    ----------
    config: Scenario
        Scenario configuration
    data: dict
        We save each file read in a data dictionary

    Methods
    -------
    get_data(key='load')
        Return the parsed data for load.
    read_file(fpath="load.csv")
        Read load data.
    parse_data
        Read all files from a configuration file map.
    """

    config: Scenario
    data: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(Files parsed: {len(self.data)})"

    def get_data(self, key: str) -> Any:
        """Return data."""
        if key not in self.data:
            raise KeyError(f"Key `{key}` not found in data dictionary.")
        return self.data[key]

    def read_file(self, fpath: Path | str, filter_funcs: list[Callable] | None = None, **kwargs):
        """Read input model data from the file system.

        Currently supported formats:
            - .csv
            - .h5
            - .xml
        More to come!

        Parameters
        ----------
        fpath: Path, str
            Absolute location of the file in the system
        filter_func: List
            Filter functions to apply

        """
        data = file_handler(fpath, **kwargs)
        if data is None:
            return

        if isinstance(filter_funcs, list):
            for func in filter_funcs:
                data = func(data, **kwargs)
        return data

    def parse_data(
        self,
        *,
        base_folder: str | Path | None,
        fmap: dict,
        filter_func: list[Callable] | None = None,
        **kwargs,
    ) -> None:
        """Parse all the data for the given translation."""
        _fmap = deepcopy(fmap)
        if base_folder is None:
            logger.warning("Missing base folder for {}", self.config.name)
            return None
        logger.trace("Parsing data for {}", self.__class__.__name__)
        for dname, data in _fmap.items():
            if not isinstance(data, dict):
                continue
            if not data.get("fname"):
                continue
            fpath = check_file_exists(fname=data["fname"], run_folder=base_folder)
            if fpath is not None:
                if "fpath" in data:
                    _fpath = data.pop("fpath")
                    # assert fpath == _fpath, f"Multiple files found. {fpath} and {_fpath}"
                    fpath = _fpath
                assert isinstance(fpath, Path) or isinstance(fpath, str)
                fmap[dname]["fpath"] = fpath
                self.data[dname] = self.read_file(fpath=fpath, filter_funcs=filter_func, **{**data, **kwargs})
            logger.debug("Loaded file for {} from {}", dname, fpath)
        return None

    @abstractmethod
    def build_system(self) -> System:
        """Create the infra_sys model."""


class PCMParser(BaseParser):
    """Class defining shared methods for PCM (currently plexos and sienna) parsers."""

    pass


def file_handler(
    fpath: Path | str, optional: bool = False, **kwargs
) -> pl.LazyFrame | Sequence | XMLHandler | None:
    """Return FileHandler based on file extension.

    Raises
    ------
    FileNotFoundError
        If the file is not found.
    NotImplementedError
        If the file format is not yet supported.
    """
    logger.trace("Attempting to read: {}", fpath)
    if not isinstance(fpath, Path):
        fpath = Path(fpath)

    if not fpath.exists() and not optional:
        raise FileNotFoundError(f"Mandatory file {fpath} does not exists.")

    if not fpath.exists() and optional:
        logger.warning("Skipping optional file {}", fpath)
        return None

    match fpath.suffix:
        case ".csv":
            logger.trace("Reading {}", fpath)
            return pl.scan_csv(fpath)
        case ".h5":
            logger.trace("Reading {}", fpath)
            return pl.LazyFrame(pd.read_hdf(fpath).reset_index())  # type: ignore
        case ".xml":
            class_kwargs = {
                key: value for key, value in kwargs.items() if key in inspect.signature(XMLHandler).parameters
            }
            return XMLHandler.parse(fpath=fpath, **class_kwargs)
        case _:
            raise NotImplementedError(f"File {fpath.suffix = } not yet supported.")


ParserClass = TypeVar("ParserClass", bound=BaseParser)


def get_parser_data(
    config: Scenario,
    parser_class: Callable,
    filter_funcs: list[Callable] | None = None,
    **kwargs,
) -> BaseParser:
    """Return parsed system.

    This function will create the ReEDS DataPortal and populate with the most
    common set of data needed


    Paremters
    ---------
    config Scenario configuration class
    parser
        Parser to process
    filter_func
        Functions that will applied to read_data process

    Other Parameters
    ----------------
    kwargs
        year
            For filtering by solve year
        year_column
            To change the column to apply the filter
        column_mapping
            For renaming columns

    See Also
    --------
    BaseParser
    pl_filter_year
    pl_lower_case
    pl_rename
    """
    logger.debug("Creating {} instance.", parser_class.__name__)

    parser = parser_class(config=config, **kwargs)

    # Functions relative to the parser.
    # NOTE: At some point we are going to migrate this out, but this sound like a good standard set
    if filter_funcs is None and config.input_model == "reeds-US":
        logger.trace("Using default filter functions")
        filter_funcs = [pl_lowercase, pl_rename, pl_filter_year]

    # Adding special case for Plexos parser
    if model := getattr(config, "model", False):
        kwargs["model"] = model

    # Parser data
    parser.parse_data(
        fmap=config.fmap,
        base_folder=config.run_folder,
        solve_year=config.solve_year,
        filter_func=filter_funcs,
        **kwargs,
    )

    # Create system
    logger.debug("Starting creation of system: {}", config.name)

    return parser
