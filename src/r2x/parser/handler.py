"""Concrete class to handler parser models.

This module provides the abstract class to create parser objects.
"""

# System packages
import inspect
import json
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import pandas as pd
import polars as pl

# Third-party packages
from infrasys.component import Component
from loguru import logger
from plexosdb import XMLHandler
from pydantic import ValidationError

# Local packages
from r2x.api import System
from r2x.config_scenario import Scenario

from ..utils import check_file_exists
from .polars_helpers import pl_filter_year, pl_lowercase, pl_rename


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
) -> pl.LazyFrame | pl.DataFrame | Sequence | XMLHandler | None:
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

    logger.trace("Reading {}", fpath)
    match fpath.suffix:
        case ".csv":
            return csv_handler(fpath, **kwargs)
        case ".h5":
            return pl.LazyFrame(pd.read_hdf(fpath).reset_index())  # type: ignore
        case ".xml":
            class_kwargs = {
                key: value for key, value in kwargs.items() if key in inspect.signature(XMLHandler).parameters
            }
            return XMLHandler.parse(fpath=fpath, **class_kwargs)
        case ".json":
            with open(fpath) as json_file:
                data = json.load(json_file)
            return data
        case _:
            raise NotImplementedError(f"File {fpath.suffix = } not yet supported.")


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

    if data_file.is_empty():
        logger.debug("File {} is empty. Skipping it.", fpath)
        return

    if kwargs.get("keep_case") is None:
        data_file = pl_lowercase(data_file)

    return data_file


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
        filter_funcs = [pl_rename, pl_filter_year]

    # Adding special case for Plexos parser
    if model := getattr(config, "model", False):
        kwargs["model"] = model

    # Parser data
    assert config.input_config
    parser.parse_data(
        base_folder=config.run_folder,
        filter_func=filter_funcs,
        **{**config.input_config.__dict__, **kwargs},
    )

    # Create system
    logger.debug("Starting creation of system: {}", config.name)

    return parser


def create_model_instance(
    model_class: type["Component"], skip_validation: bool = False, **field_values
) -> Any:
    """Create R2X model instance."""
    valid_fields = {
        key: value
        for key, value in field_values.items()
        if key in model_class.model_fields
        if value is not None
    }
    if skip_validation:
        try:
            return model_class.model_validate(valid_fields)
        except ValidationError:
            return model_class.model_construct(**valid_fields)
    return model_class.model_validate(valid_fields)
