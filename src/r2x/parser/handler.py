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

import polars as pl

# Third-party packages
from infrasys.component import Component
from loguru import logger
from plexosdb import XMLHandler
from pydantic import ValidationError

# Local packages
from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.exceptions import R2XParserError
from r2x.utils import check_file_exists

from .handler_utils import csv_handler, h5_handler
from .polars_helpers import pl_filter_by_weather_year, pl_filter_by_year, pl_rename

FILE_PARSING_KWARGS = {
    "absolute_fpath",
    "keep_case",
    "use_filter_functions",
    "column_mapping",
    "solve_year",
    "weather_year",
    "filter_by_weather_year",
}


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

    def read_file(
        self,
        fpath: Path | str,
        filter_funcs: list[Callable] | None = None,
        use_filter_functions: bool = True,
        **kwargs,
    ):
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
        data = file_handler(fpath, parser_class=type(self).__name__, **kwargs)
        if data is None:
            return

        if kwargs.get("filter_by_weather_year") and isinstance(filter_funcs, list):
            filter_funcs.append(pl_filter_by_weather_year)

        if use_filter_functions and isinstance(filter_funcs, list):
            for func in filter_funcs:
                data = func(data, **kwargs)
        return data

    def parse_data(
        self,
        *,
        folder: Path,
        fmap: dict,
        filter_funcs: list[Callable] | None = None,
        **kwargs,
    ) -> bool:
        """Parse all the data for the given translation."""
        logger.trace("Parsing data for {}", self.__class__.__name__)
        _fmap = deepcopy(fmap)

        files_to_parse = {key: value for key, value in _fmap.items() if value is not None}
        if not len(files_to_parse) > 0:
            msg = "Not a single valid entry found on the fmap configuration."
            raise R2XParserError(msg)

        for dname, data in files_to_parse.items():
            fname = data["fname"]
            is_optional = data.get("optional")
            data = {**data, **kwargs}
            file_parsing_kwargs = {key: value for key, value in data.items() if key in FILE_PARSING_KWARGS}

            # If we pass an absolute path we check that it exists.
            if fpath := data.get("absolute_fpath"):
                fpath = Path(fpath)

            if not fpath:
                fpath = check_file_exists(fname=fname, run_folder=folder, folder=data.get("folder"))

            if not fpath and not is_optional:
                msg = (
                    f"Mandatory file {data['fname']} not found at {fpath}. "
                    "Check that the provided path is correct."
                )
                raise R2XParserError(msg)

            if fpath is not None:
                logger.debug("Loading file {} from {}", dname, fpath)
                self.data[dname] = self.read_file(fpath, filter_funcs=filter_funcs, **file_parsing_kwargs)
        return True

    @abstractmethod
    def build_system(self) -> System:
        """Create the infra_sys model."""


class PCMParser(BaseParser):
    """Class defining shared methods for PCM (currently plexos and sienna) parsers."""

    pass


def file_handler(
    fpath: Path | str, parser_class: str | None = None, optional: bool = False, **kwargs
) -> pl.LazyFrame | pl.DataFrame | Sequence | XMLHandler | None:
    """Return FileHandler based on file extension.

    Raises
    ------
    FileNotFoundError
        If the file is not found.
    NotImplementedError
        If the file format is not yet supported or the file format is not supported for the parser.
    """
    logger.trace("Attempting to read: {}", fpath)
    if not isinstance(fpath, Path):
        fpath = Path(fpath)

    logger.trace("Reading {}", fpath)
    match fpath.suffix:
        case ".csv":
            return csv_handler(fpath, **kwargs)
        case ".h5":
            assert parser_class is not None
            return h5_handler(fpath, parser_class=parser_class, **kwargs)
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
    if (filter_funcs is None) and (parser_class.__name__ == "ReEDSParser"):
        logger.trace("Using default filter functions")
        filter_funcs = [pl_rename, pl_filter_by_year]

    # Adding special case for Plexos parser
    if model := getattr(config, "model", False):
        kwargs["model"] = model

    # Parser data
    assert config.input_config
    parser.parse_data(
        folder=config.run_folder,
        filter_funcs=filter_funcs,
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
