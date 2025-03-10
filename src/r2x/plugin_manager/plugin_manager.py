from dis import LOAD_ATTR
from pydantic.type_adapter import R
from r2x.parser import parser_list
from r2x.exporter import exporter_list
from unittest.mock import Base

import importlib
import importlib.metadata
from typing import Any, Dict, List, Optional, Set, Type, TypeVar, Union

import pluggy
from loguru import logger

from r2x.parser.handler import BaseParser
from r2x.exporter.handler import BaseExporter
from r2x.plugins import hookspec
from .utils import find_subclasses_from_entry_points
# Type for parser classes
ParserClass = TypeVar('ParserClass', bound=BaseParser)
ExporterClass = TypeVar('ExporterClass', bound=BaseExporter)


class PluginManager:
    """Centralized manager for R2X plugins.

    This class handles discovery, registration, and management of both internal
    and external plugins for R2X. It provides methods to access plugin-provided
    functionality such as custom parsers.

    Attributes
    ----------
    plugin_manager : pluggy.PluginManager
        The underlying pluggy plugin manager instance
    registered_plugins : Set[str]
        Set of plugin names that have been successfully registered
    registered_parsers : Dict[str, Type[BaseParser]]
        Dictionary mapping parser names to parser classes
    """

    _instance = None

    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the plugin manager if not already initialized."""
        if not getattr(self, "_initialized", False):
            self._initialized = True

    def discover_parsers(self):
        return find_subclasses_from_entry_points(
            group_name="r2x_parser",
            base_class=BaseParser)

    @property
    def registered_parsers(self) -> List[str]:
        """Discover available plugins from entry points.

        Returns
        -------
        List[str]
            Names of discovered plugins
        """
        available_parsers = [p for p in parser_list.keys()]

        try:
            external_parsers = {
                entry_point.name: entry_point
                for entry_point in importlib.metadata.entry_points().select(group="r2x_parser")
            }
            available_parsers.extend(list(external_parsers.keys()))
            logger.debug(f"Discovered external plugins: {list(external_parsers.keys())}")
        except Exception as e:
            logger.error(f"Error discovering plugins: {e}")

        return available_parsers


    @property
    def registered_exporters(self) -> List[str]:
        """Discover available plugins from entry points.

        Returns
        -------
        List[str]
            Names of discovered plugins
        """
        available_exporters = [p for p in exporter_list.keys()]

        try:
            external_exporters = {
                entry_point.name: entry_point
                for entry_point in importlib.metadata.entry_points().select(group="r2x_exporter")
            }
            available_exporters.extend(list(external_exporters.keys()))
            logger.debug(f"Discovered external plugins: {list(external_exporters.keys())}")
        except Exception as e:
            logger.error(f"Error discovering plugins: {e}")

        return available_exporters


    def load_parser(self, parser_name:str)->Union[None,Type[BaseParser]]:
        """Load a parser by name.

        Parameters
        ----------
        parser_name : str
            Name of the parser to load

        Returns
        -------
        Type[BaseParser]
            Parser class
        """
        if parser_name in parser_list:
            return parser_list[parser_name]
        elif parser_name in self.registered_parsers:
            try:
                for entry_point in importlib.metadata.entry_points().select(group="r2x_parser"):
                    if entry_point.name == parser_name:
                        parser = entry_point.load()
                        if type(parser) == type(BaseParser):
                            return parser
                        else:
                            raise ValueError(f"Parser '{parser_name}' is not a valid parser")

            except KeyError:
                raise ValueError(f"Parser '{parser_name}' not found")
        else:
            raise ValueError(f"Parser '{parser_name}' not found")

    def load_exporter(self, exporter_name:str)->Union[None,Type[BaseExporter]]:
        """Load an exporter by name.

        Parameters
        ----------
        exporter_name : str
            Name of the exporter to load

        Returns
        -------
        Type[BaseExporter]
            Exporter class
        """
        if exporter_name in exporter_list:
            return exporter_list[exporter_name]
        elif exporter_name in self.registered_exporters:
            try:
                for entry_point in importlib.metadata.entry_points().select(group="r2x_exporter"):
                    if entry_point.name == exporter_name:
                        exporter = entry_point.load()
                        if type(exporter) == type(BaseExporter):
                            return exporter
                        else:
                            raise ValueError(f"Exporter '{exporter_name}' is not a valid exporter")

            except KeyError:
                raise ValueError(f"Exporter '{exporter_name}' not found")
        else:
            raise ValueError(f"Exporter '{exporter_name}' not found")

# Create a singleton instance
# plugin_manager = PluginManager()
