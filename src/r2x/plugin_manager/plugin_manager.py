"""Centralized manager for R2X plugins."""

from __future__ import annotations
import inspect
import importlib
import importlib.metadata
from typing import ParamSpec, TYPE_CHECKING, ClassVar
from collections.abc import Callable, Generator
from loguru import logger
from r2x.utils import validate_fmap

if TYPE_CHECKING:
    from .defaults import PluginComponent
    from argparse import ArgumentParser, _ArgumentGroup
    from polars import DataFrame
    from r2x.api import System
    from r2x.parser.handler import BaseParser
    from r2x.exporter.handler import BaseExporter
    from r2x.config_models import BaseModelConfig


DEFAULT_SYSMOD_PATH = "src/r2x/plugins"


class PluginManager:
    """
    Centralized manager for R2X plugins.

    Fields:
    _registry: A dictionary mapping plugin names to their corresponding PluginComponent instances.
    _filter_registry: A dictionary mapping filter names to their corresponding filter functions.
    _cli_registry: A dictionary mapping CLI command names to their corresponding CLI handlers.
    _system_update_registry: A dictionary mapping system update names to their corresponding functions.
    """

    _instance = None
    _registry: ClassVar[dict[str, PluginComponent]] = {}
    _filter_registry: ClassVar[dict[str, Callable[[DataFrame, ParamSpec], DataFrame]]] = {}
    _cli_registry: ClassVar[dict[str, Callable[[ArgumentParser | _ArgumentGroup], None]]] = {}
    _system_update_registry: ClassVar[dict[str, Callable[..., System]]] = {}

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
            self._initialize_registries()

    # Decorator Methods for registering functions
    @classmethod
    def register_cli(cls, cli_type: str, name: str, group_name: str | None = None):
        """Register a CLI function.

        Parameters
        ----------
            cli_type (str): The type of CLI function to register (parser, exporter, system_update).
            name (str): The name to register for the CLI function.
            group_name (str | None): The group name for the CLI function.
        """
        valid_types = {"parser", "exporter", "system_update"}
        if cli_type not in valid_types:
            raise ValueError(f"cli_type must be one of {valid_types}")

        if not group_name:
            group_name = name

        key = f"{cli_type}_{name}|{group_name}"

        def decorator(func: Callable[[ArgumentParser | _ArgumentGroup], None]):
            cls._cli_registry[key] = func
            return func

        return decorator

    @classmethod
    def register_filter(cls, name: str):
        """
        Register a filter function.

        Parameters
        ----------
            name (str): The name to register for the filter function.
        """

        def decorator(func: Callable[[DataFrame, ParamSpec], DataFrame]):
            cls._filter_registry[name] = func
            return func

        return decorator

    @classmethod
    def register_system_update(cls, name: str):
        """
        Register a system update function.

        Parameters
        ----------
            name (str): The name to register for the system update function.
        """

        def decorator(func: Callable[..., System]):
            cls._system_update_registry[name] = func
            return func

        return decorator

    def _initialize_registries(self):
        """Initialize registries with factories and eager-loaded functions."""
        # Built-in plugins (factories only)
        from .defaults import DEFAULT_MODEL_CREATORS  # Assuming this exists
        from .utils import register_functions_from_folder

        # Initialize the default plugins
        for key, plugin in DEFAULT_MODEL_CREATORS.items():
            self._registry[key] = plugin()

        # register cli for default plugins
        from r2x.parser.reeds import cli_arguments as reeds_cli_arguments
        from r2x.parser.plexos import cli_arguments as plexos_cli_arguments
        from r2x.exporter.plexos import cli_arguments as plexos_exporter_cli_arguments

        # register filter functions by importing.
        from r2x.parser.polars_helpers import pl_rename, pl_filter_by_year

        # assign the functions so ruff doesn't "ruff" at us.
        _ = (reeds_cli_arguments, plexos_cli_arguments, plexos_exporter_cli_arguments)
        _ = (pl_rename, pl_filter_by_year)

        # Internal System Modifiers
        register_functions_from_folder(DEFAULT_SYSMOD_PATH)

        # External plugins (factories via entry points)
        for entry_point in importlib.metadata.entry_points().select(group="r2x_plugin"):
            try:
                # load and register external plugins
                load_plugin = entry_point.load()
                self._registry.update(load_plugin())

            except Exception as e:
                logger.error(f"Error registering plugin {entry_point.name}: {e}")

    def get_plugin(self, name: str) -> PluginComponent:
        """
        Get a PluginComponent by name.

        Parameters
        ----------
        name : str
            Name of the plugin component

        Returns
        -------
        PluginComponent
            The plugin component with the given name

        Raises
        ------
        ValueError
            If the plugin component with the given name is not found
        """
        if name not in self._registry:
            raise ValueError(f"Plugin '{name}' not found")

        return self._registry[name]

    def get_filter(self, name: str) -> Callable[[DataFrame, ParamSpec], DataFrame]:
        """
        Get a filter function for the given name.

        Parameters
        ----------
        name : str
            Name of the filter function

        Returns
        -------
        Callable[[DataFrame, ParamSpec], DataFrame]
            Filter function
        """
        if name not in self._filter_registry:
            raise ValueError(f"Filter '{name}' not found")
        return self._filter_registry[name]

    def get_cli(self, cli_type: str, name: str) -> Callable[[ArgumentParser | _ArgumentGroup], None]:
        """
        Get the dictionary of CLI functions for the given type and name.

        Parameters
        ----------
        cli_type : str
            Type of CLI function
        name : str
            Name of the CLI function

        Returns
        -------
        Dict[str, Callable[[ArgumentParser], None]]
            Dictionary of CLI functions
        """
        key = f"{cli_type}_{name}"

        if key not in self._cli_registry:
            raise ValueError(f"CLI '{key}' not found")
        return self._cli_registry[key]

    def get_system_modifier(self, name: str) -> Callable[..., System]:
        """
        Get a system modifier function for the given name.

        Parameters
        ----------
        name : str
            Name of the system modifier

        Returns
        -------
        Callable[[Scenario, System, BaseParser | None, ParamSpec], System]
            System modifier function
        """
        if name not in self._system_update_registry:
            raise ValueError(f"System update '{name}' not found")
        return self._system_update_registry[name]

    @property
    def plugins(self) -> Generator[tuple[str, PluginComponent], None, None]:
        """
        Get all registered plugins.

        Yields
        ------
        tuple[str, PluginComponent]
            Name and plugin component
        """
        yield from [(name, plugin) for name, plugin in self._registry.items()]

    def get_model_config_class(self, config_name: str, **kwargs) -> BaseModelConfig:
        """
        Get model configuration class for a given configuration name.

        Parameters
        ----------
        config_name : str
            Name of the configuration

        Returns
        -------
        BaseModelConfig
            Model configuration class
        """
        model_config = self._registry[config_name].config
        cls_fields = {field for field in inspect.signature(model_config).parameters}
        model_kwargs = {key: value for key, value in kwargs.items() if key in cls_fields}

        model_config_instance = model_config(**model_kwargs)

        fmap = self.get_model_input_fmap(config_name)
        if fmap is not None:
            model_config_instance.fmap = fmap
        return model_config_instance

    def get_model_input_defaults(self, model_name: str) -> dict:
        """
        Get parser defaults for a given parser plugin.

        Returns
        -------
        dict or None
        Parser defaults or None if no defaults are found
        """
        defaults: dict[str, str] = {}
        for file in self._registry[model_name].parser_defaults:
            defaults = defaults | file.read()

        return defaults

    def get_model_output_defaults(self, model_name: str) -> dict:
        """
        Get exporter defaults for a given exporter plugin.

        Returns
        -------
        dict or None
        Exporter defaults or None if no defaults are found
        """
        defaults: dict[str, str] = {}
        for file in self._registry[model_name].export_defaults:
            defaults = defaults | file.read()
        return defaults

    def get_model_input_fmap(self, config_name: str) -> dict | None:
        """
        Get a validated field mapping for a given plugin.

        Returns
        -------
        dict or None
        Validated field mapping or None if no mapping is found
        """
        try:
            fmap_file = self._registry[config_name].fmap
            if fmap_file is not None:
                fmap = fmap_file.read()
                fmap = validate_fmap(fmap)
                return fmap
            else:
                return None
        except FileNotFoundError:
            raise FileNotFoundError(f"Input fmap file not found for model '{config_name}'")

    @property
    def registered_parsers(self) -> list[str]:
        """
        Get all registered input models (models with parsers).

        Returns
        -------
        List[str]
        List of available parsers
        """
        return [name for name, plugin in self.plugins if plugin.parser is not None]

    @property
    def registered_exporters(self) -> list[str]:
        """
        Get all registered export models (models with exporters).

        Returns
        -------
        Dict[str, Type[BaseExporter]]
            Dictionary mapping model names to exporter classes
        """
        return [name for name, plugin in self.plugins if plugin.exporter is not None]

    @property
    def system_modifiers(self) -> list[str]:
        """
        Get all registered modifier models (models with modifiers).

        Returns
        -------
        List[str]
            List of available modifiers
        """
        return [name for name in self._system_update_registry.keys()]

    @property
    def filter_functions(self) -> list[str]:
        """
        Get all registered modifier models (models with modifiers).

        Returns
        -------
        List[str]
            List of available modifiers
        """
        return [name for name in self._filter_registry.keys()]

    def load_parser(self, model_name: str) -> type[BaseParser]:
        """
        Load a parser for a model.

        Parameters
        ----------
        model_name : str
            Name of the input model

        Returns
        -------
        Optional[Type[BaseParser]]
            Parser class for the model
        """
        plugin = self.get_plugin(model_name)
        if plugin is not None and plugin.parser is not None:
            return plugin.parser
        else:
            raise ValueError(f"Parser not found for model '{model_name}'")

    def load_exporter(self, model_name: str) -> type[BaseExporter]:
        """
        Load an exporter for a model.

        Parameters
        ----------
        model_name : str
            Name of the export model

        Returns
        -------
        Optional[Type[BaseExporter]]
            Exporter class for the model
        """
        plugin = self.get_plugin(model_name)
        if plugin is not None and plugin.exporter is not None:
            return plugin.exporter
        else:
            raise ValueError(f"Exporter not found for model '{model_name}'")

    def get_input_defaults(self, model_name: str) -> dict:
        """
        Get the input defaults for a model.

        Parameters
        ----------
        model_name : str
            Name of the model

        Returns
        -------
        dict
            Input defaults for the model
        """
        components = self._registry.get(model_name)
        if not components:
            return {}

        # Load and merge all default files
        defaults_dict: dict[str, str] = {}
        for default_file in components.parser_defaults:
            try:
                from r2x.utils import read_json

                defaults_dict = defaults_dict | read_json(str(default_file.path))
            except Exception as e:
                logger.warning(f"Error loading input defaults {default_file.name}: {e}")

        return defaults_dict

    def get_export_defaults(self, model_name: str) -> dict:
        """
        Get the export defaults for a model.

        Parameters
        ----------
        model_name : str
            Name of the model

        Returns
        -------
        dict
            Export defaults for the model
        """
        components = self._registry.get(model_name)
        if not components:
            return {}

        # Load and merge all default files
        defaults_dict: dict[str, str] = {}
        for default_file in components.export_defaults:
            try:
                from r2x.utils import read_json

                defaults_dict = defaults_dict | read_json(str(default_file.path))
            except Exception as e:
                logger.warning(f"Error loading export defaults {default_file.name}: {e}")

        return defaults_dict
