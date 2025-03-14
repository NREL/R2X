"""
Centralized manager for R2X plugins.
"""

from __future__ import annotations
import inspect
import importlib
import importlib.metadata
from typing import Dict, Type, Generator, Tuple, Callable, Optional, List, ParamSpec, TYPE_CHECKING
from loguru import logger
from r2x.utils import validate_fmap

if TYPE_CHECKING:
    from .defaults import PluginComponent
    from argparse import ArgumentParser
    from polars import DataFrame
    from r2x.api import System
    from r2x.config_scenario import Scenario
    from r2x.parser.handler import BaseParser
    from r2x.exporter.handler import BaseExporter
    from r2x.config_models import BaseModelConfig


DEFAULT_SYSMOD_PATH = "src/r2x/plugins"

class PluginManager:
    """Centralized manager for R2X plugins."""

    _instance = None
    _registry: Dict[str, PluginComponent] = {}
    _filter_registry: Dict[str, Callable[[DataFrame, ParamSpec], DataFrame]] = {}
    _cli_registry: Dict[str, Dict[str, Callable[[ArgumentParser], None]]] = {}
    _system_update_registry: Dict[str, Callable[[Scenario, System, Optional[BaseParser], ParamSpec], System]] = {}

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

    #Decorator Methods for registering functions
    @classmethod
    def register_cli(cls, cli_type: str, name: str, group_name: Optional[str] = None):
        """Decorator for registering CLI functions."""
        valid_types = {"parser", "exporter", "system_update"}
        if cli_type not in valid_types:
            raise ValueError(f"cli_type must be one of {valid_types}")
        key = f"{cli_type}_{name}"

        def decorator(func: Callable[[ArgumentParser], None]):
            cls._cli_registry[key] = {"func": func, "group_name": group_name or name}
            return func
        return decorator

    @classmethod
    def register_filter(cls, name: str):
        """Decorator for registering filter functions."""
        def decorator(func: Callable[[DataFrame, ParamSpec], DataFrame]):
            cls._filter_registry[name] = func
            return func
        return decorator

    @classmethod
    def register_system_update(cls, name: str):
        """Decorator for registering system update functions."""
        def decorator(func: Callable[[Scenario, System, Optional[BaseParser], ParamSpec], System]):
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

        # Internal System Modifiers
        register_functions_from_folder(DEFAULT_SYSMOD_PATH)

        # External plugins (factories via entry points)
        for entry_point in importlib.metadata.entry_points().select(group='r2x_plugin'):
            try:

                # load and register external plugins
                load_plugin = entry_point.load()
                self._registry.update(load_plugin())


            except Exception as e:
                logger.error(f"Error registering plugin {entry_point.name}: {e}")

    def get_plugin(self, name: str) -> PluginComponent:
        """Get a PluginComponent."""
        if name not in self._registry:
            raise ValueError(f"Plugin '{name}' not found")

        return self._registry[name]

    def get_filter(self, name: str) -> Callable[[DataFrame, ParamSpec], DataFrame]:
        """Return a filter function for the given name."""
        if name not in self._filter_registry:
            raise ValueError(f"Filter '{name}' not found")
        return self._filter_registry[name]

    def get_cli(self, cli_type: str, name: str) -> Dict[str, Callable[[ArgumentParser], None]]:
        """Return a dictionary of CLI functions for the given type and name."""

        key = f"{cli_type}_{name}"

        if key not in self._cli_registry:
            raise ValueError(f"CLI '{key}' not found")
        return self._cli_registry[key]

    def get_system_modifier(self, name: str) -> Callable[[Scenario, System, Optional[BaseParser], ParamSpec], System]:
        if name not in self._system_update_registry:
            raise ValueError(f"System update '{name}' not found")
        return self._system_update_registry[name]

    @property
    def plugins(self)->Generator[Tuple[str, PluginComponent], None, None]:
        for name, plugin in self._registry.items():
            yield name, plugin

    def get_model_config_class(self, config_name:str, **kwargs)->BaseModelConfig:
        model_config = self._registry[config_name].config
        cls_fields = {field for field in inspect.signature(model_config).parameters}
        model_kwargs = {key: value for key, value in kwargs.items() if key in cls_fields}

        model_config_instance = model_config(**model_kwargs)

        model_config_instance.fmap = self.get_model_input_fmap(config_name)
        return model_config_instance

    def get_model_input_defaults(self, model_name:str)->dict:
        defaults = {}
        for file in self._registry[model_name].parser_defaults:
            defaults = defaults | file.read()

        return defaults

    def get_model_output_defaults(self, model_name:str)->dict:
        defaults = {}
        # TODO: We need to handle default files found in external plugins
        for file in self._registry[model_name].export_defaults:
            defaults = defaults | file.read()
        return defaults

    def get_model_input_fmap(self, config_name:str)->dict:

        try:
            fmap_file = self._registry[config_name].fmap
            if fmap_file is not None:
                fmap = fmap_file.read()
                fmap = validate_fmap(fmap)
                return fmap
            else:
                raise ValueError(f"No fmap path specified for model '{config_name}'")
        except FileNotFoundError:
            raise FileNotFoundError(f"Input fmap file not found for model '{config_name}'")

    @property
    def registered_parsers(self) -> List[str]:
        """
        Get all registered input models (models with parsers).

        Returns
        -------
        List[str]
        List of available parsers
        """
        return [
            name
            for name, plugin in self.plugins
            if plugin.parser is not None
        ]

    @property
    def registered_exporters(self) -> List[str]:
        """
        Get all registered export models (models with exporters).

        Returns
        -------
        Dict[str, Type[BaseExporter]]
            Dictionary mapping model names to exporter classes
        """
        return [
            name
            for name, plugin in self.plugins
            if plugin.exporter is not None
        ]

    @property
    def system_modifiers(self) -> List[str]:
        """
        Get all registered modifier models (models with modifiers).

        Returns
        -------
        List[str]
            List of available modifiers
        """
        return [
            name
            for name in self._system_update_registry.keys()
        ]

    @property
    def filter_functions(self) -> List[str]:
        """
        Get all registered modifier models (models with modifiers).

        Returns
        -------
        List[str]
            List of available modifiers
        """
        return [
            name
            for name in self._filter_registry.keys()
        ]



    def load_parser(self, model_name: str) -> Type[BaseParser]:
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

    def load_exporter(self, model_name: str) -> Type[BaseExporter]:
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
        defaults_dict = {}
        for default_file in components.input_defaults:
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
        defaults_dict = {}
        for default_file in components.export_defaults:
            try:
                from r2x.utils import read_json
                defaults_dict = defaults_dict | read_json(str(default_file.path))
            except Exception as e:
                logger.warning(f"Error loading export defaults {default_file.name}: {e}")

        return defaults_dict
