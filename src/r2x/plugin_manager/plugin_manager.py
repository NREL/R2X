import inspect
import importlib
import importlib.metadata
from typing import Dict, Type, Generator, Tuple, Callable, Optional
from argparse import ArgumentParser

from loguru import logger

from r2x.utils import validate_fmap
from .defaults import PluginComponent, create_default_registry

from r2x.parser.handler import BaseParser
from r2x.exporter.handler import BaseExporter
from r2x.config_models import BaseModelConfig
from r2x.api import System
from polars import DataFrame



class PluginManager:
    """Centralized manager for R2X plugins."""

    _instance = None
    _registry: Dict[str, PluginComponent] = {}
    _plugin_factories: Dict[str, Callable[[], PluginComponent]] = {}  # Lazy-loaded factories
    _plugin_cache: Dict[str, PluginComponent] = {}  # Cache for instantiated plugins
    _filter_registry: Dict[str, Callable[[DataFrame, dict], DataFrame]] = {}
    _cli_registry: Dict[str, Dict[str, Callable[[ArgumentParser], None]]] = {}
    _system_update_registry: Dict[str, Callable[[System], System]] = {}

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
        def decorator(func: Callable[[DataFrame, dict], DataFrame]):
            cls._filter_registry[name] = func
            return func
        return decorator

    @classmethod
    def register_system_update(cls, name: str):
        def decorator(func: Callable[[System], System]):
            cls._system_update_registry[name] = func
            return func
        return decorator


    def _initialize_registries(self):
        """Initialize registries with factories and eager-loaded functions."""
        # Built-in plugins (factories only)
        from .defaults import DEFAULT_MODEL_CREATORS  # Assuming this exists
        self._plugin_factories.update(DEFAULT_MODEL_CREATORS)

        # External plugins (factories via entry points)
        for entry_point in importlib.metadata.entry_points().select(group='r2x_plugin'):
            try:
                # Store the loader as a factory, not the result
                self._plugin_factories[entry_point.name] = lambda ep=entry_point: ep.load()()
                # Import the module to register filters, CLIs, and system updates
                importlib.import_module(entry_point.module)
            except Exception as e:
                logger.error(f"Error registering plugin {entry_point.name}: {e}")

    def _initialize_registry_old(self):
        """Initialize the registry with built-in models."""
        # Load built-in models
        self._registry.update(create_default_registry())
        # Load external plugins
        self._load_external_plugins()

    def _load_external_plugins(self):
        """Load external plugins."""
        # Load external plugins
        external_plugins = {}

        # search for importlib metadata group names for entry points of r2x_plugins
        for entry_point in importlib.metadata.entry_points().select(group='r2x_plugin'):
            try:
                plugin_loader = entry_point.load()
                # inpsect loader to ensure it returns the correct type.

                # More type checking for each component?
                plugin_components = plugin_loader()

                external_plugins.update(plugin_components)

            except Exception as e:
                print(f"Error loading plugin {entry_point.name}: {e}")

        self._registry.update(external_plugins)

    def get_plugin(self, name: str) -> PluginComponent:
        """Lazily load and cache a PluginComponent."""
        if name not in self._plugin_cache:
            if name not in self._plugin_factories:
                raise ValueError(f"Plugin '{name}' not found")
            self._plugin_cache[name] = self._plugin_factories[name]()
        return self._plugin_cache[name]

    def get_filter(self, name: str) -> Callable[[DataFrame, dict], DataFrame]:
        return self._filter_registry.get(name)

    def get_cli(self, cli_type: str, name: str) -> Callable[[ArgumentParser], None]:
        key = f"{cli_type}_{name}"
        entry = self._cli_registry.get(key)
        return entry["func"] if entry else None

    def get_system_update(self, name: str) -> Callable[[System], System]:
        return self._system_update_registry.get(name)

    @property
    def available_plugins(self) -> Dict[str, dict]:
        """Return metadata about available plugins without loading them."""
        return {
            name: {
                "has_parser": bool(self.get_cli("parser", name)),
                "has_exporter": bool(self.get_cli("exporter", name))
            }
            for name in self._plugin_factories.keys()
        }

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
    def registered_input_models(self) -> Dict[str, Type[BaseParser]]:
        """
        Get all registered input models (models with parsers).

        Returns
        -------
        Dict[str, Type[BaseParser]]
            Dictionary mapping model names to parser classes
        """
        return {
            name: components.parser
            for name, components in self._registry.items()
            if components.parser is not None
        }

    @property
    def registered_output_models(self) -> Dict[str, Type[BaseExporter]]:
        """
        Get all registered export models (models with exporters).

        Returns
        -------
        Dict[str, Type[BaseExporter]]
            Dictionary mapping model names to exporter classes
        """
        return {
            name: components.exporter
            for name, components in self._registry.items()
            if components.exporter is not None
        }



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
        try:
            components = self._registry[model_name]
            if components.parser is not None:
                return components.parser
            else:
                raise ValueError(f"Parser not found for model '{model_name}'")
        except KeyError:
            raise ValueError(f"Model '{model_name}' not found")

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
        try:
            component = self._registry[model_name]
            if component.exporter is not None:
                return component.exporter
            else:
                raise ValueError(f"Exporter not found for model '{model_name}'")
        except KeyError:
            raise ValueError(f"Model '{model_name}' not found")


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
