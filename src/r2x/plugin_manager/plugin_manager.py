from dataclasses import dataclass

import inspect
import importlib
import importlib.metadata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type, TypeVar, Union, Mapping

from loguru import logger

from r2x.utils import read_json, read_fmap
from .utils import find_subclasses_from_entry_points
from .defaults import PluginComponent, DefaultFile, create_default_registry


from r2x.parser.handler import BaseParser
from r2x.exporter.handler import BaseExporter
from r2x.config_models import BaseModelConfig

class PluginManager:
    """Centralized manager for R2X plugins."""

    _instance = None
    _registry: Dict[str, PluginComponent] = {}

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
            self._initialize_registry()

    def _initialize_registry(self):
        """Initialize the registry with built-in models."""
        # Load built-in models
        self._registry.update(create_default_registry())
        # Load external plugins
        # self._load_external_plugins()

    '''
    def _load_external_plugins(self):
        """Discover and load external plugins."""
        # Discover parsers


        external_parsers = self.discover_parsers()
        for name, parser_class in external_parsers.items():
            if name not in self._registry:
                # Create a new entry
                from r2x.config_models import BaseModelConfig
                self._registry[name] = PluginComponent(
                    config=BaseModelConfig,
                    parser=parser_class
                )
            else:
                # Update existing entry
                self._registry[name].parser = parser_class

        # Discover exporters
        for entry_point in importlib.metadata.entry_points().select(group="r2x_exporter"):
            try:
                name = entry_point.name
                exporter_class = entry_point.load()

                if name not in self._registry:
                    # Create a new entry
                    from r2x.config_models import BaseModelConfig
                    self._registry[name] = PluginComponent(
                        config=BaseModelConfig,
                        exporter=exporter_class
                    )
                else:
                    # Update existing entry
                    self._registry[name].exporter = exporter_class

            except Exception as e:
                logger.error(f"Error loading exporter {entry_point.name}: {e}")

        # Discover model configs
        model_configs = self.discover_models()
        for name, config_class in model_configs.items():
            if name not in self._registry:
                # Create a new entry
                self._registry[name] = PluginComponent(config=config_class)
            else:
                # Update existing entry
                self._registry[name].config = config_class

        pass

    def register_model(self,
                       name: str,
                       config_class: Type[BaseModelConfig],
                       parser_class: Optional[Type[BaseParser]] = None,
                       exporter_class: Optional[Type[BaseExporter]] = None,
                       input_defaults: List[DefaultFile] = None,
                       export_defaults: List[DefaultFile] = None):
        """
        Register a model with its components.

        Parameters
        ----------
        name : str
            Name of the model
        config_class : Type[BaseModelConfig]
            Configuration class for the model
        parser_class : Optional[Type[BaseParser]]
            Parser class for the model
        exporter_class : Optional[Type[BaseExporter]]
            Exporter class for the model
        input_defaults : List[DefaultFile]
            Default configuration files for input
        export_defaults : List[DefaultFile]
            Default configuration files for export
        """
        components = PluginComponent(
            config=config_class,
            parser=parser_class,
            exporter=exporter_class,
            input_defaults=input_defaults or [],
            export_defaults=export_defaults or []
        )
        self._registry[name] = components
        '''

    def get_model_config_class(self, config_name:str, **kwargs)->BaseModelConfig:
        model_config = self._registry[config_name].config
        cls_fields = {field for field in inspect.signature(model_config).parameters}
        model_kwargs = {key: value for key, value in kwargs.items() if key in cls_fields}

        model_config_instance = model_config(**kwargs)

        model_config_instance.fmap = self.get_model_input_fmap(config_name)
        return model_config_instance

    def get_model_input_defaults(self, model_name:str)->dict:
        defaults = {}
        for file in self._registry[model_name].input_defaults:
            defaults = defaults | read_json(str(file.path))

        return defaults

    def get_model_output_defaults(self, model_name:str)->dict:
        defaults = {}
        # TODO: We need to handle default files found in external plugins
        for file in self._registry[model_name].export_defaults:
            defaults = defaults | read_json(str(file.path))
        return defaults

    # TODO this method could probably be removed
    def get_model_input_fmap(self, config_name:str)->dict:

        try:
            fmap = self._registry[config_name].fmap
            if fmap is not None:
                fmap_path = fmap.path
                return read_fmap(str(fmap_path))
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


    # Original methods for plugin discovery
    def discover_parsers(self):
        return find_subclasses_from_entry_points(
            group_name="r2x_parser",
            base_class=BaseParser)

    def discover_models(self):
        return find_subclasses_from_entry_points(
            group_name="r2x_parser",
            base_class=BaseModelConfig
        )
