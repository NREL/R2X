from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Type, Union

from r2x.parser.handler import BaseParser
from r2x.exporter.handler import BaseExporter
from r2x.config_models import BaseModelConfig

from loguru import logger


@dataclass
class DefaultFile:
    """Represents a default configuration file."""
    name: str
    path: Path

    @classmethod
    def from_path(cls, path: str) -> "DefaultFile":
        """Create a DefaultFile from a path string."""
        path_obj = Path(path)
        return cls(name=path_obj.name, path=path_obj)

@dataclass
class PluginComponent:
    """Components associated with a model."""
    config: Type[BaseModelConfig]  # Required
    parser: Optional[Type[BaseParser]] = None
    exporter: Optional[Type[BaseExporter]] = None
    input_defaults: List[DefaultFile] = field(default_factory=list)
    export_defaults: List[DefaultFile] = field(default_factory=list)
    fmap: Optional[DefaultFile] = None

    @property
    def is_input_model(self) -> bool:
        """Check if this is an input model (has parser)."""
        return self.parser is not None

    @property
    def is_export_model(self) -> bool:
        """Check if this is an export model (has exporter)."""

        return self.exporter is not None


# Common default files that apply to all or many models
def get_common_default_files() -> Dict[str, DefaultFile]:
    """Get default files common to all models."""
    common_files = {}

    try:
        config_path = "r2x/defaults/config.json"
        common_files["config"] = DefaultFile.from_path(config_path)
    except Exception as e:
        logger.warning(f"Error loading common config file: {e}")

    try:
        plugins_path = "r2x/defaults/plugins_config.json"
        common_files["plugins"] = DefaultFile.from_path(plugins_path)
    except Exception as e:
        logger.warning(f"Error loading plugins config file: {e}")

    return common_files


# Functions to create ModelComponents for each default model
def create_reeds_plugin() -> PluginComponent:
    """Create components for the REEDS-US model."""
    from r2x.config_models import ReEDSConfig
    from r2x.parser import ReEDSParser

    # Get common defaults
    common_files = get_common_default_files()

    # Create REEDS-specific input defaults
    input_defaults = [common_files["config"], common_files["plugins"]]
    input_defaults.extend(
        [
            DefaultFile.from_path("r2x/defaults/reeds_input.json")
        ]
    )

    fmap = DefaultFile.from_path("r2x/defaults/reeds_us_mapping.json")
    # REEDS is input-only
    return PluginComponent(
        config=ReEDSConfig,
        parser=ReEDSParser,
        input_defaults=input_defaults,
        fmap=fmap
    )

def create_plexos_plugin() -> PluginComponent:
    """Create components for the PLEXOS model."""
    from r2x.config_models import PlexosConfig
    from r2x.parser import PlexosParser
    from r2x.exporter import PlexosExporter
    # Get common defaults
    common_files = get_common_default_files()

    # Create PLEXOS-specific input defaults
    input_defaults = [common_files["config"], common_files["plugins"]]
    input_defaults.extend(
        [
            DefaultFile.from_path("r2x/defaults/plexos_input.json"),
        ]
    )
    # Create PLEXOS-specific export defaults
    export_defaults =[
        DefaultFile.from_path("r2x/defaults/plexos_output.json"),
        DefaultFile.from_path("r2x/defaults/plexos_simulation_objects.json"),
        DefaultFile.from_path("r2x/defaults/plexos_horizons.json"),
        DefaultFile.from_path("r2x/defaults/plexos_models.json")
    ]

    fmap = DefaultFile.from_path("r2x/defaults/plexos_mapping.json")

    # PLEXOS is both input and export
    return PluginComponent(
        config=PlexosConfig,
        parser=PlexosParser,
        exporter=PlexosExporter,
        input_defaults=input_defaults,
        export_defaults=export_defaults,
        fmap=fmap
    )

def create_sienna_plugin() -> PluginComponent:
    """Create components for the SIENNA model."""
    from r2x.config_models import SiennaConfig
    from r2x.exporter import SiennaExporter
    # Get common defaults
    common_files = get_common_default_files()
    # Create SIENNA-specific input defaults
    input_defaults = [common_files["config"], common_files["plugins"]]
    input_defaults = [
        DefaultFile.from_path("r2x/defaults/sienna_config.json"),

    ]
    # Create SIENNA-specific export defaults
    export_defaults = [
        DefaultFile.from_path("r2x/defaults/sienna_config.json"),
    ]

    # SIENNA is both input and export
    return PluginComponent(
        config=SiennaConfig,
        exporter=SiennaExporter,
        input_defaults=input_defaults,
        export_defaults=export_defaults
    )

def create_infrasys_plugin() -> PluginComponent:
    """Create components for the INFRASYS model."""
    from r2x.config_models import InfrasysConfig

    # Get common defaults
    common_files = get_common_default_files()


    # INFRASYS is both input and export
    return PluginComponent(
        config=InfrasysConfig,
        input_defaults=[common_files["config"], common_files["plugins"]],
    )

# Dictionary mapping model names to their component creation functions
DEFAULT_MODEL_CREATORS = {
    "reeds-US": create_reeds_plugin,
    "plexos": create_plexos_plugin,
    "sienna": create_sienna_plugin,
    "infrasys": create_infrasys_plugin,
}

def create_default_registry() -> Dict[str, PluginComponent]:
    """Create the default registry with built-in models."""
    registry = {}

    # Create components for each default model
    for model_name, creator_func in DEFAULT_MODEL_CREATORS.items():
        try:
            registry[model_name] = creator_func()
        except Exception as e:
            logger.error(f"Error creating components for {model_name}: {e}")

    return registry
