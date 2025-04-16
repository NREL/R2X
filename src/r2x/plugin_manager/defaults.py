"""Default plugin components for R2X.

This module defines a PluginComponent as an optional combination of Parsers,
Configurations, Exporters, default files, filter functions, field mappings.

This file is imported by the plugin manager to load default plugins
and make them available system-wide.
"""

from loguru import logger
from r2x.plugin_manager.interfaces import DefaultFile, PluginComponent


# Common default files that apply to all or many models
def get_common_default_files() -> dict[str, DefaultFile]:
    """Get default files common to all models."""
    common_files = {}

    try:
        config_path = "defaults/config.json"

        common_files["config"] = DefaultFile.from_path(config_path)
    except Exception as e:
        logger.warning(f"Error loading common config file: {e}")

    try:
        plugins_path = "defaults/plugins_config.json"
        common_files["plugins"] = DefaultFile.from_path(plugins_path)
    except Exception as e:
        logger.warning(f"Error loading plugins config file: {e}")

    return common_files


# Functions to create ModelComponents for each default model
def create_reeds_plugin() -> PluginComponent:
    """Create components for the REEDS-US model."""
    from r2x.config_models import ReEDSConfig
    from r2x.parser.reeds import ReEDSParser

    # Get common defaults
    common_files = get_common_default_files()

    # Create REEDS-specific input defaults
    input_defaults = [common_files["config"], common_files["plugins"]]
    input_defaults.extend([DefaultFile.from_path("defaults/reeds_input.json")])

    fmap = DefaultFile.from_path("defaults/reeds_us_mapping.json")
    # REEDS is input-only
    return PluginComponent(
        config=ReEDSConfig,
        parser=ReEDSParser,
        parser_defaults=input_defaults,
        parser_filters=["pl_rename", "pl_filter_by_year"],
        fmap=fmap,
    )


def create_reeds_india_plugin() -> PluginComponent:
    """Create components for the REEDS-India model."""
    from r2x.config_models import ReEDSConfig
    from r2x.parser.reeds import ReEDSParser

    # Get common defaults
    common_files = get_common_default_files()

    # Create REEDS-specific input defaults
    input_defaults = [common_files["config"], common_files["plugins"]]
    input_defaults.extend([DefaultFile.from_path("defaults/reeds_india_input.json")])

    fmap = DefaultFile.from_path("defaults/reeds_india_mapping.json")
    # REEDS is input-only
    return PluginComponent(
        config=ReEDSConfig,
        parser=ReEDSParser,
        parser_defaults=input_defaults,
        parser_filters=["pl_rename", "pl_filter_by_year"],
        fmap=fmap,
    )


def create_plexos_plugin() -> PluginComponent:
    """Create components for the PLEXOS model."""
    from r2x.config_models import PlexosConfig
    from r2x.parser.plexos import PlexosParser
    from r2x.exporter.plexos import PlexosExporter

    # Get common defaults
    common_files = get_common_default_files()

    # Create PLEXOS-specific input defaults
    input_defaults = [common_files["config"], common_files["plugins"]]
    input_defaults.extend(
        [
            DefaultFile.from_path("defaults/plexos_input.json"),
        ]
    )
    # Create PLEXOS-specific export defaults
    export_defaults = [
        DefaultFile.from_path("defaults/plexos_output.json"),
        DefaultFile.from_path("defaults/plexos_simulation_objects.json"),
        DefaultFile.from_path("defaults/plexos_horizons.json"),
        DefaultFile.from_path("defaults/plexos_models.json"),
    ]

    fmap = DefaultFile.from_path("defaults/plexos_mapping.json")

    # PLEXOS is both input and export
    return PluginComponent(
        config=PlexosConfig,
        parser=PlexosParser,
        parser_defaults=input_defaults,
        exporter=PlexosExporter,
        export_defaults=export_defaults,
        fmap=fmap,
    )


def create_sienna_plugin() -> PluginComponent:
    """Create components for the SIENNA model."""
    from r2x.config_models import SiennaConfig
    from r2x.exporter.sienna import SiennaExporter

    # Get common defaults
    common_files = get_common_default_files()
    # Create SIENNA-specific input defaults
    input_defaults = [common_files["config"], common_files["plugins"]]
    input_defaults = [
        DefaultFile.from_path("defaults/sienna_config.json"),
    ]
    # Create SIENNA-specific export defaults
    export_defaults = [
        DefaultFile.from_path("defaults/sienna_config.json"),
    ]
    fmap = DefaultFile.from_path("defaults/sienna_mapping.json")

    # SIENNA is both input and export
    return PluginComponent(
        config=SiennaConfig,
        parser_defaults=input_defaults,
        exporter=SiennaExporter,
        export_defaults=export_defaults,
        fmap=fmap,
    )


def create_infrasys_plugin() -> PluginComponent:
    """Create components for the INFRASYS model."""
    from r2x.config_models import InfrasysConfig

    # Get common defaults
    common_files = get_common_default_files()

    # INFRASYS is both input and export
    return PluginComponent(
        config=InfrasysConfig,
        parser_defaults=[common_files["config"], common_files["plugins"]],
    )


# Dictionary mapping model names to their component creation functions
DEFAULT_MODEL_CREATORS = {
    "reeds-US": create_reeds_plugin,
    "reeds-India": create_reeds_india_plugin,
    "plexos": create_plexos_plugin,
    "sienna": create_sienna_plugin,
    "infrasys": create_infrasys_plugin,
}


def create_default_registry() -> dict[str, PluginComponent]:
    """
    Create the default registry with built-in models.

    Returns
    -------
        dict[str, PluginComponent]: A lookup of plugins by name.
    """
    registry = {}

    # Create components for each default model
    for model_name, creator_func in DEFAULT_MODEL_CREATORS.items():
        try:
            registry[model_name] = creator_func()
        except Exception as e:
            logger.error(f"Error creating components for {model_name}: {e}")

    return registry
