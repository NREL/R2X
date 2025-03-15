from r2x.plugin_manager.defaults import PluginComponent, DefaultFile

# These will get registered into the plugin manager on module load.
from .parser import cli_arguments as parser_cli
from .exporter import cli_arguments as exporter_cli
from .sysmod import cli_arguments as sysmod_cli, update_system
_ = (parser_cli, exporter_cli, sysmod_cli, update_system)

def get_common_files():
    return {
        "config": DefaultFile.from_path("defaults/config_ext.json", module="r2x_mock_plugin"),
        "plugins": DefaultFile.from_path("defaults/plugins_config_ext.json", module="r2x_mock_plugin")
    }

def create_external_parser()->PluginComponent:

    # Functions to create ModelComponents for each default model
    """Create components for the REEDS-US model."""
    from r2x.parser.reeds import ReEDSParser as TestExternalParser
    from r2x.config_models import  ReEDSConfig as TestExternalConfig
        # Get common defaults
    common_files = get_common_files()

        # Create REEDS-specific input defaults
    input_defaults = [common_files["config"], common_files["plugins"]]
    input_defaults.extend(
        [
            DefaultFile.from_path("defaults/reeds_input_ext.json", module="r2x_mock_plugin")
        ]
    )

    fmap = DefaultFile.from_path("defaults/reeds_us_mapping_ext.json", module="r2x_mock_plugin")
    # REEDS is input-only
    return PluginComponent(
        config=TestExternalConfig,
        parser=TestExternalParser,
        parser_defaults=input_defaults,
        parser_filters=['pl_rename','pl_filter_year'],
        fmap=fmap
    )

def create_external_exporter()->PluginComponent:

    # Functions to create ModelComponents for each default model
    """Create components for the PLEXOS model."""
    from .exporter import TestExternalConfig
    from r2x.exporter.plexos import PlexosExporter as TestExternalExporter
        # Get common defaults
    common_files = get_common_files()

    # Create PLEXOS-specific input defaults
    input_defaults = [common_files["config"], common_files["plugins"]]
    input_defaults.extend(
        [
            DefaultFile.from_path("defaults/plexos_input_ext.json", module="r2x_mock_plugin"),
        ]
    )
    # Create PLEXOS-specific export defaults
    export_defaults =[
        DefaultFile.from_path("defaults/plexos_output_ext.json", module="r2x_mock_plugin"),
        DefaultFile.from_path("defaults/plexos_simulation_objects_ext.json", module="r2x_mock_plugin"),
        DefaultFile.from_path("defaults/plexos_horizons_ext.json", module="r2x_mock_plugin"),
        DefaultFile.from_path("defaults/plexos_models_ext.json", module="r2x_mock_plugin")
    ]

    fmap = DefaultFile.from_path("defaults/plexos_mapping_ext.json", module="r2x_mock_plugin")

    # REEDS is input-only
    return PluginComponent(
        config=TestExternalConfig,
        exporter=TestExternalExporter,
        export_defaults=export_defaults,
        fmap=fmap
    )

# R2X expects a function that returns Dict[str, PluginComponent]
def create_plugin_components()->dict[str, PluginComponent]:

    components = {
        "ExternalParser": create_external_parser(),
        "ExternalExporter": create_external_exporter()
    }

    return components
