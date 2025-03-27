from r2x.config_models import PlexosConfig as TestExternalConfig, get_year
from argparse import ArgumentParser
from r2x.plugin_manager import PluginManager


@PluginManager.register_cli("exporter", "ExternalExporter")
def cli_arguments(parser: ArgumentParser):
    """CLI arguments for the plugin."""
    parser.add_argument(
        "--external-master-file",
        required=False,
        help="Plexos master file to use as template.",
    )


@get_year.register
def _(model_class: TestExternalConfig):
    return model_class.horizon_year
