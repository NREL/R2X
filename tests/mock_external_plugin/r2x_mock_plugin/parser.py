from r2x.plugin_manager import PluginManager

from argparse import ArgumentParser


@PluginManager.register_cli("parser", "ExternalParser")
def cli_arguments(parser: ArgumentParser):
    """CLI arguments for the plugin."""
    parser.add_argument(
        "--external-weather-year",
        type=int,
        dest="weather_year",
        help="ReEDS weather year to translate",
    )
