"""CLI helper functions."""

import argparse
import importlib
from importlib.resources import files
from .__version__ import __version__


class Flags(argparse.Action):
    """Class to enable feature flags on the code and CLI.

    This will save the feature flags in a `flags` argument in the NameSpace of
    argparse
    """

    def __call__(  # type: ignore
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str,
        optional_str: str,
    ) -> None:
        """Save flags as arguments in Namespace."""
        setattr(namespace, self.dest, dict())

        for value in values:
            # split it into key and value
            key, value = value.split("=")
            # assign into dictionary
            getattr(namespace, self.dest)[key] = value


def get_additional_arguments(
    parser: argparse.ArgumentParser,
    folder: str = "r2x.plugins",
) -> argparse.ArgumentParser:
    """Add plugin arguments to CLI.

    Args:
        parser: CLI parser object.
        plugin_list: List of plugins.
        folder: Folder that contains the plugin.
    """
    plugin_files = [plugin for plugin in files(folder).rglob("*.py") if "__" not in plugin.name]  # type: ignore
    for plugin in plugin_files:
        plugin_module = importlib.import_module(f".{plugin.stem}", folder)
        if hasattr(plugin_module, "cli_arguments"):
            plugin_cli_group = parser.add_argument_group(f"Plugin {plugin.stem}")
            plugin_module.cli_arguments(plugin_cli_group)

    parser_files = [parser for parser in files("r2x.parser").rglob("*.py")]  # type: ignore
    for parser_file in parser_files:
        parser_module = importlib.import_module(f".{parser_file.stem}", "r2x.parser")
        if hasattr(parser_module, "cli_arguments"):
            parser_cli_group = parser.add_argument_group(f"Parser {parser_file.stem}")
            parser_module.cli_arguments(parser_cli_group)
    return parser


def base_cli() -> argparse.ArgumentParser:
    """Create parser object for CLI."""
    parser = argparse.ArgumentParser(
        description="""Execute translation of ReEDS scenario results into X model""",
        add_help=True,
        prog="r2x",
    )

    group_run = parser.add_argument_group("Options for running the code")
    group = group_run.add_mutually_exclusive_group()
    group.add_argument(
        "-i",
        dest="run_folder",
        help="Location of the folder with the input model data",
    )
    group.add_argument(
        "--cases",
        dest="cases_file",
        help="Path to the cases file",
    )
    group_cli = parser.add_argument_group("Args used alongside run folder")
    group_cli.add_argument(
        "--name",
        dest="name",
        help="Scenario name",
    )
    group_cli.add_argument(
        "--input-model",
        dest="input_model",
        choices=[
            "plexos",
            "reeds-US",
            "plexos",
            "infrasys",
        ],
        help="Input model to convert from",
    )
    group_cli.add_argument(
        "--output-model",
        dest="output_model",
        choices=["plexos", "sienna", "pras"],
        help="Output model to convert to",
    )
    group_cli.add_argument(
        "-o",
        nargs="?",
        dest="output_folder",
        help="Path to save the result translation",
    )
    group_cli.add_argument(
        "-y",
        "--year",
        type=int,
        nargs="+",
        dest="solve_year",
        help="Year to translate",
    )
    group_cli.add_argument(
        "--weather-year",
        type=int,
        dest="weather_year",
        help="Year to translate",
    )
    group_cli.add_argument(
        "--scenario",
        dest="scenario_name",
        help="Scenario to select from cases file",
    )
    group_cli.add_argument(
        "-ud",
        "--user-dict",
        dest="user_dict",
        help="User provided overrides to defaults",
    )
    group_cli.add_argument(
        "-p",
        "--plugins",
        dest="plugins",
        nargs="*",
        help="Plugins to be included in the translation",
    )
    group_cli.add_argument(
        "--save",
        action="store_true",
        help="Store system in the same output folder",
    )
    group_cli.add_argument("--inspect", action="store_true", help="Inspect infrasys system")
    group_cli.add_argument("--upgrade", action="store_true", help="Run upgrader")
    group_cli.add_argument("--flags", nargs="*", dest="feature_flags", action=Flags, help="Feature flags")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--version", action="version", version=f"R2X version: {__version__}")
    parser = get_additional_arguments(parser)
    return parser
