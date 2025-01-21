"""CLI helper functions."""

import argparse
import importlib
import os
from .__version__ import __version__

FILES_WITH_ARGS = [
    "r2x.plugins.pcm_defaults",
    "r2x.plugins.break_gens",
    "r2x.plugins.emission_cap",
    "r2x.plugins.hurdle_rate",
    "r2x.plugins.cambium",
    "r2x.parser.plexos",
    "r2x.parser.reeds",
    "r2x.exporter.plexos",
    "r2x.exporter.sienna",
]


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
    folders: list[str] = FILES_WITH_ARGS,
) -> argparse.ArgumentParser:
    """Add plugin arguments to CLI.

    Args:
        parser: CLI parser object.
        plugin_list: List of plugins.
        folder: Folder that contains the plugin.
    """
    for package in folders:
        package_str = package.split(".")
        if not len(package_str) == 3:
            msg = (
                "Format of `FILES_WITH_ARGS` should be `r2x.package.script` but received {package_name}."
                "Modify `FILES_WITH_ARGS` to match the format."
            )
            raise NotImplementedError(msg)

        _, package_name, script_name = package_str
        try:
            package_script = importlib.import_module(f"{package}")
        except ImportError:
            continue
        if hasattr(package_script, "cli_arguments"):
            script_cli_group = parser.add_argument_group(f"{package_name.upper()}: {script_name}")
            package_script.cli_arguments(script_cli_group)

    return parser


def base_cli() -> argparse.ArgumentParser:
    """Create parser object for CLI."""
    parser = argparse.ArgumentParser(
        description="""Model translation framework""",
        add_help=True,
        prog="r2x",
    )
    subparsers = parser.add_subparsers(dest="command", help="Subcommands")
    init_command = subparsers.add_parser("init", help="Create an empty configuration file.")
    run_command = subparsers.add_parser("run", help="Run an R2X translation")

    init_command.add_argument(
        "-o",
        nargs="?",
        dest="path",
        default=os.getcwd(),
        help="Destination folder where the file will be copied. Defaults to current directory.",
    )

    group_run = run_command.add_argument_group("Options for running the code")
    group_run.add_argument("--inspect", action="store_true", help="Inspect resulting infrasys system.")
    group_run.add_argument("--upgrade", action="store_true", help="Run upgrader logic.")
    group = group_run.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-i",
        dest="run_folder",
        help="Location of the folder with the input model data",
    )
    group.add_argument(
        "--config",
        "--user-config",
        dest="user_config",
        help="User configuration",
    )
    group_cli = run_command.add_argument_group("Arguments used with a run folder")
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
        choices=["plexos", "sienna", "infrasys"],
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
        help="Serialize infrasys system.",
    )
    run_command.add_argument("--pdb", action="store_true", dest="pdb", help="Run with debugger enabled.")
    run_command.add_argument("--flags", nargs="*", dest="feature_flags", action=Flags, help="Feature flags")
    parser.add_argument("--verbose", "-v", action="count", default=0, help="Run with additional verbosity")
    parser.add_argument("--version", "-V", action="version", version=f"R2X version: {__version__}")
    _ = get_additional_arguments(run_command)
    return parser
