"""CLI for R2X."""

import json
import sys
import pathlib
from .cli_functions import base_cli
from .config import Configuration
from .runner import scenario_runner
from .utils import read_yaml
from .logger import setup_logging


def cli():
    """Entry point for R2X."""
    parser = base_cli()
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    args, remaining_args = parser.parse_known_args()

    if "--help" in remaining_args:
        parser.print_help(sys.stderr)
        sys.exit(0)

    args = parser.parse_args()

    setup_logging(debug=args.debug)

    cli_args = {k: v for k, v in vars(args).items() if v is not None}
    user_dict = cli_args.pop("user_dict", None)

    # Try to read file first
    if user_dict is not None:
        if pathlib.Path(user_dict).exists():
            if o_dict := read_yaml(user_dict):
                user_dict = o_dict
        else:
            user_dict = json.loads(user_dict)

    if cli_args.get("cases_file") is None:
        if solve_year := cli_args.get("solve_year"):
            cli_args["solve_year"] = solve_year[0] if len(solve_year) == 1 else solve_year
        config_mgr = Configuration.from_cli(cli_args, user_dict=user_dict)
    else:
        config_mgr = Configuration.from_cases(
            cases_fpath=cli_args["cases_file"], cli_args=cli_args, user_dict=user_dict
        )

    if scenario_name := cli_args.get("scenario_name", None):
        config_mgr = config_mgr.get(scenario_name)

    scenario_runner(config_mgr)


if __name__ == "__main__":
    from rich.console import Console

    console = Console()
    setup_logging(level="TRACE")
    args = cli()
