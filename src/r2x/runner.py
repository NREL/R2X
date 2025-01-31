"""Umbrella API for R2X model."""

import importlib
import inspect
import shutil
import sys
from importlib.resources import files
from pathlib import Path

from loguru import logger

from r2x.exporter.handler import get_exporter

from .api import System
from .config_scenario import Scenario, get_scenario_configuration
from .exporter import exporter_list
from .parser import parser_list
from .parser.handler import BaseParser, get_parser_data
from .upgrader import upgrade_handler
from .utils import (
    DEFAULT_PLUGIN_PATH,
)


def run_parser(config: Scenario, **kwargs):
    """Call get parser for parser selected.

    The parser gets selected based on `config.input_model`.

    Parameters
    ----------
    config
        Scenario
    upgrader
        Function to upgrade the inputs

    Other Parameters
    ----------------
    kwargs
        Additional key arguments to the parser.

    Raises
    ------
    KeyError
        If parser is not found on parser_list
    """
    # At some point we will read the ReEDS tag and upgrade accordingly.
    # reeds_meta = get_file(fname="meta.csv", config=config).iloc[:1]
    assert config.run_folder
    assert config.input_model
    if getattr(config, "upgrade", None):
        upgrade_handler(config.run_folder)

    # Initialize parser
    parser_class = parser_list.get(config.input_model)
    if not parser_class:
        raise KeyError(f"Parser for {config.input_model} not found")

    parser = get_parser_data(config, parser_class, **kwargs)
    system = parser.build_system()

    assert system is not None, "System failed to create"

    return system, parser


def run_plugins(config: Scenario, parser: BaseParser, system: System) -> System:
    """Run selected plugins.

    Parameters
    ----------
    config
        Configuration manager
    system
        System

    Returns
    -------
        System
    """
    if not config.plugins:
        return system

    logger.info("Running the following plugins: {}", config.plugins)
    for plugin in config.plugins:
        module = importlib.import_module(f".{plugin}", DEFAULT_PLUGIN_PATH)
        if hasattr(module, "update_system"):
            plugin_required_args = inspect.getfullargspec(module.update_system).args
            plugin_config_args = {
                key: value for key, value in config.__dict__.items() if key in plugin_required_args
            }
            system = module.update_system(config=config, parser=parser, system=system, **plugin_config_args)
    return system


def run_exporter(config: Scenario, system: System) -> None:
    """Create exporter model."""
    assert config.output_model
    exporter_class = exporter_list.get(config.output_model, None)
    if not exporter_class:
        raise KeyError(f"Exporter for {config.output_model} not found")

    get_exporter(config, system, exporter_class)


def run_single_scenario(scenario: Scenario, **kwargs) -> None:
    """Run translation process."""
    logger.info("Running {}", scenario.name)

    if scenario.input_model == "infrasys":
        fname = f"{scenario.run_folder}/{scenario.name}.json"
        system = System.from_json(filename=fname, **kwargs)
    else:
        system, parser = run_parser(scenario, **kwargs)
        system = run_plugins(config=scenario, parser=parser, system=system)

    if getattr(scenario, "inspect", None):
        from IPython import embed

        embed()
        sys.exit(0)

    # Serialize/deserialize the system
    output_fpath = str(scenario.output_folder) + f"/{scenario.name}.json"
    if getattr(scenario, "save", None):
        logger.info("Serialize system to {}", output_fpath)
        system.to_json(output_fpath, overwrite=True)
        system = System.from_json(output_fpath)

    if scenario.output_model == "infrasys":
        logger.info("Serialize system to {}", output_fpath)
        system.to_json(output_fpath, overwrite=True)
        return
    run_exporter(config=scenario, system=system)
    return


def run(cli_args: dict, user_dict: dict | None = None) -> None:
    """Run a translation.

    This function takes the `cli_args` dictionary to create the configuratio and run the translation process.
    If the user specifies multiple scenarios, this function run the translation sequentially with
    no specific order.

    Parameters
    ----------
    cli_args
        Arguments for constructing the scenario.
    user_dict
        Optional, User custom configuration.

    See Also
    --------
        get_config: Get configuration from arguments
        run_single_scenario: Run a single translation scenario

    Notes
    -----
    Currently the scenario should only have a single year to run.
    """
    config_mgr = get_scenario_configuration(cli_args=cli_args, user_dict=user_dict)
    logger.info("Running {} scenarios", len(config_mgr))
    for _, scenario in config_mgr.scenarios.items():
        # NOTE: We can pass multiple years from the CLI. In those cases we want to raise not implemented.
        if hasattr(scenario, "input_config") and isinstance(
            getattr(scenario.input_config, "solve_year", None), list
        ):
            msg = "Multi year runs from the CLI is not yet supported. Use scenarios instead."
            raise NotImplementedError(msg)
        run_single_scenario(scenario)
    return


def init(cli_args: dict) -> None:
    """Copy the default configuration file on the path that the user request.

    If the user does not provide a path, the current path is used.

    Parameters
    ----------
    cli_args
        Arguments from the CLI
    """
    logger.debug("Running init command")
    path = Path(cli_args["path"])

    with files("r2x.defaults") as package_path:  # type: ignore
        file_path = package_path / "user_dict.yaml"
        shutil.copy(file_path, Path(path) / "user_dict.yaml")
    return
