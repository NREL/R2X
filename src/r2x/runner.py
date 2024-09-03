"""Umbrella API for R2X model."""

# System packages
import importlib
import inspect
import sys

from loguru import logger

from infrasys.system import System
from r2x.exporter.handler import get_exporter

# from r2x.models import *
# Module level imports
from .config import Configuration, Scenario
from .exporter import exporter_list
from .parser import parser_list
from .parser.handler import BaseParser, get_parser_data
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
        # Here is the logic to run the upgrader
        # upgrader(config.run_folder, save=True)
        pass

    # Initialize parser
    parser_class = parser_list.get(config.input_model)
    if not parser_class:
        raise KeyError(f"Parser for {config.input_model} not found")

    parser = get_parser_data(config, parser_class, **kwargs)
    system = parser.build_system()

    assert system is not None, "System failed to create"

    # match config.input_model:
    #     case "reeds-US":
    #         return system
    #     case "plexos" | "sienna":
    #         return parser
    #     case _:
    #         raise NotImplementedError(f"Parser for {config.input_model} is not defined.")
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

    run_exporter(config=scenario, system=system)
    return


def scenario_runner(config_mgr: Configuration, **kwargs) -> None:
    """Run the different translation for a configuration.

    If the config object is Configuration class, we run all the different
    Scenarios in parallel using multiprocessing.

    Parameters
    ----------
    config
        Configuration

    Other Parameters
    ----------------
    kwargs
        arguments passed for convenience.
    """
    logger.debug("Running {} scenarios", len(config_mgr))
    for _, scenario in config_mgr.scenarios.items():
        if not isinstance(scenario.solve_year, list) or len(scenario.solve_year) == 1:
            run_single_scenario(scenario, **kwargs)
    return
