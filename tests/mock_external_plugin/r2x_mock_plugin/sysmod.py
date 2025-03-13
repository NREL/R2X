from r2x.config_scenario import Scenario
from r2x.parser.handler import BaseParser
from argparse import ArgumentParser
from r2x.api import System
from loguru import logger

def cli_arguments(parser: ArgumentParser):
    logger.info("adding mock arg")
    parser.add_argument("--mock-plugin", type=str, help="Placeholder argument for testing external plugins")

def update_system(
    config: Scenario,
    system: System,
    parser: BaseParser | None,
    kwargs: dict,
) -> System:
    """
    Run a simple mock external plugin that prints some details and returns the System without any changes.

    Parameters
    ----------
    config : Scenario
        The scenario configuration.
    parser : BaseParser
        The parser object used for parsing.
    system : System
        The system object to be updated.
    mock_plugin: str
        Any string.
    """
    mock_arg = kwargs.get("mock_plugin", "No mock argument given")
    logger.info(f"Inside mock external plugin. mock argument: {mock_arg}")

    if config is None:
        logger.info("Empty Config")
    if parser is None:
        logger.info("No parser found")

    return system
