from pluggy import HookimplMarker
from argparse import ArgumentParser
from loguru import logger
from pydantic import Base64Str

from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.parser.handler import BaseParser
from r2x.parser.reeds import ReEDSParser

hookimpl = HookimplMarker("r2x_plugin")


@hookimpl
def cli_arguments(parser: ArgumentParser):
    logger.info("adding mock arg")
    parser.add_argument("--mock-plugin", type=str, help="Placeholder argument for testing external plugins")

class TestExternalParser(BaseParser):
    """A custom parser that extends the built-in ReEDS parser.

    This parser does everything the ReEDS parser does, but adds a
    message to demonstrate that it's being used instead of the built-in one.
    """

    def __init__(self, *args, **kwargs):
        logger.info("👋 Initializing TestExternalParser - this is the external parser plugin")
        self.reeds = ReEDSParser(*args, **kwargs)
        #super().__init__(*args, **kwargs)

    def build_system(self) -> System:
        """Create infrasys system using the underlying ReEDS parser."""
        logger.info("🔨 Building system with TestExternalParser")
        return self.reeds.build_system()


@hookimpl
def register_parser():
    """Register the custom parser."""
    logger.info("📝 Registering mock external parser")
    return {"mock-parser": TestExternalParser}


@hookimpl
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
