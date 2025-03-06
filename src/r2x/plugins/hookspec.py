"""Pluggy Hook Specifications for internal and external plugins."""

import pluggy
from argparse import ArgumentParser

from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.parser.handler import BaseParser

hooskpec = pluggy.HookspecMarker("r2x_plugin")


@hooskpec
def cli_arguments(parser: ArgumentParser):
    """CLI arguments for the plugin."""
    pass


@hooskpec
def update_system(config: Scenario, system: System, parser: BaseParser, kwargs: dict) -> System:
    """Update the system object."""
    pass

@hooskpec
def register_parser() -> dict:
    """Register external parser implementations.

    Returns
    -------
    dict
        A dictionary mapping parser names to parser classes
    """
    pass
