"""Plugin to add hurdle rate between regions.

This plugin is only applicable for ReEDs, but could work with similarly arrange data
"""

from argparse import ArgumentParser

from loguru import logger

from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.models.branch import MonitoredLine
from r2x.parser.handler import BaseParser


def cli_arguments(parser: ArgumentParser):
    """CLI arguments for the plugin."""
    parser.add_argument(
        "--hurdle-rate",
        type=float,
        help="Hurdle rate between regions",
    )


def update_system(
    config: Scenario,
    system: System,
    hurdle_rate: float | None = None,
    parser: BaseParser | None = None,
) -> System:
    """Apply hurdle rate between regions.

    This function updates the default hurdle rate for the ReEDS parser using a new file

    Parameters
    ----------
    config : Scenario
        The scenario configuration.
    system : System
        The system object to be updated.
    hurdle_rate : str, optional
        The hurdle rate to apply betwen regions.
    parser : BaseParser, optional
        The parser object used for parsing.
    """
    if not config.output_model == "plexos" or not config.input_model == "reeds-US":
        msg = "Plugin `hurdle_rate.py` is not compatible with a model that is not Plexos."
        raise NotImplementedError(msg)

    if hurdle_rate is None:
        logger.warning("Could not set hurdle rate value. Skipping plugin.")
        return system
    logger.info("Applying hurdle rate to transmission lines")

    if parser is not None:
        assert parser.data.get("hierarchy") is not None, (
            "Did not find hierarchy file on parser. Check parser object."
        )

    for line in system.get_components(MonitoredLine):
        region_to = line.to_bus.load_zone.name
        region_from = line.from_bus.load_zone.name
        if region_to != region_from:
            if previous_hurdle := line.ext.get("Wheeling Charge"):
                logger.debug(
                    "Changing hurdle rate for {} from {} to {}.", line.name, previous_hurdle, hurdle_rate
                )
            line.ext["Wheeling Charge"] = hurdle_rate
            line.ext["Wheeling Charge Back"] = hurdle_rate
    return system
