"""Plugin for Cambium specific configuration

This plugin is only applicable for ReEDs, but could work with similarly arrange data
"""

import polars as pl

from argparse import ArgumentParser
from loguru import logger

from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.models.branch import MonitoredLine
from r2x.models.generators import Generator
from r2x.models.topology import ACBus, LoadZone
from r2x.parser.handler import BaseParser


def cli_arguments(parser: ArgumentParser):
    """CLI arguments for the plugin."""
    parser.add_argument(
        "--perturb",
        type=float,
        help="Load perturbation scalar",
    )


def update_system(
    config: Scenario,
    system: System,
    perturb: float,
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
        msg = "Plugin `cambium.py` is not compatible with a model that is not Plexos."
        raise NotImplementedError(msg)

    logger.info("Applying cambium configuration")

    # Fixed load for Nuclear generators
    for generator in system.get_components(
        Generator,
        filter_func=lambda x: any(
            tech in x.ext["reeds_tech"] for tech in ["nuclear", "lfill", "biopower"]
        ),
    ):
        generator.ext["Fixed Load"] = generator.active_power

    # Removing probabilistic outages
    for generator in system.get_components(Generator):
        if generator.planned_outage_rate is None or generator.forced_outage_rate is None:
            continue

        generator.active_power = (1-generator.planned_outage_rate)*(1-generator.forced_outage_rate)*generator.active_power
        
        generator.planned_outage_rate = None
        generator.forced_outage_rate = None
        generator.mean_time_to_repair = None

    for zone in system.get_components(ACBus):
        zone.ext["Load Scalar"] = perturb

    #comment out rest of this for not including hurdle rates
    if parser is not None:
        if not (parser.data.get("hurdle_rate") is not None):
            msg = "Hurdle rate file not found. Skipping pluging."
            logger.debug(msg)
            return system
        hurdle_rate_data = parser.data["hurdle_rate"]
        for line in system.get_components(MonitoredLine):
            to_bus = line.to_bus.name
            from_bus = line.from_bus.name
            hurdle_rate = (
                hurdle_rate_data.filter(pl.col("from_bus") == from_bus)
                .filter(pl.col("to_bus") == to_bus)["hurdle_rate"]
                .item()
            )
            logger.debug(f"Setting hurdle rate between {to_bus=} and {from_bus=} of {hurdle_rate=}")
            if to_bus != from_bus:
                if previous_hurdle := line.ext.get("Wheeling Charge"):
                    logger.debug(
                        "Changing hurdle rate for {} from {} to {}.",
                        line.name,
                        previous_hurdle,
                        hurdle_rate,
                    )
                # NOTE: This assume that we have the same hurdle rate in both directions.
                line.ext["Wheeling Charge"] = hurdle_rate
                line.ext["Wheeling Charge Back"] = hurdle_rate
    #end comment out
    return system
 