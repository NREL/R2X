"""Plugin to add CCS incentive to the model.

This plugin is only applicable for ReEDs, but could work with similarly arrange data
"""

import polars as pl
from loguru import logger

from r2x.api import System
from r2x.units import ureg
from r2x.config_scenario import Scenario
from r2x.models.generators import Generator
from r2x.parser.handler import BaseParser


def update_system(
    config: Scenario,
    system: System,
    parser: BaseParser | None = None,
) -> System:
    """Apply CCS incentive to CCS eligible technologies.

    The incentive is calculated with the capture incentive ($/ton) and capture rate
    (ton/MWh), to produce a subtractor ($/MWh) implemented with PLEXOS' "Use of
    Service Charge".

    Parameters
    ----------
    config : Scenario
        The scenario configuration.
    parser : BaseParser
        The parser object used for parsing.
    system : System
        The system object to be updated.

    Notes
    -----
    The names of some of the columns for the parser data are specified in the `reeds_us_mapping.json`.
    """
    if not config.output_model == "plexos" and not config.input_model == "reeds-US":
        msg = "Plugin `ccs_credit.py` is not compatible with a model that is not Plexos or ReEDs input."
        raise NotImplementedError(msg)

    if parser is None:
        msg = "Missing parser information for ccs_credit. Skipping plugin."
        logger.debug(msg)
        return system

    required_files = ["co2_incentive", "emission_capture_rate", "upgrade_link"]
    if parser is not None:
        if not all(key in parser.data for key in required_files):
            logger.warning("Missing required files for ccs_credit. Skipping plugin.")
            return system

    production_rate = parser.data["emission_capture_rate"]

    # Some technologies on ReEDS are eligible for incentive but have not been upgraded yet. Since the
    # co2_incentive does not capture all the possible technologies, we get the technologies before upgrading
    # and if they exist in the system we apply the incentive.
    incentive = parser.data["co2_incentive"].join(
        parser.data["upgrade_link"], left_on="tech", right_on="to", how="left"
    )
    ccs_techs = incentive["tech"].unique()
    ccs_techs = ccs_techs.unique().extend(incentive["from"].unique())

    for generator in system.get_components(
        Generator, filter_func=lambda gen: gen.ext and gen.ext["reeds_tech"] in ccs_techs
    ):
        reeds_tech = generator.ext["reeds_tech"]
        reeds_vintage = generator.ext["reeds_vintage"]
        reeds_tech_mask = (
            (pl.col("tech") == reeds_tech)
            & (pl.col("region") == generator.bus.name)
            & (pl.col("vintage") == reeds_vintage)
        )
        generator_production_rate = production_rate.filter(reeds_tech_mask)

        if generator_production_rate.is_empty():
            msg = f"Generator {generator.name=} does not appear on the production rate file. Skipping it."
            logger.debug(msg)
            continue

        upgrade_mask = (
            (pl.col("from") == reeds_tech)
            & (pl.col("region") == generator.bus.name)
            & (pl.col("vintage") == reeds_vintage)
        )
        generator_incentive = incentive.filter(reeds_tech_mask.or_(upgrade_mask))["incentive"].item()
        generator.ext["UoS Charge"] = ureg.Quantity(
            -generator_incentive * generator_production_rate["capture_rate"].item(),
            "usd/MWh",  # Negative quantity to capture incentive in the objetive function.
        )
    return system
