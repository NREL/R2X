"""Plugin to dissaggregate and aggregate generators.

This plugin breaks apart generators that are to big in conmparisson with the
WECC database. If the generator is to small after the breakup than the capacity
threshold variable, we drop the genrator entirely.
"""

# System packages
import re
from argparse import ArgumentParser
from infrasys.base_quantity import BaseQuantity
import numpy as np
import pandas as pd
from r2x.api import System

from r2x.models import Emission, Generator
from r2x.config import Scenario
from r2x.parser.handler import BaseParser
from r2x.units import ureg, ActivePower
from r2x.utils import read_json

# Local imports

from loguru import logger


# Constants
CAPACITY_THRESHOLD = 5  # MW
PROPERTIES_TO_BREAK = [
    "ramp_up",
    "ramp_down",
    "min_rated_capacity",
    "startup_cost",
    "pump_load",
    "storage_capacity",
]


def cli_arguments(parser: ArgumentParser):
    """CLI arguments for the plugin."""
    parser.add_argument(
        "--capacity-threshold",
        type=int,
        help="Capacity threshold for aggregating generators.",
        default=CAPACITY_THRESHOLD,
    )


def update_system(
    config: Scenario,
    parser: BaseParser,
    system: System,
    pcm_defaults_fpath: str | None = None,
    capacity_threshold: int = CAPACITY_THRESHOLD,
) -> System:
    """Break apart large generators based on average capacity.

    This function updates and overrides the data objects from the translator to
    be passed to the exporter.

    Args:
        translator: r2x.Translator object,
        *args: additional arguments that can be passed,
        **kwargs: additiona arguments.
    """
    logger.info("Dividing generators into average size generators")
    if pcm_defaults_fpath is None:
        logger.debug("Using {}", config.defaults["pcm_defaults_fpath"])
        pcm_defaults = read_json(config.defaults["pcm_defaults_fpath"])
    else:
        logger.debug("Using custom defaults from {}", pcm_defaults_fpath)
        pcm_defaults: dict = read_json(pcm_defaults_fpath)

    reference_generators = (
        pd.DataFrame.from_dict(pcm_defaults)
        .transpose()
        .reset_index()
        .rename(
            columns={
                "index": "tech",
            }
        )
        .set_index("tech")
        .replace({np.nan: None})
        .to_dict(orient="index")
    )
    non_break_techs = config.defaults.get("non_break_techs", [])

    return break_generators(system, reference_generators, capacity_threshold, non_break_techs)


def break_generators(  # noqa: C901
    system: System,
    reference_generators: dict[str, dict],
    capacity_threshold: int = CAPACITY_THRESHOLD,
    non_break_techs: list[str] | None = None,
    break_category: str = "category",
) -> System:
    """Break component generator into smaller units."""
    if non_break_techs:
        regex_pattern = f"^(?!{'|'.join(non_break_techs)})."  # Oh yes.
    else:
        regex_pattern = ".*"

    capacity_dropped = 0  # count capacity dropped
    for component in system.get_components(Generator, filter_func=lambda x: re.search(regex_pattern, x.name)):
        if not (tech := getattr(component, break_category, None)):
            logger.trace("Skipping component {} with missing category", component.label)
            continue
        logger.trace("Breaking {}", component.label)

        if not (reference_tech := reference_generators.get(tech)):
            logger.trace("{} not found in reference_generators", tech)
            continue
        if not (avg_capacity := reference_tech.get("avg_capacity_MW", None)):
            continue
        logger.trace("Average_capacity: {}", avg_capacity)
        reference_base_power = (
            component.active_power.magnitude
            if isinstance(component.active_power, BaseQuantity)
            else component.active_power
        )
        no_splits = int(reference_base_power // avg_capacity)
        remainder = reference_base_power % avg_capacity
        if no_splits > 1:
            split_no = 1
            logger.trace(
                "Breaking generator {} with active_power {} into {} generators of {} capacity",
                component.name,
                reference_base_power,
                no_splits,
                avg_capacity,
            )
            for _ in range(no_splits):
                component_name = component.name + f"_{split_no:02}"
                new_component = system.copy_component(component, name=component_name, attach=True)
                new_base_power = (
                    ActivePower(avg_capacity, component.active_power.units)
                    if isinstance(component.active_power, BaseQuantity)
                    else avg_capacity * ureg.MW
                )
                new_component.active_power = new_base_power
                proportion = (
                    avg_capacity / reference_base_power
                )  # Required to recalculate properties that depend on active_power
                for property in PROPERTIES_TO_BREAK:
                    if attr := getattr(new_component, property, None):
                        new_component.ext[f"{property}_original"] = attr
                        setattr(new_component, property, attr * proportion)
                new_component.ext["original_capacity"] = component.active_power
                new_component.ext["original_name"] = component.name
                new_component.ext["broken"] = True

                # NOTE: This will be migrated once we implement the SQLite for the components.
                # Add emission objects
                for emission in system.get_components(
                    Emission, filter_func=lambda x: x.generator_name == component.name
                ):
                    new_emission = system.copy_component(
                        emission, name=f"{component_name}_{emission.emission_type}", attach=True
                    )
                    new_emission.generator_name = component_name
                    system.remove_component(emission)

                if system.has_time_series(component):
                    logger.trace(
                        "Component {} has time series attached to it. Copying first one", component.label
                    )
                    ts = system.get_time_series(component)
                    system.add_time_series(ts, new_component)
                split_no += 1
            if remainder > capacity_threshold:
                component_name = component.name + f"_{split_no:02}"
                new_component = system.copy_component(component, name=component_name, attach=True)
                new_component.active_power = remainder * ureg.MW
                proportion = (
                    remainder / reference_base_power
                )  # Required to recalculate properties that depend on active_power
                for property in PROPERTIES_TO_BREAK:
                    if attr := getattr(new_component, property, None):
                        new_component.ext[f"{property}_original"] = attr
                        setattr(new_component, property, attr * proportion)
                new_component.ext["original_capacity"] = component.active_power
                new_component.ext["original_name"] = component.name
                new_component.ext["broken"] = True
                # NOTE: This will be migrated once we implement the SQLite for the components.
                # Add emission objects
                for emission in system.get_components(
                    Emission, filter_func=lambda x: x.generator_name == component.name
                ):
                    new_emission = system.copy_component(
                        emission, name=f"{component_name}_{emission.emission_type}", attach=True
                    )
                    new_emission.generator_name = component_name
                    system.remove_component(emission)
                if system.has_time_series(component):
                    logger.trace(
                        "Component {} has time series attached to it. Copying first one", component.label
                    )
                    ts = system.get_time_series(component)
                    system.add_time_series(ts, new_component)
            else:
                capacity_dropped = capacity_dropped + remainder
                logger.debug("Dropped {} capacity for {}", remainder, component.name)

            # Finally remove the component
            system.remove_component(component)
    logger.info("Total capacity dropped {} MW", capacity_dropped)
    return system
