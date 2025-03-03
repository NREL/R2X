"""Electrolyzer representation on PC.

This extension incorporates the load related to the usage of electrolyzer for
each of the ReEDS regions.
"""

# System packages
from datetime import datetime, timedelta

# Third-party packages
import numpy as np
import polars as pl
from infrasys.time_series_models import SingleTimeSeries
from loguru import logger

# Local imports
from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.models import ACBus, Generator, InterruptiblePowerLoad
from r2x.parser.handler import BaseParser
from r2x.parser.polars_helpers import pl_left_multi_join
from r2x.units import ActivePower, FuelPrice

ELECTROLYZER_LOAD_FMAP = "electrolyzer_load"
MONTHLY_H2_FPRICE_FMAP = "h2_fuel_price"


def update_system(parser: BaseParser, config: Scenario, system: System, **_):
    """Modify infrasys system to include electrolyzer load and monthly hydrogen fuel price."""
    logger.info("Adding electrolyzer representation to the system")

    system = electrolyzer_load(config=config, parser=parser, system=system)
    system = hydrogen_fuel_price(config=config, parser=parser, system=system)

    return system


def electrolyzer_load(config: Scenario, parser: BaseParser, system: System) -> System:
    """Add electroylzer load to each region as a fixed load."""
    assert config.input_config.weather_year

    if ELECTROLYZER_LOAD_FMAP not in parser.data:
        logger.warning("No electrolyzer data found on parser. Check parsing filenames.")
        return system

    load_data = parser.data[ELECTROLYZER_LOAD_FMAP]

    if load_data is None:
        logger.warning("No electrolyzer data found on parser. Check parsing filenames.")
        return system

    # Pivot load data to have sum of load for all techs on each column
    load_data_pivot = load_data.pivot(
        index="hour", columns="region", values="load_MW", aggregate_function="sum"
    ).lazy()

    # Create 8760 using hour_map
    hour_map = parser.data["hour_map"]
    total_load_per_region = pl_left_multi_join(hour_map, load_data_pivot).fill_null(0)

    for region in load_data["region"].unique():
        bus = system.get_component(ACBus, name=region)
        max_active_power = ActivePower(0, "MW")
        for component in system.get_components(InterruptiblePowerLoad, filter_func=lambda x: x.bus == bus):
            max_active_power += component.active_power

        # Assert that max active power is greater than 1 MW.
        if max_active_power.magnitude < 1:
            logger.warning("Electrolyzer load for region {} is smaller than 1 MW. Skipping it.", region)
            continue

        # Define new power load and added to the system
        power_load = InterruptiblePowerLoad(name=region, max_active_power=max_active_power, bus=bus)
        system.add_component(power_load)

        ts = SingleTimeSeries.from_array(
            data=ActivePower(total_load_per_region[region], "MW"),
            variable_name="fixed_load",
            initial_time=datetime(year=config.input_config.weather_year, month=1, day=1),
            resolution=timedelta(hours=1),
        )

        user_dict = {"solve_year": config.input_config.weather_year}
        system.add_time_series(ts, power_load, **user_dict)
        logger.debug("Adding electrolyzer load to region: {}", region)
    return system


def hydrogen_fuel_price(config: Scenario, parser: BaseParser, system: System) -> System:
    """Add monthly hydrogen fuel price for generator using hydrogen."""
    if MONTHLY_H2_FPRICE_FMAP not in parser.data:
        logger.warning("No monthly electrolyzer data found on parser. Check parsing filenames.")
        return system

    assert config.input_config.weather_year

    logger.debug("Adding monthly fuel prices for h2 technologies.")
    h2_fprice = parser.data[MONTHLY_H2_FPRICE_FMAP]

    date_time_array = np.arange(
        f"{config.input_config.weather_year}",
        f"{config.input_config.weather_year + 1}",
        dtype="datetime64[h]",
    )[:-24]  # Removing 1 day to match ReEDS convention and converting into a vector

    months = np.array([dt.astype("datetime64[M]").astype(int) % 12 + 1 for dt in date_time_array])

    # Adding fuel price for all hydrogen generators
    for h2_generator in system.get_components(Generator, filter_func=lambda x: "h2" in x.name):
        if h2_generator.bus.name not in h2_fprice["region"]:
            continue
        region_h2_fprice = h2_fprice.filter(pl.col("region") == h2_generator.bus.name)

        month_datetime_series = np.zeros(len(date_time_array), dtype=float)
        for row in region_h2_fprice.iter_rows(named=True):
            month = int(row["month"].strip("m"))
            month_filter = np.where(months == month)
            month_datetime_series[month_filter] = row["h2_price"]
        # Units from monthly hydrogen fuel price are in $/kg
        # Convert $/kG to $/MMbtu ~ 7.5 kg/MMbtu. ~ $7.5/MWh
        month_datetime_series = month_datetime_series * 7.5

        ts = SingleTimeSeries.from_array(
            data=FuelPrice(month_datetime_series, "kg/MWh"),
            variable_name="fuel_price",
            initial_time=datetime(year=config.input_config.weather_year, month=1, day=1),
            resolution=timedelta(hours=1),
        )
        system.add_time_series(ts, h2_generator)

    return system
