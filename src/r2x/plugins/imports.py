"""Plugin to create time series for Imports.

This plugin create the time series representation for imports. Currently, it only process the canadian imports
on ReEDS.

This plugin is only applicable for ReEDs, but could work with similarly arrange data
"""

from datetime import datetime, timedelta
from infrasys.time_series_models import SingleTimeSeries
import polars as pl
from loguru import logger

from r2x.api import System
from r2x.config_models import ReEDSConfig
from r2x.config_scenario import Scenario
from r2x.models.generators import HydroDispatch
from r2x.parser.handler import BaseParser
from r2x.units import Energy


def update_system(
    config: Scenario,
    system: System,
    parser: BaseParser | None = None,
) -> System:
    """Apply an emission cap constraint for the system.

    This function adds to the sytem a :class:`~r2x.models.Constraint`object that is used to set the maximum
    emission per year.

    Parameters
    ----------
    config : Scenario
        The scenario configuration.
    parser : BaseParser
        The parser object used for parsing.
    system : System
        The system object to be updated.
    """
    assert config.input_config
    if not isinstance(config.input_config, ReEDSConfig):
        msg = "Plugin `imports.py` is not compatible with an input model that is not ReEDS."
        raise NotImplementedError(msg)
    assert config.input_config.weather_year
    weather_year = config.input_config.weather_year

    if parser is None:
        msg = "Missing parser information for imports. Skipping plugin."
        logger.debug(msg)
        return system

    if parser is not None:
        # NOTE: We might change this condition once we change the imports definition on ReEDS.
        assert all(key in parser.data for key in ["canada_imports", "canada_szn_frac"]), (
            "Missing required files for import plugin."
        )
        assert "hour_map" in parser.data, "Missing hour map from ReEDS run."

    logger.info("Adding imports time series...")
    hour_map = parser.data["hour_map"]
    szn_frac = parser.data["canada_szn_frac"]
    total_imports = parser.data["canada_imports"]

    hourly_time_series = hour_map.join(szn_frac, on="season", how="left")
    assert not hourly_time_series.is_empty()
    hourly_time_series = hourly_time_series.with_columns(
        pl.col("time_index").str.to_datetime(),
    )
    daily_time_series = hourly_time_series.group_by(pl.col("time_index").dt.date()).median()

    # NOTE: Since the seasons can be repeated, the szn frac can be greater than one. To avoid this, we
    # normalize it again to redistributed the fraction throught the 365 or 366 days.
    daily_time_series_normalized = daily_time_series.with_columns(pl.col("value") / pl.col("value").sum())

    # NOTE: This will need change if we modify the model for the imports. CUrrently all is assumed to be
    # modeled as HydroEnergyReservoir. Currently we only apply it to can-imports.
    initial_time = datetime(year=weather_year, month=1, day=1)
    for generator in system.get_components(HydroDispatch, filter_func=lambda x: "can-imports" in x.name):
        daily_budget = (
            total_imports.filter(pl.col("r") == generator.bus.name)["value"].item()
            * daily_time_series_normalized["value"].to_numpy()
        )
        ts = SingleTimeSeries.from_array(
            data=Energy(daily_budget[:-1] / 1e3, "GWh"),
            variable_name="hydro_budget",
            initial_time=initial_time,
            resolution=timedelta(days=1),
        )
        system.add_time_series(ts, generator)
    return system
