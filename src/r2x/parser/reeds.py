"""Functions related to parsers."""

from collections import defaultdict
import importlib
from datetime import datetime, timedelta
from itertools import repeat
from operator import attrgetter
from argparse import ArgumentParser

import numpy as np
import polars as pl
import pyarrow as pa
from infrasys.time_series_models import SingleTimeSeries
from loguru import logger

from r2x.api import System
from r2x.enums import ACBusTypes, PrimeMoversType, ReserveDirection, ReserveType
from r2x.models import (
    ACBus,
    Area,
    Bus,
    Emission,
    Generator,
    GenericBattery,
    HybridSystem,
    HydroGen,
    LoadZone,
    MonitoredLine,
    PowerLoad,
    RenewableDispatch,
    RenewableNonDispatch,
    Reserve,
    ReserveMap,
    TransmissionInterface,
    TransmissionInterfaceMap,
)
from r2x.models.core import MinMax
from r2x.parser.handler import BaseParser
from r2x.units import ActivePower, EmissionRate, Energy, Percentage, Time, ureg
from r2x.utils import match_category, read_csv

from .parser_helpers import pl_left_multi_join

R2X_MODELS = importlib.import_module("r2x.models")
UNITS = importlib.import_module("r2x.units")
BASE_WEATHER_YEAR = 2007


def cli_arguments(parser: ArgumentParser):
    """CLI arguments for the plugin."""
    parser.add_argument(
        "--weather-year",
        type=int,
        dest="weather_year",
        help="ReEDS weather year to translate",
    )


class ReEDSParser(BaseParser):
    """ReEDS parser class."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if not self.config.weather_year:
            raise AttributeError("Missing weather year from the configuration class.")
        if not self.config.solve_year:
            raise AttributeError("Missing solve year from the configuration class.")
        self.device_map = self.config.defaults["reeds_device_map"]
        self.weather_year: int = self.config.weather_year

    def build_system(self) -> System:
        """Create IS system for the ReEDS model."""
        self.system = System(name=self.config.name, auto_add_composed_components=True)

        # Construct transmission network and buses
        self._construct_buses()
        self._construct_reserves()
        self._construct_branches()
        self._construct_tx_interfaces()
        self._construct_generators()

        # Construct additional objects
        self._construct_emissions()

        # Time series construction
        self._construct_load()
        self._construct_hydro_profiles()
        self._construct_cf_time_series()
        self._construct_reserve_provision()
        self._construct_hybrid_systems()

        return self.system

    # NOTE: Rename to create topology
    def _construct_buses(self):
        logger.info("Creating bus objects.")
        bus_data = self.get_data("hierarchy").collect()

        zones = bus_data["transmission_region"].unique()
        for zone in zones:
            self.system.add_component(LoadZone(name=zone))

        for area in bus_data["state"].unique():
            self.system.add_component(Area(name=area))

        for idx, bus in enumerate(bus_data.iter_rows(named=True)):
            self.system.add_component(
                ACBus(
                    number=idx + 1,
                    name=bus["region"],
                    area=self.system.get_component(Area, name=bus["state"]),
                    load_zone=self.system.get_component(LoadZone, name=bus["transmission_region"]),
                    base_voltage=100 * ureg.kV,  # 100kV default since ReEDS does not model voltage
                    bus_type=ACBusTypes.PV,
                )
            )

    def _construct_reserves(self):
        logger.info("Creating reserves objects.")
        bus_data = self.get_data("hierarchy").collect()

        reserves = bus_data["transmission_region"].unique()
        for reserve in reserves:
            for name in self.config.defaults["default_reserve_types"]:
                reserve_duration = self.config.defaults["reserve_duration"].get(name)
                time_frame = self.config.defaults["reserve_time_frame"].get(name)
                load_risk = self.config.defaults["reserve_load_risk"].get(name)
                vors = self.config.defaults["reserve_vors"].get(name)
                reserve_area = self.system.get_component(LoadZone, name=reserve)
                self.system.add_component(
                    Reserve(
                        name=f"{reserve}_{name}",
                        region=reserve_area,
                        reserve_type=ReserveType[name],
                        vors=vors,
                        duration=reserve_duration,
                        load_risk=load_risk,
                        time_frame=time_frame,
                        direction=ReserveDirection.UP,
                    )
                )
        # Add reserve map
        self.system.add_component(ReserveMap(name="reserve_map"))

    def _construct_branches(self):
        logger.info("Creating branch objects.")
        branch_data = self.get_data("tx_cap")
        tx_loss = self.get_data("tx_losses")

        branch_data = pl_left_multi_join(branch_data, tx_loss)
        ext = {"Wheeling Charge": 0.001, "Wheeling Charge Back": 0.001}
        reverse_lines = set()
        for idx, branch in enumerate(branch_data.iter_rows(named=True)):
            from_bus = self.system.get_component(ACBus, branch["from_bus"])
            to_bus = self.system.get_component(ACBus, branch["to_bus"])
            branch_name = f"{idx+1:>04}-{branch['from_bus']}-{branch['to_bus']}"
            reverse_key = (branch["kind"], branch["from_bus"], branch["to_bus"])
            if reverse_key in reverse_lines:
                continue

            # NOTE: Calulate the inverse branch to add it as a single line
            # inverse_line_mask = (pl.col("to_bus") == from_bus.name) & (pl.col("from_bus") == to_bus.name)
            # if not (branch_data.filter(inverse_line_mask).is_empty()):
            #     rating_down = branch_data.filter(inverse_line_mask)["rating"]
            reverse_row = branch_data.filter(
                (pl.col("kind") == branch["kind"])
                & (pl.col("from_bus") == branch["to_bus"])
                & (pl.col("to_bus") == branch["from_bus"])
            ).to_dicts()

            if reverse_row:
                reverse_row_dict = reverse_row[0]
                rating_down = -reverse_row_dict["max_active_power"] * ureg.MW
                reverse_lines.add(
                    (reverse_row_dict["kind"], reverse_row_dict["from_bus"], reverse_row_dict["to_bus"])
                )
            else:
                rating_down = -branch["max_active_power"] * ureg.MW

            losses = branch["losses"] if branch["losses"] else 0
            self.system.add_component(
                MonitoredLine(
                    name=branch_name,
                    from_bus=from_bus,
                    to_bus=to_bus,
                    rating_up=branch["max_active_power"] * ureg.MW,
                    rating_down=rating_down,
                    losses=losses * ureg.percent,
                    ext=ext,
                ),
            )

    def _construct_tx_interfaces(self):
        logger.info("Creating transmission interfaces objects.")
        interface_lines = self.system.get_components(
            MonitoredLine, filter_func=lambda x: x.from_bus.load_zone.name != x.to_bus.load_zone.name
        )
        interfaces = defaultdict(dict)  # Holder of interfaces
        tx_interface_map = TransmissionInterfaceMap(name="transmission_map")
        for line in interface_lines:
            zone_from = line.from_bus.load_zone.name
            zone_to = line.to_bus.load_zone.name
            zone_pair = tuple(sorted((zone_from, zone_to)))
            zone_pair_name = f"{zone_pair[0]}_{zone_pair[1]}"
            if (zone_from, zone_to) == zone_pair:
                positive_flow, negative_flow = line.rating_up.magnitude, line.rating_down.magnitude
            else:
                positive_flow, negative_flow = -line.rating_down.magnitude, -line.rating_up.magnitude

            if "positive_flow" not in interfaces[zone_pair] or "negative_flow" not in interfaces[zone_pair]:
                interfaces[zone_pair]["positive_flow"] = positive_flow
                interfaces[zone_pair]["negative_flow"] = negative_flow
            else:
                interfaces[zone_pair]["positive_flow"] += positive_flow
                interfaces[zone_pair]["negative_flow"] += negative_flow

            tx_interface_map.mapping[zone_pair_name].append(line.label)

        for interface in interfaces:
            interface_name = f"{interface[0]}_{interface[1]}"
            interface_values = interfaces[interface]

            # The inferface transmission flow is defined as the sum of the rating of the lines.
            # In the case that the rate up/down does not match, we take the max and set it in both directions.
            max_power_flow = max(interface_values["positive_flow"], abs(interface_values["negative_flow"]))

            # Ramp multiplier defines the MW/min ratio for the interface
            ramp_multiplier = self.config.defaults["interface_max_ramp_up_multiplier"]
            self.system.add_component(
                TransmissionInterface(
                    name=interface_name,
                    active_power_flow_limits=MinMax(-max_power_flow, max_power_flow),
                    direction_mapping={},  # TBD
                    ext={
                        "ramp_up": max_power_flow * ramp_multiplier * ureg.Unit("MW/min"),
                        "ramp_down": max_power_flow * ramp_multiplier * ureg.Unit("MW/min"),
                    },
                )
            )
        self.system.add_component(tx_interface_map)

    def _construct_emissions(self) -> None:
        """Construct emission objects."""
        logger.info("Creating emission objects")
        emit_rates = self.get_data("emission_rates").collect()

        emit_rates = emit_rates.with_columns(
            pl.concat_str([pl.col("tech"), pl.col("tech_vintage"), pl.col("region")], separator="_").alias(
                "generator_name"
            ),
            pl.concat_str(
                [
                    pl.col("tech"),
                    pl.col("tech_vintage"),
                    pl.col("region"),
                    pl.col("emission_type"),
                ],
                separator="_",
            ).alias("name"),
        )

        for generator in self.system.get_components(
            Generator, filter_func=lambda x: x.name in emit_rates["generator_name"]
        ):
            generator_emission = emit_rates.filter(pl.col("generator_name") == generator.name)
            for row in generator_emission.iter_rows(named=True):
                row["rate"] = EmissionRate(row["rate"], "kg/MWh")
                valid_fields = {key: value for key, value in row.items() if key in Emission.model_fields}
                emission_model = Emission(**valid_fields)
                self.system.add_component(emission_model)

    def _construct_generators(self) -> None:  # noqa: C901
        """Construct generators objects."""
        logger.info("Creating generator objects.")
        capacity_data = self.get_data("online_capacity")
        fuel = self.get_data("fuels")

        # Fuel price requires two input files
        fuel_price_input = self.get_data("fuel_price")
        bfuel_price_output = (
            self.get_data("bfuel_price").with_columns(fuel=pl.lit("biomass")).join(fuel, on="fuel")
        ).select(pl.exclude("fuel"))
        fuel_price = pl.concat([fuel_price_input, bfuel_price_output], how="diagonal")

        heat_rate = self.get_data("heat_rate")
        cost_vom = self.get_data("cost_vom")
        forced_outages = self.get_data("forced_outages")
        planned_outages = self.get_data("planned_outages")
        storage_duration = self.get_data("storage_duration")
        storage_eff = self.get_data("storage_eff")
        category_map = self.config.defaults.get("tech_categories", None)

        # NOTE: Temp unit definition. This should be read from the mapping file?
        unit_definition = {
            "fuel_price": "usd/MMBtu",
            "heat_rate": "MMBtu/MWh",
            "planned_outage_rate": "",
            "forced_outage_rate": "",
            "vom_price": "usd/MWh",
            "active_power": "MW",
            "pump_efficiency": "",
            "charge_efficiency": "",
            "discharge_efficiency": "",
            "pump_load": "MW",
            "storage_capacity": "MWh",
            "storage_duration": "h",
            "initial_energy": "%",
            "initial_volume": "MWh",
        }

        # Combine all the generator dataset in a single frame
        gen_data = pl_left_multi_join(
            capacity_data,
            fuel,
            fuel_price,
            heat_rate,
            cost_vom,
            storage_duration,
            storage_eff,
            forced_outages,
            planned_outages,
        )

        # NOTE: Populate fuel_price information for technologies that use bio_fuel
        gen_data = gen_data.with_columns(
            category=pl.col("tech").map_elements(
                lambda row: match_category(row, category_map), return_dtype=pl.String
            )
        )

        # Adding extra columns
        gen_data = gen_data.with_columns(
            [
                (pl.col("active_power") * pl.col("storage_duration")).alias("storage_capacity"),
                (
                    pl.col("active_power")
                    * pl.col("storage_duration")
                    / self.config.defaults["initial_volume_divisor"]  # Storage start at half of its capacity
                ).alias("initial_volume"),
                (pl.lit(100) / self.config.defaults["initial_volume_divisor"]).alias("initial_energy"),  # 50%
                (pl.col("active_power")).alias("pump_load"),
                (pl.col("charge_efficiency")).alias("pump_efficiency"),
                (
                    pl.when(pl.col("tech").is_in(self.config.defaults["commit_technologies"]))
                    .then(1)
                    .otherwise(None)
                ).alias("must_run"),
            ]
        )

        # NOTE: ReEDS exports some technologies that have a online_capacity of zero.
        # Once it is fixed on main we can remove this line over here.
        gen_data = gen_data.filter(pl.col("active_power") > 0)

        # Check that we mapped all categories
        categories = gen_data["category"].unique()
        for category in categories:
            device_map = self.device_map.get(category, "")
            if getattr(R2X_MODELS, device_map, None) is None:
                logger.warning(
                    "Could not parse category {}. Check that the type map is including this category.",
                    category,
                )
        non_cf_generators = gen_data.filter(~pl.col("category").is_in(self.config.defaults["vre_categories"]))
        cf_generators = gen_data.filter(pl.col("category").is_in(self.config.defaults["vre_categories"]))
        cf_generators = self._aggregate_renewable_generators(cf_generators)

        combined_data = pl.concat([non_cf_generators, cf_generators], how="align")

        for row in combined_data.iter_rows(named=True):
            device_map = self.device_map.get(row["category"], "")
            if getattr(R2X_MODELS, device_map, None) is None:
                continue
            gen_model = getattr(R2X_MODELS, device_map)
            for key, value in row.items():
                if key in unit_definition:
                    if value:
                        row[key] = value * ureg.Unit(unit_definition[key])
            # NOTE: We can uncomment this if we define the units onf the REEDS mapping.
            #     if key in self.config.fmap:
            #         units = self.config.fmap[key].get("units", "")
            #         if value is not None:
            #             row[key] = ureg.Quantity(value, units)

            # EXTRACT THIS NAME CONVENTION TO THE CONFIGURATION
            match gen_model.__name__:
                case "RenewableDispatch" | "RenewableNonDispatch":
                    name = row["tech"] + "_" + row["region"]
                case _:
                    name = row["tech"] + "_" + row["tech_vintage"] + "_" + row["region"]
            row["name"] = name

            # TODO(pesap): Add prime mover type enums to reeds parser.
            # https://github.nrel.gov/PCM/R2X/issues/345
            # NOTE: This should be prime mover type enums.
            tech_fuel_pm_map = self.config.defaults["tech_fuel_pm_map"]
            row["prime_mover_type"] = (
                tech_fuel_pm_map[row["category"]].get("type")
                if row["category"] in tech_fuel_pm_map.keys()
                else tech_fuel_pm_map["default"].get("type")
            )
            row["prime_mover_type"] = PrimeMoversType[row["prime_mover_type"]]
            row["fuel"] = (
                tech_fuel_pm_map[row["category"]].get("fuel")
                if row["category"] in tech_fuel_pm_map.keys()
                else tech_fuel_pm_map["default"].get("fuel")
            )
            bus = self.system.get_component(ACBus, name=row["region"])
            row["bus"] = bus

            # Add reserves/services to generator if they are not excluded
            if row["tech"] not in self.config.defaults["excluded_reserve_techs"]:
                row["services"] = list(
                    self.system.get_components(
                        Reserve,
                        filter_func=lambda x: x.region.name == bus.load_zone.name,
                    )
                )
                reserve_map = self.system.get_component(ReserveMap, name="reserve_map")
                for reserve_type in row["services"]:
                    reserve_map.mapping[reserve_type.name].append(row["name"])

            valid_fields = {
                key: value for key, value in row.items() if key in gen_model.model_fields if value is not None
            }
            valid_fields["ext"] = {
                key: value for key, value in row.items() if key not in valid_fields if value
            }

            commit = True if row["tech"] in self.config.defaults["commit_technologies"] else False
            valid_fields["ext"].update(
                {
                    "reeds_tech": row["tech"],
                    "reeds_vintage": row["tech_vintage"],
                    "Commit": commit,
                }
            )

            self.system.add_component(gen_model(**valid_fields))

    def _construct_load(self):
        logger.info("Adding load time series.")

        bus_data = self.get_data("hierarchy").collect()
        load_df = self.get_data("load").collect()
        start = datetime(year=self.weather_year, month=1, day=1)
        resolution = timedelta(hours=1)

        # Calculate starting index for the weather year
        if len(load_df) > 8760:
            end_idx = 8760 * (self.weather_year - BASE_WEATHER_YEAR + 1)  # +1 to be inclusive.
        else:
            end_idx = 8760
        for _, bus_data in enumerate(bus_data.iter_rows(named=True)):
            bus_name = bus_data["region"]
            bus = self.system.get_component(ACBus, name=bus_name)
            ts = SingleTimeSeries.from_array(
                data=ActivePower(load_df[bus_name][end_idx - 8760 : end_idx].to_numpy(), "MW"),
                variable_name="max_active_power",
                initial_time=start,
                resolution=resolution,
            )
            user_dict = {"solve_year": self.config.weather_year}
            max_load = np.max(ts.data.to_numpy())
            load = PowerLoad(name=f"{bus.name}", bus=bus, max_active_power=max_load * ureg.MW)
            self.system.add_component(load)
            self.system.add_time_series(ts, load, **user_dict)

    def _construct_cf_time_series(self):
        logger.info("Adding cf time series")
        if not self.config.weather_year:
            raise AttributeError("Missing weather year from the configuration class.")

        cf_data = self.get_data("cf").collect()
        cf_adjustment = self.get_data("cf_adjustment").collect()
        # NOTE: We take the median of  the seasonal adjustment since we
        # aggregate the generators by technology vintage
        cf_adjustment = cf_adjustment.group_by("tech").agg(pl.col("cf_adj").median())
        ilr = self.get_data("ilr").collect()
        ilr = dict(
            ilr.group_by("tech").agg(pl.col("ilr").sum()).iter_rows()
        )  # Dict is more useful here than series
        start = datetime(year=self.config.weather_year, month=1, day=1)
        resolution = timedelta(hours=1)

        # Calculate starting index for the weather year starting
        if len(cf_data) > 8760:
            end_idx = 8760 * (self.config.weather_year - BASE_WEATHER_YEAR + 1)  # +1 to be inclusive.
        else:
            end_idx = 8760

        counter = 0
        # NOTE: At some point, I would like to create a single time series per
        # BA instead of attaching one per generator. We would need to invert
        # the order of the loop and just use that to attach it to the different
        for generator in self.system.get_components(RenewableDispatch, RenewableNonDispatch):
            profile_name = generator.name  # .rsplit("_", 1)[0]
            if profile_name not in cf_data.columns:
                msg = (
                    f"{generator.__class__.__name__}:{generator.name} do not "
                    "have a corresponding time series. Consider changing the model to `RenewableGen`"
                )
                logger.warning(msg)
                continue

            cf_adj = cf_adjustment.filter(pl.col("tech") == generator.ext["reeds_tech"])["cf_adj"]
            ilr_value = ilr.get(generator.ext["reeds_tech"], 1)
            rating_profile = (
                generator.active_power
                * ilr_value
                * cf_adj
                * cf_data[profile_name][end_idx - 8760 : end_idx].to_numpy()
            )
            ts = SingleTimeSeries.from_array(
                data=rating_profile,
                variable_name="max_active_power",
                initial_time=start,
                resolution=resolution,
            )
            user_dict = {"solve_year": self.config.weather_year}
            self.system.add_time_series(ts, generator, **user_dict)
            counter += 1
        logger.debug("Added {} time series objects", counter)

    def _construct_reserve_provision(self):
        # NOTE: We need to re-think this chunk of code. The code is bad.
        # Provision is just based on wind/solar and load for the given region.
        logger.debug("Creating reserve provision")

        # We assume that they all start at the same time, so they have the same
        # resolution of the generator time series
        start = datetime(year=self.weather_year, month=1, day=1)
        resolution = timedelta(hours=1)
        for reserve in self.system.get_components(Reserve):
            region = reserve.region
            provision_objects = {}
            provision_objects["solar"] = [
                component
                for component in self.system.get_components(
                    RenewableDispatch,
                    filter_func=lambda x: x.bus.load_zone.name == region.name
                    and (
                        x.prime_mover_type == PrimeMoversType.PV or x.prime_mover_type == PrimeMoversType.RTPV
                    ),
                )
                if self.system.has_time_series(component)
            ]
            provision_objects["wind"] = [
                component
                for component in self.system.get_components(
                    RenewableDispatch,
                    filter_func=lambda x: x.bus.load_zone.name == region.name
                    and (
                        x.prime_mover_type == PrimeMoversType.WT or x.prime_mover_type == PrimeMoversType.WS
                    ),
                )
                if self.system.has_time_series(component)
            ]
            provision_objects["load"] = [
                component
                for component in self.system.get_components(
                    PowerLoad, filter_func=lambda x: x.bus.load_zone.name == region.name
                )
                if self.system.has_time_series(component)
            ]
            solar_names = list(map(attrgetter("name"), provision_objects["solar"]))
            wind_names = list(map(attrgetter("name"), provision_objects["wind"]))
            load_names = list(map(attrgetter("name"), provision_objects["load"]))
            solar_reserves = list(
                map(
                    getattr,
                    map(self.system.get_time_series, provision_objects["solar"]),
                    repeat("data"),
                )
            )
            wind_reserves = list(
                map(
                    getattr,
                    map(self.system.get_time_series, provision_objects["wind"]),
                    repeat("data"),
                )
            )
            load_reserves = list(
                map(
                    getattr,
                    map(self.system.get_time_series, provision_objects["load"]),
                    repeat("data"),
                )
            )
            wind_provision = (
                pa.Table.from_arrays(wind_reserves, names=wind_names)
                .to_pandas()
                .sum(axis=1)
                .mul(self.config.defaults["wind_reserves"].get(reserve.reserve_type.name, 1))
            )
            solar_provision = (
                pa.Table.from_arrays(solar_reserves, names=solar_names)
                .to_pandas()
                .sum(axis=1)
                .mul(self.config.defaults["solar_reserves"].get(reserve.reserve_type.name, 1))
            )
            load_provision = (
                pa.Table.from_arrays(load_reserves, names=load_names)
                .to_pandas()
                .sum(axis=1)
                .mul(self.config.defaults["load_reserves"].get(reserve.reserve_type.name, 1))
            )
            total_provision = load_provision.add(solar_provision, fill_value=0).add(
                wind_provision, fill_value=0
            )
            if total_provision.empty:
                msg = (
                    f"Reserve provision for {reserve=} is zero."
                    "Check that renewable devices contribute to the reserve"
                )
                logger.warning(msg)
            self.system.add_time_series(
                SingleTimeSeries.from_array(
                    total_provision.to_numpy(),
                    variable_name="requirement",
                    initial_time=start,
                    resolution=resolution,
                ),
                reserve,
            )
            # Add total provision as requirement
            setattr(reserve, "max_requirement", total_provision.sum())

    def _construct_hydro_profiles(self):
        season_map = self.config.defaults.get("season_map", None)
        month_hrs = read_csv("month_hrs.csv")
        month_map = self.config.defaults["month_map"]

        hydro_cf = self.get_data("hydro_cf")
        hydro_cf = hydro_cf.with_columns(
            month=pl.col("month").map_elements(lambda row: month_map.get(row, row), return_dtype=pl.String)
        )
        hydro_cf = hydro_cf.with_columns(
            month=pl.col("month").map_elements(lambda row: month_map.get(row, row), return_dtype=pl.String)
        )
        hydro_cap_adj = self.get_data("hydro_cap_adj")
        hydro_cap_adj = hydro_cap_adj.with_columns(
            season=pl.col("season").map_elements(lambda row: season_map.get(row, row), return_dtype=pl.String)
        )
        hydro_minload = self.get_data("hydro_min_gen")
        hydro_minload = hydro_minload.with_columns(
            season=pl.col("season").map_elements(lambda row: season_map.get(row, row), return_dtype=pl.String)
        )
        month_hrs = month_hrs.rename({"szn": "season"})
        hydro_data = pl_left_multi_join(
            hydro_cf,
            month_hrs,
            # hydro_cap_adj,
            # hydro_minload,
        )
        hydro_data = hydro_data.with_columns(
            season=pl.col("season").map_elements(lambda row: season_map.get(row, row), return_dtype=pl.String)
        )

        resolution = timedelta(hours=1)
        initial_time = datetime(self.weather_year, 1, 1)
        date_time_array = np.arange(
            f"{self.weather_year}",
            f"{self.weather_year + 1}",
            dtype="datetime64[h]",
        )[:-24]  # Removing 1 day to match ReEDS convention and converting into a vector
        # date_time_array = np.datetime_as_string(date_time_array, unit="m")
        for generator in self.system.get_components(HydroGen):
            tech = generator.ext["reeds_tech"]
            region = generator.bus.name

            hydro_ratings = hydro_data.filter((pl.col("tech") == tech) & (pl.col("region") == region))
            # Extract months from datetime series
            months = np.array([dt.astype("datetime64[M]").astype(int) % 12 + 1 for dt in date_time_array])

            # Define the values for each season
            season_datetime_series = np.zeros(len(date_time_array), dtype=float)
            seasons = ["winter", "spring", "summer", "fall"]

            match generator.__class__.__name__:
                case "HydroDispatch":
                    for row in hydro_ratings.iter_rows(named=True):
                        season = row["season"]
                        season_start_month = seasons.index(season) * 3 + 1
                        season_end_month = season_start_month + 2
                        season_indices = np.where(
                            (months >= season_start_month) & (months <= season_end_month)
                        )
                        max_energy_season = (
                            generator.active_power
                            * Percentage(row["hydro_cf"], "")
                            * Time(len(season_indices[0]), "h")
                        )
                        season_datetime_series[season_indices] = max_energy_season.magnitude
                    ts = SingleTimeSeries.from_array(
                        Energy(season_datetime_series, "MWh"),
                        "max_active_power",
                        initial_time=initial_time,
                        resolution=resolution,
                    )
                    self.system.add_time_series(ts, generator)
                case "HydroEnergyReservoir":
                    if generator.category == "can-imports":
                        continue
                    for row in hydro_ratings.iter_rows(named=True):
                        season = row["season"]
                        season_start_month = seasons.index(season) * 3 + 1
                        season_end_month = season_start_month + 2
                        season_indices = np.where(
                            (months >= season_start_month) & (months <= season_end_month)
                        )
                        rating = generator.active_power * Percentage(row["hydro_cf"], "")
                        season_datetime_series[season_indices] = rating.magnitude
                    ts = SingleTimeSeries.from_array(
                        ActivePower(season_datetime_series, "MW"),
                        "max_active_power",
                        initial_time=initial_time,
                        resolution=resolution,
                    )
                    self.system.add_time_series(ts, generator)
                case _:
                    pass

    def _construct_hybrid_systems(self):
        """Create hybrid storage units and add them to the system."""
        hybrids = list(
            self.system.get_components(RenewableDispatch, filter_func=lambda x: "pvb" in x.ext["tech"])
        )

        for device in hybrids:
            hybrid_values = device.model_dump()
            hybrid_name = hybrid_values.pop("name")

            ext_dict = device.ext

            # The only two required fields to create the new units are
            # `active_power` and the `bus`. We add some validation on both to
            # guarantee that the parser is correctly adding those into the ext
            # dictionary.
            bus: ACBus = device.bus
            msg = "Bus object not found for hybtid system. Check ingestion of the model inputs"
            assert bus is not None and isinstance(bus, Bus), msg

            # Create storage asset for hybrid.
            storage_unit_fields = {
                key: value for key, value in ext_dict.items() if key in GenericBattery.model_fields
            }
            storage_unit_fields["prime_mover_type"] = PrimeMoversType.BA

            # If at some point we change the power of the storage it should be here
            storage_unit = GenericBattery(
                name=f"{hybrid_name}",
                active_power=device.active_power,  # Assume same power for the battery
                category="pvb-storage",
                bus=bus,
                ext=ext_dict,
                **storage_unit_fields,
            )
            self.system.add_component(storage_unit)

            # Updating name of the renewable component
            new_device = self.system.copy_component(device, name=f"{device.name}", attach=True)
            ts = self.system.get_time_series(device)
            self.system.add_time_series(ts, new_device)
            self.system.remove_component(device)
            hybrid_construct = HybridSystem(
                name=f"{hybrid_name}", renewable_unit=new_device, storage_unit=storage_unit
            )
            self.system.add_component(hybrid_construct)

    def _aggregate_renewable_generators(self, data: pl.DataFrame) -> pl.DataFrame:
        return data.group_by(["tech", "region"]).agg(
            [pl.col("active_power").sum(), pl.exclude("active_power").first()]
        )