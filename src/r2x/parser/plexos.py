"""Functions related to parsers."""

from datetime import datetime, timedelta
import importlib
from importlib.resources import files
from pathlib import Path, PureWindowsPath
from argparse import ArgumentParser
import pandas as pd
import re

from pint import UndefinedUnitError, Quantity
import polars as pl
import numpy as np
from loguru import logger
from infrasys.exceptions import ISNotStored
from infrasys.time_series_models import SingleTimeSeries
from infrasys.value_curves import InputOutputCurve, AverageRateCurve
from infrasys.cost_curves import FuelCurve
from infrasys.function_data import (
    LinearFunctionData,
    QuadraticFunctionData,
)

from r2x.units import ureg
from r2x.api import System
from r2x.config import Scenario
from r2x.enums import ACBusTypes, ReserveDirection, ReserveType, PrimeMoversType
from r2x.exceptions import ModelError, ParserError
from plexosdb import PlexosSQLite
from plexosdb.enums import ClassEnum, CollectionEnum
from r2x.models.costs import ThermalGenerationCost, RenewableGenerationCost, HydroGenerationCost
from r2x.models import (
    ACBus,
    Generator,
    GenericBattery,
    MonitoredLine,
    PowerLoad,
    Reserve,
    LoadZone,
    ReserveMap,
    TransmissionInterface,
    TransmissionInterfaceMap,
    RenewableGen,
    ThermalGen,
    HydroDispatch,
)
from r2x.utils import validate_string

from .handler import PCMParser
from .parser_helpers import (
    handle_leap_year_adjustment,
    fill_missing_timestamps,
    resample_data_to_hourly,
    filter_property_dates,
    field_filter,
    prepare_ext_field,
)

models = importlib.import_module("r2x.models")

R2X_MODELS = importlib.import_module("r2x.models")
BASE_WEATHER_YEAR = 2007
XML_FILE_KEY = "xml_file"
PROPERTY_SV_COLUMNS_BASIC = ["name", "value"]
PROPERTY_SV_COLUMNS_NAMEYEAR = ["name", "year", "month", "day", "period", "value"]
PROPERTY_TS_COLUMNS_BASIC = ["year", "month", "day", "period", "value"]
PROPERTY_TS_COLUMNS_MULTIZONE = ["year", "month", "day", "period"]
PROPERTY_TS_COLUMNS_PIVOT = ["year", "month", "day"]
PROPERTY_TS_COLUMNS_YM = ["year", "month"]
PROPERTY_TS_COLUMNS_MDP = ["month", "day", "period"]
PROPERTY_TS_COLUMNS_MONTH_PIVOT = [
    "name",
    "m01",
    "m02",
    "m03",
    "m04",
    "m05",
    "m06",
    "m07",
    "m08",
    "m09",
    "m10",
    "m11",
    "m12",
]
DEFAULT_QUERY_COLUMNS_SCHEMA = {  # NOTE: Order matters
    "membership_id": pl.Int64,
    "parent_object_id": pl.Int32,
    "parent_object_name": pl.String,
    "parent_class_name": pl.String,
    "child_class_name": pl.String,
    "category": pl.String,
    "object_id": pl.Int32,
    "name": pl.String,
    "property_name": pl.String,
    "property_unit": pl.String,
    "property_value": pl.Float32,
    "band": pl.Int32,
    "date_from": pl.String,
    "date_to": pl.String,
    "memo": pl.String,
    "scenario_category": pl.String,
    "scenario": pl.String,
    "action": pl.String,
    "data_file_tag": pl.String,
    "data_file": pl.String,
    "variable_tag": pl.String,
    "timeslice_tag": pl.String,
    "timeslice": pl.String,
    "timeslice_value": pl.Float32,
}
COLUMNS = [
    "name",
    "property_name",
    "property_value",
    "band",
    "scenario",
    "date_from",
    "date_to",
    "text",
]
DEFAULT_INDEX = ["object_id", "name", "category"]


def cli_arguments(parser: ArgumentParser):
    """CLI arguments for the plugin."""
    parser.add_argument(
        "--model",
        required=False,
        help="Plexos model to translate",
    )


class PlexosParser(PCMParser):
    """Plexos parser class."""

    def __init__(self, *args, xml_file: str | None = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        assert self.config.run_folder
        self.run_folder = Path(self.config.run_folder)
        self.system = System(name=self.config.name, auto_add_composed_components=True)
        self.property_map = self.config.defaults["plexos_property_map"]
        self.device_map = self.config.defaults["plexos_device_map"]
        self.fuel_map = self.config.defaults["plexos_fuel_map"]
        self.device_match_string = self.config.defaults["device_name_inference_map"]

        # TODO(pesap): Rename exceptions to include R2X
        # https://github.com/NREL/R2X/issues/5
        # R2X needs at least one of this maps defined to correctly work.
        if not self.fuel_map and not self.device_map and not self.device_match_string:
            msg = (
                "Neither `plexos_fuel_map` or `plexos_device_map` or `device_match_string` was provided. "
                "To fix, provide any of the mappings."
            )
            raise ParserError(msg)

        # Populate databse from XML file.
        xml_file = xml_file or self.run_folder / self.config.fmap["xml_file"]["fname"]
        self.db = PlexosSQLite(xml_fname=xml_file)

        # Extract scenario data
        model_name = getattr(self.config, "model", None)
        logger.debug("Parsing plexos model={}", model_name)
        if model_name is None:
            model_name = self._select_model_name()
        self._process_scenarios(model_name=model_name)

        # date from is in days since 1900, convert to year
        date_from = self._collect_horizon_data(model_name=model_name).get("Date From")
        if date_from is not None:
            self.study_year: int = int((date_from / 365.25) + 1900)
            self.config.weather_year = self.study_year
        else:
            if self.config.weather_year is None:
                msg = (
                    "Weather year can not be None if the model does not provide a year."
                    "Check for correct model name"
                )
                raise ValueError(msg)
            self.study_year = self.config.weather_year

    def build_system(self) -> System:
        """Create infrasys system."""
        logger.info("Building infrasys system using {}", self.__class__.__name__)

        # If we decide to change the engine for handling the data we can do it here.
        object_data = self._plexos_table_data()
        self.plexos_data = self._polarize_data(object_data=object_data)

        # Construct the network
        self._construct_load_zones()
        self._construct_buses()
        self._construct_branches()
        self._construct_interfaces()
        self._construct_reserves()

        # Generators
        self._construct_generators()
        self._add_buses_to_generators()
        self._add_generator_reserves()

        # Batteries
        self._construct_batteries()
        self._add_buses_to_batteries()
        self._add_battery_reserves()

        self._construct_load_profiles()
        return self.system

    def _collect_horizon_data(self, model_name: str) -> dict[str, float]:
        """Collect horizon data (Date From/To) from Plexos database."""
        horizon_query = f"""
        SELECT
            atr.name AS attribute_name,
            COALESCE(attr_data.value, atr.default_value) AS attr_val
        FROM
            t_object
        LEFT JOIN t_class AS class ON
            t_object.class_id == class.class_id
        LEFT JOIN t_attribute AS atr ON
            t_object.class_id  == atr.class_id
        LEFT JOIN t_membership AS tm ON
            t_object.object_id  == tm.child_object_id
        LEFT JOIN t_class AS parent_class ON
            tm.parent_class_id == parent_class.class_id
        LEFT JOIN t_object AS to2 ON
            tm.parent_object_id == to2.object_id
        LEFT JOIN t_attribute_data attr_data ON
            attr_data.attribute_id == atr.attribute_id AND t_object.object_id == attr_data.object_id
        WHERE
            class.name == '{ClassEnum.Horizon.value}'
            AND parent_class.name == '{ClassEnum.Model.value}'
            AND to2.name == '{model_name}'
        """
        horizon_data = self.db.query(horizon_query)
        horizon_map = {key: value for key, value in horizon_data}
        return horizon_map

    def _reconcile_timeseries(self, data_file):
        """
        Adjust timesseries data to match the study year datetime
        index, such as removing or adding leap-year data.
        """
        date_time_column = pd.date_range(
            start=f"1/1/{self.study_year}",
            end=f"1/1/{self.study_year + 1}",
            freq="1h",
            inclusive="left",
        )
        leap_year = len(date_time_column) == 8784

        if data_file.height in [8784, 8760]:
            if data_file.height == 8784 and not leap_year:
                before_feb_29 = data_file.slice(0, 1416)
                after_feb_29 = data_file.slice(1440, len(data_file) - 1440)
                return pl.concat([before_feb_29, after_feb_29])
            elif data_file.height == 8760 and leap_year:
                return handle_leap_year_adjustment(data_file)
            return data_file

        if data_file.height <= 8760:
            return fill_missing_timestamps(data_file, date_time_column)

        if data_file.height in [17568, 17520]:
            return resample_data_to_hourly(data_file)

        return data_file

    def _get_fuel_prices(self):
        logger.debug("Creating fuel representation")
        system_fuels = (pl.col("child_class_name") == ClassEnum.Fuel.value) & (
            pl.col("parent_class_name") == ClassEnum.System.value
        )
        fuels = self._get_model_data(system_fuels)
        fuel_prices = {}
        for fuel_name, fuel_data in fuels.group_by("name"):
            fuel_name = fuel_name[0]
            property_records = fuel_data[
                [
                    "band",
                    "property_name",
                    "property_value",
                    "property_unit",
                    "data_file",
                    "variable",
                    "action",
                    "variable_tag",
                    "timeslice",
                    "timeslice_value",
                ]
            ].to_dicts()

            for property in property_records:
                property.update({"property_unit": "$/MMBtu"})

            mapped_records, multi_band_records = self._parse_property_data(property_records, fuel_name)
            mapped_records["name"] = fuel_name
            fuel_prices[fuel_name] = mapped_records["Price"]
        return fuel_prices

    def _construct_load_zones(self, default_model=LoadZone) -> None:
        """Create LoadZone representation.

        Plexos can define load at multiple levels, but for balancing the load,
        we assume that it happens at the region level, which is a typical way
        of doing it.
        """
        logger.debug("Creating load zone representation")
        system_regions = (pl.col("child_class_name") == ClassEnum.Region.value) & (
            pl.col("parent_class_name") == ClassEnum.System.value
        )
        regions = self._get_model_data(system_regions)

        region_pivot = regions.pivot(  # noqa: PD010
            index=DEFAULT_INDEX,
            columns="property_name",
            values="property_value",
            aggregate_function="first",
        )
        for region in region_pivot.iter_rows(named=True):
            valid_fields, ext_data = field_filter(region, default_model.model_fields)
            valid_fields = prepare_ext_field(valid_fields, ext_data)
            self.system.add_component(default_model(**valid_fields))
        return

    def _construct_buses(self, default_model=ACBus) -> None:
        logger.debug("Creating buses representation")
        system_buses = (pl.col("child_class_name") == ClassEnum.Node.value) & (
            pl.col("parent_class_name") == ClassEnum.System.value
        )
        region_buses = (pl.col("child_class_name") == ClassEnum.Region.value) & (
            pl.col("parent_class_name") == ClassEnum.Node.value
        )
        system_buses = self._get_model_data(system_buses)
        buses_region = self._get_model_data(region_buses)
        buses = system_buses.pivot(  # noqa: PD010
            index=DEFAULT_INDEX,
            columns="property_name",
            values="property_value",
            aggregate_function="first",
        )
        for idx, bus in enumerate(buses.iter_rows(named=True)):
            mapped_bus = {self.property_map.get(key, key): value for key, value in bus.items()}

            valid_fields, ext_data = field_filter(mapped_bus, default_model.model_fields)

            # Get region from buses region memberships
            region_name = buses_region.filter(pl.col("parent_object_id") == bus["object_id"])["name"].item()

            valid_fields["load_zone"] = self.system.get_component(LoadZone, name=region_name)

            # NOTE: We do not parser differenet kind of buses from Plexos
            valid_fields["bus_type"] = ACBusTypes.PV

            valid_fields["base_voltage"] = (
                230.0 if not valid_fields.get("base_voltage") else valid_fields["base_voltage"]
            )

            valid_fields = prepare_ext_field(valid_fields, ext_data)
            self.system.add_component(default_model(number=idx + 1, **valid_fields))
        return

    def _construct_reserves(self, default_model=Reserve):
        logger.debug("Creating reserve representation")
        system_reserves = (pl.col("child_class_name") == ClassEnum.Reserve.name) & (
            pl.col("parent_class_name") == ClassEnum.System.name
        )
        system_reserves = self._get_model_data(system_reserves)

        reserve_pivot = system_reserves.pivot(  # noqa: PD010
            index=DEFAULT_INDEX,
            columns="property_name",
            values="property_value",
            aggregate_function="first",
        )
        for reserve in reserve_pivot.iter_rows(named=True):
            mapped_reserve = {self.property_map.get(key, key): value for key, value in reserve.items()}
            valid_fields, ext_data = field_filter(mapped_reserve, default_model.model_fields)

            if ext_data:
                # Add reserve type and direction based on Plexos type. If the
                # key is not present, the assumed one is the default (Spinning.Up)
                reserve_type = validate_string(ext_data.pop("Type", "default"))

                # Mapping is integer, but parsing reads it as float.
                if isinstance(reserve_type, float):
                    reserve_type = int(reserve_type)

                # If we do not support the reserve type yield the default one.
                plexos_reserve_map = self.config.defaults["reserve_types"].get(
                    str(reserve_type), self.config.defaults["reserve_types"]["default"]
                )  # Pass string so we do not need to convert the json mapping.

                valid_fields["reserve_type"] = ReserveType[plexos_reserve_map["type"]]
                valid_fields["direction"] = ReserveDirection[plexos_reserve_map["direction"]]

                valid_fields = prepare_ext_field(valid_fields, ext_data)

            self.system.add_component(default_model(**valid_fields))

        reserve_map = ReserveMap(name="contributing_generators")
        self.system.add_component(reserve_map)
        return

    def _construct_branches(self, default_model=MonitoredLine):
        logger.debug("Creating lines")
        system_lines = (pl.col("child_class_name") == ClassEnum.Line.value) & (
            pl.col("parent_class_name") == ClassEnum.System.value
        )
        system_lines = self._get_model_data(system_lines)
        lines_pivot = system_lines.pivot(  # noqa: PD010
            index=DEFAULT_INDEX,
            columns="property_name",
            values="property_value",
            aggregate_function="first",
        )

        lines_pivot_memberships = self.db.get_memberships(
            *lines_pivot["name"].to_list(), object_class=ClassEnum.Line
        )
        for line in lines_pivot.iter_rows(named=True):
            line_properties_mapped = {self.property_map.get(key, key): value for key, value in line.items()}
            line_properties_mapped["rating"] = line_properties_mapped.pop("max_power_flow", None)
            line_properties_mapped["rating_up"] = line_properties_mapped.pop("max_power_flow", None)
            line_properties_mapped["rating_down"] = line_properties_mapped.pop("min_power_flow", None)

            valid_fields, ext_data = field_filter(line_properties_mapped, default_model.model_fields)

            from_bus_name = next(
                membership
                for membership in lines_pivot_memberships
                if (
                    membership[2] == line["name"]
                    and membership[5] == ClassEnum.Line.name
                    and membership[6].replace(" ", "") == CollectionEnum.NodeFrom.name
                )
            )[3]
            from_bus = self.system.get_component(ACBus, from_bus_name)
            to_bus_name = next(
                membership
                for membership in lines_pivot_memberships
                if (
                    membership[2] == line["name"]
                    and membership[5] == ClassEnum.Line.name
                    and membership[6].replace(" ", "") == CollectionEnum.NodeTo.name
                )
            )[3]
            to_bus = self.system.get_component(ACBus, to_bus_name)
            valid_fields["from_bus"] = from_bus
            valid_fields["to_bus"] = to_bus

            valid_fields = prepare_ext_field(valid_fields, ext_data)
            self.system.add_component(default_model(**valid_fields))
        return

    def _infer_model_type(self, generator_name):
        inference_mapper = self.device_match_string
        generator_name_lower = generator_name.lower()
        for key, device_info in inference_mapper.items():
            if key in generator_name_lower:
                return device_info
        return None

    def _get_fuel_pmtype(self, generator_name, generator_fuel_map):
        plexos_fuel_name = generator_fuel_map.get(generator_name)
        logger.trace("Parsing generator = {} with fuel type = {}", generator_name, plexos_fuel_name)

        fuel_pmtype = (
            self.device_map.get(generator_name)
            or self.fuel_map.get(plexos_fuel_name)
            or self._infer_model_type(generator_name)
        )

        return fuel_pmtype, plexos_fuel_name

    def _get_model_type(self, fuel_pmtype):
        if fuel_pmtype is not None:
            for model, conditions in self.config.defaults["generator_models"].items():
                for cond in conditions:
                    if (cond["fuel"] == fuel_pmtype["fuel"] or cond["fuel"] is None) and (
                        cond["type"] == fuel_pmtype["type"] or cond["type"] is None
                    ):
                        return model
        return ""

    def _construct_generators(self):
        logger.debug("Creating generators")
        system_generators = (pl.col("child_class_name") == ClassEnum.Generator.name) & (
            pl.col("parent_class_name") == ClassEnum.System.name
        )

        system_generators = self._get_model_data(system_generators)
        if getattr(self.config.feature_flags, "plexos-csv", None):
            system_generators.write_csv("generators.csv")
        # NOTE: The best way to identify the type of generator on Plexos is by reading the fuel
        fuel_query = f"""
        SELECT
            parent_obj.name AS parent_object_name,
            child_obj.name AS fuel_name
        FROM
            t_membership AS mem
            LEFT JOIN t_object AS child_obj ON mem.child_object_id = child_obj.object_id
            LEFT JOIN t_object AS parent_obj ON mem.parent_object_id = parent_obj.object_id
            LEFT JOIN t_class AS child_cls ON child_obj.class_id = child_cls.class_id
            LEFT JOIN t_class AS parent_cls ON parent_obj.class_id = parent_cls.class_id
        WHERE
            child_cls.name = '{ClassEnum.Fuel.value}'
            AND parent_cls.name = '{ClassEnum.Generator.value}'
        """
        generator_fuel = self.db.query(fuel_query)
        generator_fuel_map = {key: value for key, value in generator_fuel}
        fuel_prices = self._get_fuel_prices()

        # Iterate over properties for generator
        for generator_name, generator_data in system_generators.group_by("name"):
            generator_name = generator_name[0]
            fuel_pmtype, plexos_fuel_name = self._get_fuel_pmtype(generator_name, generator_fuel_map)
            model_map = self._get_model_type(fuel_pmtype)
            if getattr(R2X_MODELS, model_map, None) is None:
                logger.warning(
                    "Model map not found for generator={} with fuel_type={}. Skipping it.",
                    generator_name,
                    plexos_fuel_name,
                )
                continue
            fuel_type, pm_type = fuel_pmtype["fuel"], fuel_pmtype["type"]
            model_map = getattr(R2X_MODELS, model_map)

            property_records = generator_data[
                [
                    "band",
                    "property_name",
                    "property_value",
                    "property_unit",
                    "data_file",
                    "variable",
                    "action",
                    "variable_tag",
                    "timeslice",
                    "timeslice_value",
                ]
            ].to_dicts()
            mapped_records, multi_band_records = self._parse_property_data(property_records, generator_name)
            mapped_records["name"] = generator_name

            # if multi_band_records:
            #     pass

            # Add prime mover mapping
            mapped_records["prime_mover_type"] = pm_type or self.config.plexos_fuel_map["default"].get("type")
            mapped_records["prime_mover_type"] = PrimeMoversType[mapped_records["prime_mover_type"]]
            mapped_records["fuel"] = fuel_type or self.config.prime_mover_map["default"].get("fuel")

            # match model_map:
            #     case HydroPumpedStorage():
            #         mapped_records["prime_mover_type"] = PrimeMoversType.PS

            # Pumped Storage generators are not required to have Max Capacity property
            if "max_active_power" not in mapped_records and "pump_load" in mapped_records:
                mapped_records["rating"] = mapped_records["pump_load"]

            mapped_records = self._set_unit_availability(mapped_records)
            if mapped_records is None:
                # When unit availability is not set, we skip the generator
                continue

            required_fields = {
                key: value for key, value in model_map.model_fields.items() if value.is_required()
            }
            if not all(key in mapped_records for key in required_fields):
                missing_fields = [key for key in required_fields if key not in mapped_records]
                logger.warning(
                    "Skipping Generator {}. Missing Required Fields: {}", generator_name, missing_fields
                )
                continue

            mapped_records["fuel_price"] = fuel_prices.get(generator_fuel_map.get(generator_name), 0)
            mapped_records = self._construct_operating_costs(mapped_records, generator_name, model_map)

            mapped_records["base_mva"] = 1

            valid_fields, ext_data = field_filter(mapped_records, model_map.model_fields)

            ts_fields = {k: v for k, v in mapped_records.items() if isinstance(v, SingleTimeSeries)}
            valid_fields.update(
                {
                    ts_name: np.mean(ts.data)
                    for ts_name, ts in ts_fields.items()
                    if ts_name in valid_fields.keys()
                }
            )

            valid_fields = prepare_ext_field(valid_fields, ext_data)
            self.system.add_component(model_map(**valid_fields))
            generator = self.system.get_component_by_label(f"{model_map.__name__}.{generator_name}")

            if ts_fields:
                generator = self.system.get_component_by_label(f"{model_map.__name__}.{generator_name}")
                ts_dict = {"solve_year": self.study_year}
                for ts_name, ts in ts_fields.items():
                    ts.variable_name = ts_name
                    self.system.add_time_series(ts, generator, **ts_dict)

    def _add_buses_to_generators(self):
        # Add buses to generators
        generators = [generator["name"] for generator in self.system.to_records(Generator)]
        generator_memberships = self.db.get_memberships(
            *generators,
            object_class=ClassEnum.Generator,
            collection=CollectionEnum.Nodes,
        )
        for generator in self.system.get_components(Generator):
            buses = [membership for membership in generator_memberships if membership[2] == generator.name]
            if buses:
                for bus in buses:
                    try:
                        bus_object = self.system.get_component(ACBus, name=bus[3])
                    except ISNotStored:
                        logger.warning(
                            "Skipping membership for generator:{} since reserve {} is not stored",
                            generator.name,
                            buses[3],
                        )
                        continue
                    generator.bus = bus_object
        return

    def _add_generator_reserves(self):
        reserve_map = self.system.get_component(ReserveMap, name="contributing_generators")
        generators = [generator["name"] for generator in self.system.to_records(Generator)]
        generator_memberships = self.db.get_memberships(
            *generators,
            object_class=ClassEnum.Generator,
            parent_class=ClassEnum.Reserve,
            collection=CollectionEnum.Generators,
        )
        for generator in self.system.get_components(Generator):
            reserves = [membership for membership in generator_memberships if membership[3] == generator.name]
            if reserves:
                # NOTE: This would get replaced if we have a method on infrasys
                # that check if something exists on the system
                for reserve in reserves:
                    try:
                        reserve_object = self.system.get_component(Reserve, name=reserve[2])
                    except ISNotStored:
                        logger.warning(
                            "Skipping membership for generator:{} since reserve {} is not stored",
                            generator.name,
                            reserve[2],
                        )
                        continue
                    reserve_map.mapping[reserve_object.name].append(generator.name)

        return

    def _construct_batteries(self):
        logger.debug("Creating battery objects")
        system_batteries = self._get_model_data(
            (pl.col("child_class_name") == ClassEnum.Battery.name)
            & (pl.col("parent_class_name") == ClassEnum.System.name)
        )

        required_fields = {
            key: value for key, value in GenericBattery.model_fields.items() if value.is_required()
        }

        for battery_name, battery_data in system_batteries.group_by("name"):
            battery_name = battery_name[0]
            logger.trace("Parsing battery = {}", battery_name)
            property_records = battery_data[
                [
                    "band",
                    "property_name",
                    "property_value",
                    "property_unit",
                    "data_file",
                    "variable",
                    "action",
                    "variable_tag",
                    "timeslice",
                    "timeslice_value",
                ]
            ].to_dicts()

            mapped_records, _ = self._parse_property_data(property_records, battery_name)
            mapped_records["name"] = battery_name

            if "Max Power" in mapped_records:
                mapped_records["rating"] = mapped_records["Max Power"]

            if "Capacity" in mapped_records:
                mapped_records["storage_capacity"] = mapped_records["Capacity"]

            mapped_records["prime_mover_type"] = PrimeMoversType.BA

            valid_fields, ext_data = field_filter(mapped_records, GenericBattery.model_fields)

            valid_fields = self._set_unit_availability(valid_fields)
            if valid_fields is None:
                continue

            if not all(key in valid_fields for key in required_fields):
                missing_fields = [key for key in required_fields if key not in valid_fields]
                logger.warning(
                    "Skipping battery {}. Missing required fields: {}", battery_name, missing_fields
                )
                continue

            if mapped_records["storage_capacity"] == 0:
                logger.warning("Skipping battery {} since it has zero capacity", battery_name)
                continue

            valid_fields = prepare_ext_field(valid_fields, ext_data)
            self.system.add_component(GenericBattery(**valid_fields))
        return

    def _add_buses_to_batteries(self):
        batteries = [battery["name"] for battery in self.system.to_records(GenericBattery)]
        if not batteries:
            msg = "No battery objects found on the system. Skipping adding membership to buses."
            logger.warning(msg)
            return
        generator_memberships = self.db.get_memberships(
            *batteries,
            object_class=ClassEnum.Battery,
            collection=CollectionEnum.Nodes,
        )
        for component in self.system.get_components(GenericBattery):
            buses = [membership for membership in generator_memberships if membership[2] == component.name]
            if buses:
                for bus in buses:
                    try:
                        bus_object = self.system.get_component(ACBus, name=bus[3])
                    except ISNotStored:
                        logger.warning(
                            "Skipping membership for battery:{} since bus {} is not stored",
                            component.name,
                            buses[3],
                        )
                        continue
                    component.bus = bus_object
        return

    def _add_battery_reserves(self):
        reserve_map = self.system.get_component(ReserveMap, name="contributing_generators")
        batteries = [battery["name"] for battery in self.system.to_records(GenericBattery)]
        if not batteries:
            msg = "No battery objects found on the system. Skipping adding membership to buses."
            logger.warning(msg)
            return
        generator_memberships = self.db.get_memberships(
            *batteries,
            object_class=ClassEnum.Battery,
            parent_class=ClassEnum.Reserve,
            collection=CollectionEnum.Batteries,
        )
        for battery in self.system.get_components(GenericBattery):
            reserves = [membership for membership in generator_memberships if membership[3] == battery.name]
            if reserves:
                # NOTE: This would get replaced if we have a method on infrasys
                # that check if something exists on the system
                for reserve in reserves:
                    try:
                        reserve_object = self.system.get_component(Reserve, name=reserve[2])
                    except ISNotStored:
                        logger.warning(
                            "Skipping membership for generator:{} since reserve {} is not stored",
                            battery.name,
                            reserve[2],
                        )
                        continue
                    reserve_map.mapping[reserve_object.name].append(battery.name)
        return

    def _construct_interfaces(self, default_model=TransmissionInterface):
        """Construct Transmission Interface and Transmission Interface Map."""
        logger.debug("Creating transmission interfaces")
        system_interfaces_mask = (pl.col("child_class_name") == ClassEnum.Interface.name) & (
            pl.col("parent_class_name") == ClassEnum.System.name
        )
        system_interfaces = self._get_model_data(system_interfaces_mask)
        interfaces = system_interfaces.pivot(  # noqa: PD010
            index=DEFAULT_INDEX,
            columns="property_name",
            values="property_value",
            aggregate_function="first",
        )

        interface_property_map = {
            v: k
            for k, v in self.config.defaults["plexos_property_map"].items()
            if k in default_model.model_fields
        }

        tx_interface_map = TransmissionInterfaceMap(name="transmission_map")
        for interface in interfaces.iter_rows(named=True):
            mapped_interface = {
                interface_property_map.get(key, key): value for key, value in interface.items()
            }
            valid_fields, ext_data = field_filter(mapped_interface, default_model.model_fields)

            # Check that the interface has all the required fields of the model.
            required_fields = {
                key: value for key, value in default_model.model_fields.items() if value.is_required()
            }
            if not all(key in mapped_interface for key in required_fields):
                missing_fields = [key for key in required_fields if key not in mapped_interface]
                logger.warning(
                    "{}:{} missing required fields: {}. Skipping it.",
                    default_model.__name__,
                    interface["name"],
                    missing_fields,
                )
                continue

            valid_fields = prepare_ext_field(valid_fields, ext_data)
            self.system.add_component(default_model(**valid_fields))

        # Add lines memberships
        lines = [line["name"] for line in self.system.to_records(MonitoredLine)]
        lines_memberships = self.db.get_memberships(
            *lines,
            object_class=ClassEnum.Line,
            parent_class=ClassEnum.Interface,
            collection=CollectionEnum.Lines,
        )
        for line in self.system.get_components(MonitoredLine):
            interface = next(
                (membership for membership in lines_memberships if membership[3] == line.name), None
            )
            if interface:
                # NOTE: This would get replaced if we have a method on infrasys
                # that check if something exists on the system
                try:
                    interface_object = self.system.get_component(TransmissionInterface, name=interface[2])
                except ISNotStored:
                    logger.warning(
                        "Skipping membership for line:{} since interface {} is not stored",
                        line.name,
                        interface[2],
                    )
                    continue
                tx_interface_map.mapping[interface_object.name].append(line.label)
        self.system.add_component(tx_interface_map)
        return

    def _construct_value_curves(self, mapped_records, generator_name):
        """Construct value curves for generators."""
        if any("Heat Rate" in key or "heat_rate" in key for key in mapped_records.keys()):
            vc = None
            heat_rate_avg = mapped_records.get("heat_rate", None)
            heat_rate_base = mapped_records.get("Heat Rate Base", None)
            heat_rate_incr = mapped_records.get("Heat Rate Incr", None)
            heat_rate_incr2 = mapped_records.get("Heat Rate Incr2", None)
            if any(
                isinstance(val, SingleTimeSeries) for val in [heat_rate_avg, heat_rate_base, heat_rate_incr]
            ):
                logger.warning("Market-Bid Cost not implemented for generator={}", generator_name)
                return mapped_records
            if heat_rate_avg:
                fn = LinearFunctionData(proportional_term=heat_rate_avg.magnitude, constant_term=0)
                vc = AverageRateCurve(
                    name=f"{generator_name}_HR",
                    function_data=fn,
                    initial_input=heat_rate_avg.magnitude,
                )
            elif heat_rate_incr2 and "** 2" in str(heat_rate_incr2.units):
                fn = QuadraticFunctionData(
                    quadratic_term=heat_rate_incr2.magnitude,
                    proportional_term=heat_rate_incr.magnitude,
                    constant_term=heat_rate_base.magnitude,
                )
            elif not heat_rate_incr2 and heat_rate_incr:
                fn = LinearFunctionData(
                    proportional_term=heat_rate_incr.magnitude, constant_term=heat_rate_base.magnitude
                )
            else:
                logger.warning("Heat Rate type not implemented for generator={}", generator_name)
                fn = None
            if not vc:
                vc = InputOutputCurve(name=f"{generator_name}_HR", function_data=fn)
            mapped_records["hr_value_curve"] = vc
        return mapped_records

    def _construct_operating_costs(self, mapped_records, generator_name, model_map):
        """Construct operating costs from Value Curves and Operating Costs."""
        mapped_records = self._construct_value_curves(mapped_records, generator_name)
        hr_curve = mapped_records.get("hr_value_curve")

        if issubclass(model_map, RenewableGen):
            mapped_records["operation_cost"] = RenewableGenerationCost()
        elif issubclass(model_map, ThermalGen):
            if hr_curve:
                fuel_cost = mapped_records["fuel_price"]
                if isinstance(fuel_cost, SingleTimeSeries):
                    fuel_cost = np.mean(fuel_cost.data)
                elif isinstance(fuel_cost, Quantity):
                    fuel_cost = fuel_cost.magnitude
                fuel_curve = FuelCurve(value_curve=hr_curve, fuel_cost=fuel_cost)
                mapped_records["operation_cost"] = ThermalGenerationCost(variable=fuel_curve)
                mapped_records.pop("hr_value_curve")
            else:
                logger.warning("No heat rate curve found for generator={}", generator_name)
        elif issubclass(model_map, HydroDispatch):
            mapped_records["operation_cost"] = HydroGenerationCost()
        else:
            logger.warning(
                "Operating Cost not implemented for generator={} model map={}", generator_name, model_map
            )
        return mapped_records

    def _select_model_name(self):
        # TODO(pesap): Handle exception if no model name found
        # https://github.com/NREL/R2X/issues/10
        query = f"""
        select obj.name
        from t_object as obj
        left join t_class as cls on obj.class_id = cls.class_id
        where cls.name = '{ClassEnum.Model.name}'
        """
        result = self.db.query(query)
        model_names = [row[0] for row in result]

        print("Available models:")
        for idx, name in enumerate(model_names):
            print(f"{idx + 1}. {name}")

        while True:
            try:
                choice = int(input("Select a model by number: "))
                if 1 <= choice <= len(model_names):
                    return model_names[choice - 1]
                else:
                    print(f"Please enter a number between 1 and {len(model_names)}.")
            except ValueError:
                print("Invalid input. Please enter a number.")

    def _process_scenarios(self, model_name: str | None = None) -> None:
        """Create a SQLite representation of the XML."""
        if model_name is None:
            msg = "Required model name not found. Parser requires a model to parse from the Plexos database"
            raise ModelError(msg)
        # self.db = PlexosSQLite(xml_fname=xml_fpath)

        logger.trace("Getting object_id for model={}", model_name)
        model_id = self.db.query("select object_id from t_object where name = ?", params=(model_name,))
        if len(model_id) > 1:
            msg = f"Multiple models with the same {model_name} returned. Check database or spelling."
            logger.debug(model_id)
            raise ModelError(msg)
        if not model_id:
            msg = f"Model `{model_name}` not found on the XML. Check spelling of the `model_name`."
            raise ParserError(msg)
        self.model_id = model_id[0][0]  # Unpacking tuple [(model_id,)]

        # NOTE: When doing performance updates this query could get some love.
        valid_scenarios = self.db.query(
            "select obj.name from t_membership mem "
            "left join t_object as obj on obj.object_id = mem.child_object_id "
            "left join t_class as cls on cls.class_id = obj.class_id "
            f"where mem.parent_object_id = {self.model_id} and cls.name = '{ClassEnum.Scenario}'"
        )
        if not valid_scenarios:
            msg = f"{model_name=} does not have any scenario attached to it."
            logger.warning(msg)
            return
        assert valid_scenarios
        self.scenarios = [scenario[0] for scenario in valid_scenarios]  # Flatten list of tuples
        return None

    def _set_unit_availability(self, mapped_records):
        """
        Set availability and active power limit TS for generators.
        Note: variables use infrasys naming scheme, rating != plexos rating.
        """
        # TODO @ktehranchi: #35 Include date_from and date_to in the availability
        # https://github.com/NREL/R2X/issues/35

        availability = mapped_records.get("available", None)
        if availability is not None and availability > 0:
            # Set availability, rating, storage_capacity as multiplier of availability/'units'
            if mapped_records.get("storage_capacity") is not None:
                mapped_records["storage_capacity"] *= mapped_records.get("available")
            mapped_records["rating"] = mapped_records.get("rating") * mapped_records.get("available")
            mapped_records["available"] = 1

            # Set active power limits
            rating_factor = mapped_records.get("Rating Factor", 100)
            rating_factor = self._apply_action(np.divide, rating_factor, 100)
            rating = mapped_records.get("rating", None)
            max_active_power = mapped_records.get("max_active_power", None)
            min_energy_hour = mapped_records.get("Min Energy Hour", None)

            if isinstance(max_active_power, dict):
                max_active_power = self._time_slice_handler("max_active_power", max_active_power)

            if max_active_power is not None:
                units = max_active_power.units
                val = self._apply_action(np.multiply, rating_factor, max_active_power)
            elif rating is not None:
                units = rating.units
                val = self._apply_action(np.multiply, rating_factor, rating.magnitude)
            else:
                return mapped_records
            val = self._apply_unit(val, units)

            if min_energy_hour is not None:
                mapped_records["min_active_power"] = mapped_records.pop("Min Energy Hour")

            if isinstance(val, SingleTimeSeries):
                val.variable_name = "max_active_power"
                self._apply_action(np.divide, val, rating.magnitude)
                mapped_records["max_active_power"] = val
            mapped_records["active_power"] = rating

            if isinstance(rating_factor, SingleTimeSeries):
                mapped_records.pop("Rating Factor")  # rm for ext exporter

        else:  # if unit field not activated in model, skip generator
            mapped_records = None
        return mapped_records

    def _plexos_table_data(self) -> list[tuple]:
        # Get objects table/membership table
        sql_query = files("plexosdb.queries").joinpath("object_query.sql").read_text()
        object_data = self.db.query(sql_query)
        return object_data

    def _polarize_data(self, object_data: list[tuple]) -> pl.DataFrame:
        return pl.from_records(object_data, schema=DEFAULT_QUERY_COLUMNS_SCHEMA)

    def _get_model_data(self, data_filter) -> pl.DataFrame:
        """Filter plexos data for a given class and all scenarios in a model."""
        scenario_specific_data = None
        scenario_filter = None
        if getattr(self, "scenarios", None):
            scenario_filter = pl.col("scenario").is_in(self.scenarios)
            scenario_specific_data = self.plexos_data.filter(data_filter & scenario_filter)
            scenario_specific_data = filter_property_dates(scenario_specific_data, self.study_year)

        base_case_filter = pl.col("scenario").is_null()
        # Default is to parse data normally if there is not scenario. If scenario exist modify the filter.
        if scenario_specific_data is None:
            system_data = self.plexos_data.filter(data_filter & base_case_filter)
            system_data = filter_property_dates(system_data, self.study_year)
        else:
            # include both scenario specific and basecase data
            combined_key_base = pl.col("name") + "_" + pl.col("property_name")
            combined_key_scenario = (
                scenario_specific_data["name"] + "_" + scenario_specific_data["property_name"]
            )

            base_case_filter = base_case_filter & (
                ~combined_key_base.is_in(combined_key_scenario) | pl.col("property_name").is_null()
            )
            base_case_data = self.plexos_data.filter(data_filter & base_case_filter)
            base_case_data = filter_property_dates(base_case_data, self.study_year)

            system_data = pl.concat([scenario_specific_data, base_case_data])

        # If date_from / date_to is specified, override the base_case_value
        rows_to_keep = system_data.filter(pl.col("date_from").is_not_null() | pl.col("date_to").is_not_null())
        rtk_key = rows_to_keep["name"] + "_" + rows_to_keep["property_name"]
        sys_data_key = system_data["name"] + "_" + system_data["property_name"]
        if not rows_to_keep.is_empty():
            # remove if name and property_name are the same
            system_data = system_data.filter(~sys_data_key.is_in(rtk_key))
            system_data = pl.concat([system_data, rows_to_keep])

        # Get System Variables
        variable_data = None
        variable_filter = (
            (pl.col("child_class_name") == ClassEnum.Variable.name)
            & (pl.col("parent_class_name") == ClassEnum.System.name)
            & (pl.col("data_file").is_not_null())
        )
        variable_scenario_data = None
        if scenario_specific_data is not None and scenario_filter is not None:
            variable_scenario_data = self.plexos_data.filter(variable_filter & scenario_filter)

        if variable_scenario_data is not None:
            variable_base_data = self.plexos_data.filter(variable_filter & pl.col("scenario").is_null())
        else:
            variable_base_data = self.plexos_data.filter(variable_filter & base_case_filter)
        if variable_base_data is not None and variable_scenario_data is not None:
            variable_data = pl.concat([variable_scenario_data, variable_base_data])

        return self._join_variable_data(system_data, variable_data)

    def _join_variable_data(self, system_data, variable_data):
        """Join system data with variable data."""
        # Filter Variables
        if variable_data is not None:
            results = []
            grouped = variable_data.group_by("name")
            for group_name, group_df in grouped:
                if group_df.height > 1:
                    # Check if any scenario_name exists
                    scenario_exists = group_df.filter(pl.col("scenario").is_not_null())

                    if scenario_exists.height > 0:
                        # Select the first row with a scenario_name
                        selected_row = scenario_exists[0]
                    else:
                        # If no scenario_name, select the row with the lowest band_id
                        selected_row = group_df.sort("band").head(1)[0]
                else:
                    # If the group has only one row, select that row
                    selected_row = group_df[0]

                results.append(
                    {
                        "name": group_name[0],
                        "variable_name": selected_row["data_file_tag"][0],
                        "variable": selected_row["data_file"][0],
                    }
                )
            variables_filtered = pl.DataFrame(results)
            system_data = system_data.join(
                variables_filtered, left_on="variable_tag", right_on="name", how="left"
            )
        else:
            # NOTE: We might want to include this at the instead of each function call
            system_data = system_data.with_columns(
                pl.lit(None).alias("variable"), pl.lit(None).alias("variable_name")
            )
        system_data = system_data.join(
            variables_filtered, left_on="variable_tag", right_on="name", how="left"
        )
        return system_data

    def _construct_load_profiles(self):
        logger.debug("Creating load profile representation")
        system_regions = (pl.col("child_class_name") == ClassEnum.Region.name) & (
            pl.col("parent_class_name") == ClassEnum.System.name
        )
        regions = self._get_model_data(system_regions).filter(~pl.col("variable").is_null())
        for region, region_data in regions.group_by("name"):
            if not len(region_data) == 1:
                msg = (
                    "load data has more than one row for {}. Selecting the first match. "
                    "Check filtering of properties"
                )
                logger.warning(msg, region)

            ts = self._csv_file_handler(
                property_name="max_active_power", property_data=region_data["variable"][0]
            )
            max_load = np.max(ts.data.to_numpy())
            ts = self._apply_action(np.divide, ts, max_load)
            if not ts:
                continue

            bus_region_membership = self.db.get_memberships(
                region[0],
                object_class=ClassEnum.Region,
                parent_class=ClassEnum.Node,
                collection=CollectionEnum.Region,
            )
            for bus in bus_region_membership:
                bus = self.system.get_component(ACBus, name=bus[2])
                load = PowerLoad(
                    name=f"{bus.name}",
                    bus=bus,
                    max_active_power=float(max_load / len(bus_region_membership)) * ureg.MW,
                )
                self.system.add_component(load)
                ts_dict = {"solve_year": self.study_year}

                self.system.add_time_series(ts, load, **ts_dict)
        return

    def _csv_file_handler(self, property_name, property_data):
        fpath_text = property_data
        if "\\" in fpath_text:
            relative_path = PureWindowsPath(fpath_text)
        else:
            relative_path = Path(fpath_text)
        assert relative_path
        assert self.config.run_folder
        fpath = self.config.run_folder / relative_path
        try:
            data_file = pl.read_csv(
                fpath.as_posix(), infer_schema_length=10000
            )  # This might not work on Windows machines
        except FileNotFoundError:
            logger.warning("File {} not found. Skipping it.", relative_path)
            return

        # Lowercase files
        data_file = data_file.with_columns(pl.col(pl.String).str.to_lowercase()).rename(
            {column: column.lower() for column in data_file.columns}
        )

        single_value_data = self._retrieve_single_value_data(property_name, data_file)
        if single_value_data is not None:
            return single_value_data

        time_series_data = self._retrieve_time_series_data(property_name, data_file)
        if time_series_data is not None:
            return time_series_data
        logger.debug("Skipped file {}", relative_path)

    def _retrieve_single_value_data(self, property_name, data_file):
        if (
            all(column in data_file.columns for column in [property_name.lower(), "year"])
            and "month" not in data_file.columns
        ):
            data_file = data_file.filter(pl.col("year") == self.study_year)
            return data_file[property_name.lower()][0]

        if data_file.columns[:2] == PROPERTY_SV_COLUMNS_BASIC:
            return data_file.filter(pl.col("name") == property_name.lower())["value"][0]

        if data_file.columns == PROPERTY_SV_COLUMNS_NAMEYEAR:  # double check this case
            filter_condition = (pl.col("year") == self.study_year) & (pl.col("name") == property_name.lower())
            try:
                return data_file.filter(filter_condition)["value"][0]
            except IndexError:
                logger.warning("Property {} missing data_file data. Skipping it.", property_name)
                return
        return

    def _retrieve_time_series_data(self, property_name, data_file):
        output_columns = ["year", "month", "day", "hour", "value"]

        if all(column in data_file.columns for column in PROPERTY_TS_COLUMNS_MONTH_PIVOT):
            # Convert these types to ["pattern", "value"]
            data_file = data_file.filter(pl.col("name") == property_name.lower())
            data_file = data_file.melt(id_vars=["name"], variable_name="pattern")
            data_file = data_file.select(["pattern", "value"])

        match data_file.columns:
            case ["pattern", "value"]:
                dt = pl.datetime_range(
                    datetime(self.study_year, 1, 1), datetime(self.study_year, 12, 31), "1h", eager=True
                ).alias("datetime")
                date_df = pl.DataFrame({"datetime": dt})
                date_df = date_df.with_columns(
                    [
                        date_df["datetime"].dt.year().alias("year"),
                        date_df["datetime"].dt.month().alias("month"),
                        date_df["datetime"].dt.day().alias("day"),
                        date_df["datetime"].dt.hour().alias("hour"),
                    ]
                )

                data_file = data_file.with_columns(
                    month=pl.col("pattern").str.extract(r"(\d{2})$").cast(pl.Int8)
                )  # If other patterns exist, this will need to change.
                data_file = date_df.join(data_file.select("month", "value"), on="month", how="inner").select(
                    output_columns
                )

            case ["month", "day", "period", "value"]:
                data_file = data_file.rename({"period": "hour"})
                data_file = data_file.with_columns(pl.lit(self.study_year).alias("year"))
                columns = ["year"] + [col for col in data_file.columns if col != "year"]
                data_file = data_file.select(columns)

            case columns if all(
                column in columns for column in [*PROPERTY_TS_COLUMNS_MDP, property_name.lower()]
            ):
                data_file = data_file.with_columns(pl.lit(self.study_year).alias("year"))
                data_file = data_file.rename({property_name.lower(): "value"})
                data_file = data_file.rename({"period": "hour"})
                data_file = data_file.select(output_columns)

            case columns if all(
                column in columns for column in [*PROPERTY_TS_COLUMNS_YM, property_name.lower()]
            ):
                data_file = data_file.filter(pl.col("year") == self.study_year)
                data_file = data_file.rename({property_name.lower(): "value"})
                data_file = data_file.with_columns(day=pl.lit(1), hour=pl.lit(0))
                data_file = data_file.select(output_columns)

            case columns if all(column in columns for column in PROPERTY_TS_COLUMNS_BASIC):
                data_file = data_file.rename({"period": "hour"})
                data_file = data_file.filter(pl.col("year") == self.study_year)

            # case columns if all(column in columns for column in PROPERTY_TS_COLUMNS_MULTIZONE):
            #     # Need to test these file types still
            #     # Drop all columns that are not datetime columns or the region name
            #     data_file = data_file.filter(pl.col("year") == self.study_year)
            #     data_file = data_file.drop(
            #         *[col for col in data_file.columns if col not in DATETIME_COLUMNS_MULTIZONE + [region]]
            #     )
            #     # Rename region name to hour
            #     data_file = data_file.rename({region: "value"})

            case columns if all(column in columns for column in PROPERTY_TS_COLUMNS_PIVOT):
                data_file = data_file.filter(pl.col("year") == self.study_year)
                data_file = data_file.melt(id_vars=PROPERTY_TS_COLUMNS_PIVOT, variable_name="hour")
                data_file = data_file.with_columns(pl.col("hour").cast(pl.Int8))

            case _:
                logger.warning("Data file columns not supported. Skipping it.")
                logger.warning("Datafile Columns: {}", data_file.columns)
                return

        if data_file.is_empty():
            logger.debug("Weather year doesn't exist in {}. Skipping it.", property_name)
            return

        # Format to SingleTimeSeries
        if data_file.columns == output_columns:
            data_file = data_file.sort(["year", "month", "day", "hour"])
            resolution = timedelta(hours=1)
            first_row = data_file.row(0)
            start = datetime(year=first_row[0], month=first_row[1], day=first_row[2])
            variable_name = property_name  # Change with property mapping
            data_file = self._reconcile_timeseries(data_file)
            return SingleTimeSeries.from_array(
                data=data_file["value"].cast(pl.Float64),
                resolution=resolution,
                initial_time=start,
                variable_name=variable_name,
                # Maybe change this to be the property name rather than the object name?
            )
        return

    def _time_slice_handler(self, property_name, property_data):
        """Deconstructs dict of timeslices into SingleTimeSeries objects."""
        resolution = timedelta(hours=1)
        initial_time = datetime(self.study_year, 1, 1)
        date_time_array = np.arange(
            f"{self.study_year}",
            f"{self.study_year + 1}",
            dtype="datetime64[h]",
        )  # Removing 1 day to match ReEDS convention and converting into a vector
        months = np.array([dt.astype("datetime64[M]").astype(int) % 12 + 1 for dt in date_time_array])
        month_datetime_series = np.zeros(len(date_time_array), dtype=float)

        # Helper function to parse the key patterns
        def parse_key(key):
            # Split by semicolons for multiple ranges
            ranges = key.split(";")
            month_list = []
            for rng in ranges:
                # Match ranges like 'M5-10' and single months like 'M1'
                match = re.match(r"M(\d+)(?:-(\d+))?", rng)
                if match:
                    start_month = int(match.group(1))
                    end_month = int(match.group(2)) if match.group(2) else start_month
                    # Generate the list of months from the range
                    month_list.extend(range(start_month, end_month + 1))
            return month_list

        # Fill the month_datetime_series with the property data values
        for key, value in property_data.items():
            months_in_key = parse_key(key)
            # Set the value in the array for the corresponding months
            for month in months_in_key:
                month_datetime_series[months == month] = value.magnitude

        return SingleTimeSeries.from_array(
            month_datetime_series,
            property_name,
            initial_time=initial_time,
            resolution=resolution,
        )

    def _apply_unit(self, value, unit):
        if isinstance(value, SingleTimeSeries):
            value.units = str(unit)
            return value
        return value * unit if unit else value

    def _apply_action(self, action, val_a, val_b):
        val_a_data = val_a.data if isinstance(val_a, SingleTimeSeries) else val_a
        val_b_data = val_b.data if isinstance(val_b, SingleTimeSeries) else val_b

        results = action(val_a_data, val_b_data)
        if isinstance(val_a, SingleTimeSeries):
            val_a.data = results
            return val_a
        if isinstance(val_b, SingleTimeSeries):
            val_b.data = results
            return val_b
        return results

    def _get_value(self, prop_value, unit, record, record_name):
        """Parse Property value from record csv, timeslice, and datafiles."""
        data_file = (
            self._csv_file_handler(record_name, record.get("data_file")) if record.get("data_file") else None
        )
        if data_file is None and record.get("data_file"):
            return None

        variable = (
            self._csv_file_handler(record.get("variable_tag"), record.get("variable"))
            if record.get("variable")
            else None
        )

        actions = {
            "×": np.multiply,  # noqa
            "+": np.add,
            "-": np.subtract,
            "/": np.divide,
            "=": lambda x, y: y,
        }
        action = actions[record.get("action")] if record.get("action") else None
        timeslice_value = record.get("timeslice_value")

        if variable is not None:
            if record.get("action") == "=":
                return self._apply_unit(variable, unit)
            if data_file is not None:
                return self._apply_unit(self._apply_action(action, variable, data_file), unit)
            if prop_value is not None:
                return self._apply_unit(self._apply_action(action, variable, prop_value), unit)
            return self._apply_unit(variable, unit)

        if data_file is not None:
            if timeslice_value is not None and timeslice_value != -1:
                return self._apply_unit(self._apply_action(action, data_file, timeslice_value), unit)
            return self._apply_unit(data_file, unit)

        if timeslice_value is not None and timeslice_value != -1:
            return self._apply_unit(timeslice_value, unit)

        return self._apply_unit(prop_value, unit)

    def _parse_property_data(self, record_data, record_name):
        mapped_properties = {}
        property_counts = {}
        multi_band_properties = set()

        for record in record_data:
            band = record["band"]
            timeslice = record["timeslice"]
            prop_name = record["property_name"]
            prop_value = record["property_value"]
            unit = record["property_unit"].replace("$", "usd")

            mapped_property_name = self.property_map.get(prop_name, prop_name)
            unit = None if unit == "-" else unit
            if unit:
                try:
                    unit = ureg[unit]
                except UndefinedUnitError:
                    unit = None

            value = self._get_value(prop_value, unit, record, record_name)
            if value is None:
                logger.warning("Property {} missing record data for {}. Skipping it.", prop_name, record_name)
                continue

            if timeslice is not None:
                # Timeslice Properties
                if mapped_property_name not in property_counts:
                    # First Time reading timeslice
                    nested_dict = {}
                    nested_dict[timeslice] = value
                    mapped_properties[mapped_property_name] = nested_dict
                    property_counts[mapped_property_name] = {timeslice}
                elif timeslice not in property_counts[mapped_property_name]:
                    mapped_properties[mapped_property_name][timeslice] = value
                    property_counts[mapped_property_name].add(timeslice)
                    multi_band_properties.add(mapped_property_name)
            else:
                # Standard Properties
                if mapped_property_name not in property_counts:
                    # First time reading basic property
                    mapped_properties[mapped_property_name] = value
                    property_counts[mapped_property_name] = {band}
                else:
                    # Multi-band properties
                    if band not in property_counts[mapped_property_name]:
                        new_prop_name = f"{mapped_property_name}_{band}"
                        mapped_properties[new_prop_name] = value
                        property_counts[mapped_property_name].add(band)
                        multi_band_properties.add(mapped_property_name)
                    else:
                        logger.warning(
                            "Property {} for {} has multiple values specified. Using the last one.",
                            mapped_property_name,
                            record_name,
                        )
                        mapped_properties[mapped_property_name] = value
        return mapped_properties, multi_band_properties


if __name__ == "__main__":
    from ..logger import setup_logging
    from .handler import get_parser_data

    run_folder = Path("")
    # Functions relative to the parser.
    setup_logging(level="DEBUG")

    config = Scenario.from_kwargs(
        name="Plexos-Test",
        input_model="plexos",
        run_folder=run_folder,
        solve_year=2030,
        weather_year=2030,
        model="",
    )
    config.fmap["xml_file"]["fname"] = ""

    parser = get_parser_data(config=config, parser_class=PlexosParser)
