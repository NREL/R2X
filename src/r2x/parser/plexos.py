"""Functions related to parsers."""

from datetime import datetime, timedelta
import importlib
from importlib.resources import files
from pathlib import Path, PureWindowsPath
from argparse import ArgumentParser
import pandas as pd

from pint import UndefinedUnitError
import polars as pl
import numpy as np
from loguru import logger
from infrasys.exceptions import ISNotStored
from infrasys.time_series_models import SingleTimeSeries


from r2x.units import ureg
from r2x.api import System
from r2x.config import Scenario
from r2x.enums import ACBusTypes, ReserveDirection, ReserveType, PrimeMoversType
from r2x.exceptions import ModelError
from plexosdb import PlexosSQLite
from plexosdb.enums import ClassEnum, CollectionEnum
from r2x.model import (
    ACBus,
    Generator,
    GenericBattery,
    HydroPumpedStorage,
    MonitoredLine,
    PowerLoad,
    Reserve,
    LoadZone,
    ReserveMap,
    TransmissionInterface,
    TransmissionInterfaceMap,
)
from r2x.utils import validate_string

from .handler import PCMParser

models = importlib.import_module("r2x.model")

R2X_MODELS = importlib.import_module("r2x.model")
BASE_WEATHER_YEAR = 2007
XML_FILE_KEY = "xml_file"
PROPERTY_SV_COLUMNS_BASIC = ["name", "value"]
PROPERTY_SV_COLUMNS_NAMEYEAR = ["name", "year", "month", "day", "period", "value"]
PROPERTY_TS_COLUMNS_BASIC = ["year", "month", "day", "period", "value"]
PROPERTY_TS_COLUMNS_MULTIZONE = ["year", "month", "day", "period"]
PROPERTY_TS_COLUMNS_PIVOT = ["year", "month", "day"]
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
    "date_to": pl.String,
    "date_from": pl.String,
    "memo": pl.String,
    "scenario_category": pl.String,
    "scenario": pl.String,
    "action": pl.String,
    "data_file_tag": pl.String,
    "data_file": pl.String,
    "variable_tag": pl.String,
    # "variable": pl.String,
    "timeslice_tag": pl.String,
    "timeslice": pl.String,
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
DEFAULT_INDEX = [
    "object_id",
    "name",
    "category",
    # "band",
]
PROPERTIES_WITH_TEXT_TO_SKIP = ["Units Out", "Forced Outage Rate", "Commit", "Rating", "Maintenance Rate"]


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

        # if not self.study_year: #fall back on config??
        #     self.study_year = self.config.weather_year

        self.run_folder = Path(self.config.run_folder)
        self.system = System(name=self.config.name)
        self.property_map = self.config.defaults["plexos_property_map"]
        self.device_map = self.config.defaults["plexos_device_map"]
        self.prime_mover_map = self.config.defaults["tech_fuel_pm_map"]

        # Populate databse from XML file.
        xml_file = xml_file or self.run_folder / self.config.fmap["xml_file"]["fname"]
        self.db = PlexosSQLite(xml_fname=xml_file)

        # Extract scenario data
        model_name = getattr(self.config, "model", None)
        if model_name is None:
            model_name = self._select_model_name()
        self._process_scenarios(model_name=model_name)
        self.study_year: int = int(
            (self._collect_horizon_data(model_name=model_name).get("Date From") / 365.25) + 1900
        )

    def _collect_horizon_data(self, model_name: str) -> datetime:
        horizon_query = f"""
        SELECT
        atr.name as attribute_name,
        COALESCE(attr_data.value, atr.default_value) AS attr_val
        -- t_object.name as object_name
        FROM
        t_object
        left join t_class as class ON
            t_object.class_id == class.class_id
        left join t_attribute AS atr on
            t_object.class_id  == atr.class_id
        left join t_membership tm on
            t_object.object_id  == tm.child_object_id
        left join t_class AS parent_class on
            tm.parent_class_id == parent_class.class_id
        left join t_object to2 on
            tm.parent_object_id == to2.object_id
        LEFT JOIN t_attribute_data attr_data on
            attr_data.attribute_id == atr.attribute_id and t_object.object_id == attr_data.object_id
        WHERE
            class.name == '{ClassEnum.Horizon.value}'
            AND parent_class.name == '{ClassEnum.Model.value}'
            AND to2.name == '{model_name}'
        """
        horizon_data = self.db.query(horizon_query)
        horizon_map = {key: value for key, value in horizon_data}
        return horizon_map

    def build_system(self) -> System:
        """Create infrasys system."""
        logger.info("Building infrasys system using {}", self.__class__.__name__)
        # self.append_to_db = self.config.defaults.get("append_to_existing_database", False)

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
        # self._construct_renewable_profiles()

        # self._construct_areas()
        # self._construct_transformers()
        return self.system

    def _reconcile_timeseries(self, data_file):
        """Reconcile timeseries data."""

        def handle_leap_year_adjustment(data_file):
            # Adjust for non-leap year with leap-year data
            feb_28 = data_file.slice(1392, 24)
            before_feb_29 = data_file.slice(0, 1416)
            after_feb_29 = data_file.slice(1416, len(data_file) - 1440)
            return pl.concat([before_feb_29, feb_28, after_feb_29])

        def fill_missing_timestamps(data_file, date_time_column):
            # Add missing timestamps and fill nulls
            data_file = data_file.with_columns(
                (
                    pl.col("year").cast(pl.Int32).cast(pl.Utf8)
                    + "-"
                    + pl.col("month").cast(pl.Int32).cast(pl.Utf8).str.zfill(2)
                    + "-"
                    + pl.col("day").cast(pl.Int32).cast(pl.Utf8).str.zfill(2)
                    + " "
                    + pl.col("hour").cast(pl.Int32).cast(pl.Utf8).str.zfill(2)
                    + ":00:00"
                )
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
                .alias("timestamp")
            ).with_columns(pl.col("timestamp").dt.cast_time_unit("ns"))

            complete_timestamps_df = pl.from_pandas(pd.DataFrame({"timestamp": date_time_column}))
            missing_timestamps_df = complete_timestamps_df.join(data_file, on="timestamp", how="anti")

            missing_timestamps_df = missing_timestamps_df.with_columns(
                pl.col("timestamp").dt.year().alias("year"),
                pl.col("timestamp").dt.month().alias("month"),
                pl.col("timestamp").dt.day().alias("day"),
                pl.col("timestamp").dt.hour().alias("hour"),
                pl.lit(None).alias("value"),
            ).select(["year", "month", "day", "hour", "value", "timestamp"])

            complete_df = (
                pl.concat([data_file, missing_timestamps_df]).sort("timestamp").fill_null(strategy="forward")
            )
            complete_df.drop_in_place("timestamp")
            return complete_df

        def resample_data_to_hourly(data_file):
            # Resample data to hourly frequency
            data_file = data_file.with_columns((pl.col("hour") % 48).alias("hour"))
            data_file = (
                data_file.with_columns(
                    (
                        pl.datetime(
                            data_file["year"],
                            data_file["month"],
                            data_file["day"],
                            hour=data_file["hour"] // 2,
                            minute=(data_file["hour"] % 2) * 30,
                        )
                    ).alias("timestamp")
                )
                .sort("timestamp")
                .filter(pl.col("timestamp").is_not_null())
            )

            return (
                data_file.group_by_dynamic("timestamp", every="1h")
                .agg([pl.col("value").mean().alias("value")])
                .with_columns(
                    pl.col("timestamp").dt.year().alias("year"),
                    pl.col("timestamp").dt.month().alias("month"),
                    pl.col("timestamp").dt.day().alias("day"),
                    pl.col("timestamp").dt.hour().alias("hour"),
                    pl.col("value").alias("value"),
                )
                .select(["year", "month", "day", "hour", "value"])
            )

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
            valid_fields = {
                k: v for k, v in region.items() if k in default_model.model_fields if v is not None
            }
            ext_data = {
                k: v for k, v in region.items() if k not in default_model.model_fields if v is not None
            }
            if ext_data:
                valid_fields["ext"] = ext_data
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
            valid_fields = {
                k: v for k, v in mapped_bus.items() if k in default_model.model_fields if v is not None
            }
            ext_data = {
                k: v for k, v in mapped_bus.items() if k not in default_model.model_fields if v is not None
            }
            if ext_data:
                valid_fields["ext"] = ext_data

            # Get region from buses region memberships
            region_name = buses_region.filter(pl.col("parent_object_id") == bus["object_id"])["name"].item()

            valid_fields["load_zone"] = self.system.get_component(LoadZone, name=region_name)

            # NOTE: We do not parser differenet kind of buses from Plexos
            valid_fields["bus_type"] = ACBusTypes.PV

            valid_fields["base_voltage"] = (
                230.0 if not valid_fields.get("base_voltage") else valid_fields["base_voltage"]
            )
            self.system.add_component(default_model(id=idx + 1, **valid_fields))
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
            valid_fields = {
                k: v for k, v in mapped_reserve.items() if k in default_model.model_fields if v is not None
            }
            ext_data = {
                k: v
                for k, v in mapped_reserve.items()
                if k not in default_model.model_fields
                if v is not None
            }
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

                valid_fields["ext"] = ext_data

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
            valid_fields = {
                k: v
                for k, v in line_properties_mapped.items()
                if k in default_model.model_fields
                if v is not None
            }

            ext_data = {
                k: v
                for k, v in line_properties_mapped.items()
                if k not in default_model.model_fields
                if v is not None
            }
            if ext_data:
                valid_fields["ext"] = ext_data

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

            self.system.add_component(default_model(**valid_fields))
        return

    def _infer_model_type(self, generator_name):
        model_type_mapping = self.config.device_name_inference_map
        generator_name_lower = generator_name.lower()
        for key, model_type in model_type_mapping.items():
            if key in generator_name_lower:
                # logger.debug("Inferred model type for generator={} as {}", generator_name, model_type)
                return model_type
        return ""

    def _field_filter(self, property_fields, eligible_fields):
        valid = {k: v for k, v in property_fields.items() if k in eligible_fields if v is not None}
        extra = {k: v for k, v in property_fields.items() if k not in eligible_fields if v is not None}
        if extra:
            valid["ext"] = extra
        # NOTE: Plexos can define either a generator with Units = 0 to indicate
        # that it has been retired, 1 that is online or > 1 when it has
        # multiple units. For the R2X model to work, we need to check if
        # the unit is available, but fixed the available key to 1 to avoid
        # an error.
        if available := valid.get("available", None):
            if available > 0:
                valid["available"] = 1
        return valid, extra

    def _construct_generators(self):
        logger.debug("Creating generators")
        system_generators = (pl.col("child_class_name") == ClassEnum.Generator.name) & (
            pl.col("parent_class_name") == ClassEnum.System.name
        )

        system_generators = self._get_model_data(system_generators)
        system_generators.write_csv("generators.csv")
        # NOTE: The best way to identify the type of generator on Plexos is by reading the fuel
        fuel_query = f"""
        SELECT
            parent_obj.name as parent_object_name,
            child_obj.name as fuel_name
        FROM
            t_membership as mem
            left JOIN t_object as child_obj ON mem.child_object_id = child_obj.object_id
            left JOIN t_object as parent_obj ON mem.parent_object_id = parent_obj.object_id
            LEFT JOIN t_class AS child_cls ON child_obj.class_id = child_cls.class_id
            LEFT JOIN t_class AS parent_cls ON parent_obj.class_id = parent_cls.class_id
        WHERE
            child_cls.name = '{ClassEnum.Fuel.value}'
            AND parent_cls.name = '{ClassEnum.Generator.value}'
        """
        generator_fuel = self.db.query(fuel_query)
        generator_fuel_map = {key: value for key, value in generator_fuel}

        # Iterate over properties for generator
        for generator_name, generator_data in system_generators.group_by("name"):
            generator_name = generator_name[0]
            generator_fuel_type = generator_fuel_map.get(generator_name)
            logger.trace("Parsing generator = {} with fuel type = {}", generator_name, generator_fuel_type)
            model_map = (
                self.config.device_map.get(generator_name, "")
                or self.config.fuel_map.get(generator_fuel_type, "")
                or self._infer_model_type(generator_name)
            )

            if getattr(R2X_MODELS, model_map, None) is None:
                logger.warning(
                    "Model map not found for generator={} with fuel_type={}. Skipping it.",
                    generator_name,
                    generator_fuel_type,
                )
                continue
            model_map = getattr(R2X_MODELS, model_map)
            required_fields = {
                key: value for key, value in model_map.model_fields.items() if value.is_required()
            }
            # Handle properties from plexos assuming that same property can
            # appear multiple times on different bands.
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
                ]
            ].to_dicts()

            mapped_records, multi_band_records = self._parse_property_data(property_records, generator_name)
            mapped_records["name"] = generator_name

            # NOTE: Add logic to create Function data here
            if multi_band_records:
                pass

            # Add prime mover mapping
            mapped_records["prime_mover_type"] = (
                self.prime_mover_map[generator_fuel_type].get("type")
                if generator_fuel_type in self.prime_mover_map.keys()
                else self.prime_mover_map["default"].get("type")
            )
            mapped_records["prime_mover_type"] = PrimeMoversType[mapped_records["prime_mover_type"]]
            mapped_records["fuel"] = (
                self.prime_mover_map[generator_fuel_type].get("fuel")
                if generator_fuel_type in self.prime_mover_map.keys()
                else self.prime_mover_map["default"].get("fuel")
            )

            match model_map:
                case HydroPumpedStorage():
                    mapped_records["prime_mover_type"] = PrimeMoversType.PS

            # Pumped Storage generators are not required to have Max Capacity property
            if "base_power" not in mapped_records and "pump_load" in mapped_records:
                mapped_records["base_power"] = mapped_records["pump_load"]

            valid_fields, ext_data = self._field_filter(mapped_records, model_map.model_fields)

            ts_fields = {k: v for k, v in mapped_records.items() if isinstance(v, SingleTimeSeries)}
            valid_fields.update(
                {
                    ts_name: np.mean(ts.data)
                    for ts_name, ts in ts_fields.items()
                    if ts_name in valid_fields.keys()
                }
            )

            if not all(key in valid_fields for key in required_fields):
                logger.warning(
                    "Skipping Generator {} since it does not have all the required fields", generator_name
                )
                continue
            self.system.add_component(model_map(**valid_fields))
            if ts_fields:
                generator = self.system.get_component_by_label(f"{model_map.__name__}.{generator_name}")
                ts_dict = {"solve_year": self.study_year}
                for ts_name, ts in ts_fields.items():
                    ts.variable_name = generator_name + ts_name
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
                ]
            ].to_dicts()

            # logger.debug("Parsing battery = {}", battery_name)
            mapped_records, _ = self._parse_property_data(property_records, battery_name)
            mapped_records["name"] = battery_name

            if "Max Power" in mapped_records:
                mapped_records["base_power"] = mapped_records["Max Power"]

            if "Capacity" in mapped_records:
                mapped_records["storage_capacity"] = mapped_records["Capacity"]

            mapped_records["prime_mover_type"] = PrimeMoversType.BA

            valid_fields = {
                k: v for k, v in mapped_records.items() if k in GenericBattery.model_fields if v is not None
            }

            ext_data = {
                k: v
                for k, v in mapped_records.items()
                if k not in GenericBattery.model_fields
                if v is not None
            }
            # NOTE: Plexos can define either a generator with Units = 0 to indicate
            # that it has been retired, 1 that is online or > 1 when it has
            # multiple units. For the R2X model to work, we need to check if
            # the unit is available, but fixed the available key to 1 to avoid
            # an error.
            if available := valid_fields.get("available", None):
                if available > 0:
                    valid_fields["available"] = 1
            if ext_data:
                valid_fields["ext"] = ext_data

            if not all(key in valid_fields for key in required_fields):
                logger.warning(
                    "Skipping battery {} since it does not have all the required fields", battery_name
                )
                continue

            # # Look up date_file for any fields specified in the ext_data
            # if "date_file" in ext_data:
            #     date_file = ext_data.pop("date_file")
            #     date_file_path = self.run_folder / date_file
            #     if date_file_path.exists():
            #         valid_fields["ext"]["date_file"] = date_file_path
            #     else:
            #         logger.warning("Date file {} not found for battery {}", date_file, battery_name)
            #         valid_fields["ext"]["date_file"] = None

            if mapped_records["storage_capacity"] == 0:
                logger.warning("Skipping battery {} since it has zero capacity", battery_name)
                continue
            self.system.add_component(GenericBattery(**valid_fields))
        return

    def _add_buses_to_batteries(self):
        batteries = [battery["name"] for battery in self.system.to_records(GenericBattery)]
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
            valid_fields = {
                k: v for k, v in mapped_interface.items() if k in default_model.model_fields if v is not None
            }
            ext_data = {
                k: v
                for k, v in mapped_interface.items()
                if k not in default_model.model_fields
                if v is not None
            }
            if ext_data:
                valid_fields["ext"] = ext_data

            # Check that the interface has all the required fields of the model.
            if not all(
                k in valid_fields for k, field in default_model.model_fields.items() if field.is_required()
            ):
                logger.warning(
                    "{}:{} does not have all the required fields. Skipping it.",
                    default_model.__name__,
                    interface["name"],
                )
                continue

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

    def _select_model_name(self):
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

        logger.debug("Getting object_id for model={}", model_name)
        model_id = self.db.query("select object_id from t_object where name = ?", params=(model_name,))
        if len(model_id) > 1:
            msg = f"Multiple models with the same {model_name} returned. Check database or spelling."
            raise ModelError(msg)

        if not model_id:
            logger.warning("Model {} not found on the system.", model_name)
            return
        self.model_id = model_id[0][0]  # Unpacking tuple [(model_id,)]

        # NOTE: When doing performance updates this query could get some love.
        valid_scenarios = self.db.query(
            "select obj.name from t_membership mem "
            "left join t_object as obj on obj.object_id = mem.child_object_id "
            "left join t_class as cls on cls.class_id = obj.class_id "
            f"where mem.parent_object_id = {self.model_id} and cls.name = '{ClassEnum.Scenario}'"
        )
        assert valid_scenarios
        self.scenarios = [scenario[0] for scenario in valid_scenarios]  # Flatten list of tuples
        return None

    def _plexos_table_data(self) -> list[tuple]:
        # Get objects table/membership table
        sql_query = files("plexosdb.queries").joinpath("object_query.sql").read_text()
        object_data = self.db.query(sql_query)
        return object_data

    def _polarize_data(self, object_data: list[tuple]) -> pl.DataFrame:
        return pl.from_records(object_data, schema=DEFAULT_QUERY_COLUMNS_SCHEMA)

    def _get_model_data(self, data_filter) -> pl.DataFrame:
        """Filter plexos data for a given class and all scenarios in a model."""
        if not getattr(self, "scenarios", None):
            msg = (
                "Function `._get_model_data` does not work without any valid scenarios. "
                "Check that the model name exists on the xml file."
            )
            raise ModelError(msg)
        scenario_filter = pl.col("scenario").is_in(self.scenarios)
        scenario_specific_data = self.plexos_data.filter(data_filter & scenario_filter)

        base_case_filter = pl.col("scenario").is_null()
        if scenario_specific_data.is_empty():
            return self.plexos_data.filter(data_filter & base_case_filter)

        base_case_filter = base_case_filter & (
            ~(
                pl.col("name").is_in(scenario_specific_data["name"])
                & pl.col("property_name").is_in(scenario_specific_data["property_name"])
            )
            | pl.col("property_name").is_null()
        )
        base_case_data = self.plexos_data.filter(data_filter & base_case_filter)

        system_data = pl.concat([scenario_specific_data, base_case_data])

        # get system variables
        variable_filter = (
            (pl.col("child_class_name") == ClassEnum.Variable.name)
            & (pl.col("parent_class_name") == ClassEnum.System.name)
            & (pl.col("data_file").is_not_null())
        )
        variable_scenario_data = self.plexos_data.filter(variable_filter & scenario_filter)

        if variable_scenario_data.is_empty():
            variable_base_data = self.plexos_data.filter(variable_filter & pl.col("scenario").is_null())
        else:
            variable_base_data = self.plexos_data.filter(variable_filter & base_case_filter)
        variable_data = pl.concat([variable_scenario_data, variable_base_data])

        # Filter Variables
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
        return system_data

    def _construct_load_profiles(self):
        logger.debug("Creating load profile representation")
        system_regions = (pl.col("child_class_name") == ClassEnum.Region.name) & (
            pl.col("parent_class_name") == ClassEnum.System.name
        )
        regions = self._get_model_data(system_regions).filter(~pl.col("variable").is_null())
        # system_nodes = (pl.col("child_class_name") == ClassEnum.Node.name) & (
        #     pl.col("parent_class_name") == ClassEnum.System.name
        # )
        # nodes = self._get_model_data(system_nodes).filter(~pl.col("data_file").is_null())
        # nodes.write_csv("load_nodes.csv")

        # regions = self._get_model_data(system_regions).filter(~pl.col("data_file").is_null())
        # assert self.config.run_folder
        # regions.write_csv("load_regions.csv")
        for region, region_data in regions.group_by("name"):
            # If the weather year is not in the data, then drop that load.
            # Each band needs to be a different time series and load.
            # Expression is typically used when you want your outputs to track the different bands
            # Action will modify the input data from the TS file. Only when there is an Expression definition.
            if not len(region_data) == 1:
                msg = (
                    "load data has more than one row for {}. Selecting the first match. "
                    "Check filtering of properties"
                )
                logger.warning(msg, region)

            ts = self._csv_file_handler(
                property_name="max_active_power", property_data=region_data["variable"][0]
            )
            if not ts:
                continue

            max_load = np.max(ts.data.to_numpy())
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

    def _construct_renewable_profiles(self):
        logger.debug("Creating renewable profile representation")
        system_regions = (pl.col("child_class_name") == ClassEnum.Generator) & (
            pl.col("parent_class_name") == ClassEnum.System
        )

        # NOTE: The best way to identify the type of generator on Plexos is by reading the fuel
        fuel_query = f"""
        SELECT
            parent_obj.name as parent_object_name,
            child_obj.name as fuel_name
        FROM t_membership as mem
            LEFT JOIN t_object as child_obj ON mem.child_object_id = child_obj.object_id
            LEFT JOIN t_object as parent_obj ON mem.parent_object_id = parent_obj.object_id
        WHERE mem.child_class_name = {ClassEnum.Fuel.value}  and
            mem.parent_class_name = {ClassEnum.Generator.value}
        """
        generator_fuel = self.db.query(fuel_query)
        generator_fuel_map = {key: value for key, value in generator_fuel}
        generators = self._get_model_data(system_regions).filter(~pl.col("text").is_null())
        assert self.config.run_folder

        for generator_name, generator_data in generators.group_by("name"):
            fuel_type = generator_fuel_map.get(generator_name)
            model_map = self.config.defaults["model_map"].get(fuel_type, "")
            if not model_map:
                logger.warning("Could not find model map for {}. Skipping it.", generator_name)
                continue
            generator = self.system.get_component_by_label(f"{model_map}.{generator_name}")

            for property_name, property_data in generator_data.group_by("property_name"):
                if property_name in PROPERTIES_WITH_TEXT_TO_SKIP:
                    continue
                ts = self._text_handler(property_name, property_data)
                if ts is not None:
                    ts_dict = {"solve_year": self.study_year}
                    self.system.add_time_series(ts, generator, **ts_dict)

        return

    def _text_handler(self, property_name, property_data):
        if property_data["text"].str.ends_with(".csv").all():
            return self._csv_file_handler(property_name, property_data)
        elif property_data["text"].str.starts_with("M").all():
            return self._time_slice_handler(property_name, property_data)
        elif property_data["text"].str.starts_with("H").any():
            logger.warning("Hour slices not yet supported for {}", property_name)
            return
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
        logger.warning("Property {} not supported. Skipping it.", property_name)
        logger.warning("Data file {} not supported yet. Skipping it.", relative_path)
        logger.warning("Columns not supported: {}", data_file.columns)

    def _retrieve_single_value_data(self, property_name, data_file):
        if all(column in data_file.columns for column in [property_name.lower(), "year"]):
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

        # Convert these types to ["pattern", "value"]
        if all(column in data_file.columns for column in PROPERTY_TS_COLUMNS_MONTH_PIVOT):
            data_file = data_file.filter(pl.col("name") == property_name.lower())
            data_file = data_file.melt(id_vars=["name"], variable_name="pattern")
            data_file = data_file.select(["pattern", "value"])

        if data_file.columns == ["pattern", "value"]:
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
            )  # if other patterns exist will need to change.
            data_file = date_df.join(data_file.select("month", "value"), on="month", how="inner").select(
                output_columns
            )

        elif data_file.columns == ["month", "day", "period", "value"]:
            data_file = data_file.rename({"period": "hour"})
            data_file = data_file.with_columns(pl.lit(self.study_year).alias("year"))
            columns = ["year"] + [col for col in data_file.columns if col != "year"]
            data_file = data_file.select(columns)

        elif all(column in data_file.columns for column in [*PROPERTY_TS_COLUMNS_MDP, property_name.lower()]):
            data_file = data_file.with_columns(pl.lit(self.study_year).alias("year"))
            data_file = data_file.rename({property_name.lower(): "value"})
            data_file = data_file.rename({"period": "hour"})
            data_file = data_file.select(output_columns)

        elif all(column in data_file.columns for column in PROPERTY_TS_COLUMNS_BASIC):
            data_file = data_file.rename({"period": "hour"})
            data_file = data_file.filter(pl.col("year") == self.study_year)

        # elif all(column in data_file.columns for column in PROPERTY_TS_COLUMNS_MULTIZONE):
        #     # need to test these file types still
        #     # drop all columns that are not datetime columns or the region name
        #     data_file = data_file.filter(pl.col("year") == self.study_year)
        #     data_file = data_file.drop(
        #         *[col for col in data_file.columns if col not in DATETIME_COLUMNS_MULTIZONE + [region]]
        #     )
        #     # rename region name to hour
        #     data_file = data_file.rename({region: "value"})
        elif all(column in data_file.columns for column in PROPERTY_TS_COLUMNS_PIVOT):
            # need to test these file types still
            data_file = data_file.filter(pl.col("year") == self.study_year)
            data_file = data_file.melt(id_vars=PROPERTY_TS_COLUMNS_PIVOT, variable_name="hour")

        if data_file.is_empty():
            logger.warning("Weather year doesn't existing in {}. Skipping it.", property_name)
            return

        assert not data_file.is_empty()
        # Format to SingleTimeSeries
        if data_file.columns == output_columns:
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
        # Deconstruct pattern
        resolution = timedelta(hours=1)
        initial_time = datetime(self.study_year, 1, 1)
        date_time_array = np.arange(
            f"{self.study_year}",
            f"{self.study_year + 1}",
            dtype="datetime64[h]",
        )  # Removing 1 day to match ReEDS convention and converting into a vector
        months = np.array([dt.astype("datetime64[M]").astype(int) % 12 + 1 for dt in date_time_array])

        month_datetime_series = np.zeros(len(date_time_array), dtype=float)
        if not len(property_data) == 12:
            logger.warning("Partial time slices is not yet supported for {}", property_name)
            return

        property_records = property_data[["text", "property_value"]].to_dicts()
        variable_name = property_name  # Change with property mapping
        for record in property_records:
            month = int(record["text"].strip("M"))
            month_indices = np.where(months == month)
            month_datetime_series[month_indices] = record["property_value"]
        return SingleTimeSeries.from_array(
            month_datetime_series,
            variable_name,
            initial_time=initial_time,
            resolution=resolution,
        )

    def _apply_unit(self, value, unit):
        if type(value) is SingleTimeSeries:
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
        data_file = variable = action = None
        if record.get("data_file"):
            data_file = self._csv_file_handler(record_name, record.get("data_file"))
        if record.get("variable"):
            variable = self._csv_file_handler(record.get("variable_tag"), record.get("variable"))
            if variable is None:
                variable = self._csv_file_handler(record_name, record.get("variable"))

        if record.get("action"):
            actions = {
                "×": np.multiply,  # noqa
                "+": np.add,
                "-": np.subtract,
                "/": np.divide,
                "=": lambda x, y: y,
            }
            action = actions[record.get("action")]
        if variable is not None and record.get("action") == "=":
            return self._apply_unit(variable, unit)
        if variable is not None and data_file is not None:
            # logger.debug("Record Name: {}", record_name)
            # logger.debug("Variable: {}", variable)
            # logger.debug("Data File: {}", data_file)
            # confirm direction of action operator
            return self._apply_unit(self._apply_action(action, variable, data_file), unit)
        if variable is not None and prop_value is not None:
            return self._apply_unit(self._apply_action(action, variable, prop_value), unit)
        elif variable is not None:
            return self._apply_unit(variable, unit)
        elif data_file is not None:
            return self._apply_unit(data_file, unit)
        return self._apply_unit(prop_value, unit)

    def _parse_property_data(self, record_data, record_name):
        mapped_properties = {}
        property_counts = {}
        multi_band_properties = set()

        for record in record_data:
            band = record["band"]
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
            if prop_name not in property_counts:
                value = self._get_value(prop_value, unit, record, record_name)
                mapped_properties[mapped_property_name] = value
                property_counts[mapped_property_name] = {band}
            else:
                if band not in property_counts[mapped_property_name]:
                    new_prop_name = f"{mapped_property_name}_{band}"
                    value = self._get_value(prop_value, unit, record, record_name)
                    mapped_properties[new_prop_name] = value
                    property_counts[mapped_property_name].add(band)
                    multi_band_properties.add(mapped_property_name)
                    # If it's the same property and band, update the value
                else:
                    value = self._get_value(prop_value, unit, record, record_name)
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
