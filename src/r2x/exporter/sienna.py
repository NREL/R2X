"""R2X Sienna system exporter."""

# System packages
import json
import os
from typing import Any

# Third-party packages
from loguru import logger

# Local imports
from r2x.exporter.handler import BaseExporter
from r2x.models import (
    ACBranch,
    Bus,
    DCBranch,
    Generator,
    HydroPumpedStorage,
    PowerLoad,
    Reserve,
    ReserveMap,
    Storage,
)


class SiennaExporter(BaseExporter):
    """Sienna exporter class.

    This is the class that export the infrasys system to csv's required to read
    in PowerSystems.jl. The class contains a method per file that needs to get
    created and some of the methods manipulate the data to be compliant wth
    PowerSystems.jl. The order is important for some steps, however, most
    of them could be run sequentially.

    The method `run` is the responsible of running all the exporter.

    Notes
    -----
    All the methods defined an `output_columns` which defines the fields from
    the `Component` that we want to export.

    All methods call `export_component_to_csv` or `export_dict_to_csv` that
    is defined on the `BaseExporter` class.


    See Also
    --------
    BaseExporter.export_component_to_csv
    BaseExporter.export_dict_to_csv
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_data = {}
        self.property_map = self.config.defaults.get("sienna_property_map", {})

    def run(self, *args, path=None, **kwargs) -> "SiennaExporter":
        """Run sienna exporter workflow.


        Notes
        -----
        All methods call `export_component_to_csv` or `export_dict_to_csv` that
        is defined on the `BaseExporter` class.

        If performance is a constraint, we could parallelize the methods.
        """
        logger.info("Starting {}", self.__class__.__name__)

        self.process_bus_data()
        self.process_load_data()
        self.process_branch_data()
        self.process_dc_branch_data()
        self.process_gen_data()
        self.process_reserves_data()
        self.process_storage_data()
        self.export_data()
        self.create_timeseries_pointers()
        return self

    def process_bus_data(self, fname: str = "bus.csv") -> None:
        """Create bus.csv file.

        Parameters
        ----------
        fname : str
            Name of the file to be created
        """
        output_fields = [
            "bus_id",
            "name",
            "area",
            "zone",
            "base_voltage",
            "bus_type",
        ]
        self.system.export_component_to_csv(
            Bus,
            fpath=self.output_folder / fname,
            fields=output_fields,
            key_mapping={"number": "bus_id", "load_zone": "zone"},
            restval="NA",
        )

    def process_load_data(self, fname: str = "load.csv") -> None:
        """Create load.csv file.

        Parameters
        ----------
        fname : str
            Name of the file to be created
        """
        output_fields = [
            "bus_id",
            "name",
            "available",
            "active_power",
            "reactive_power",
            "max_active_power",
            "max_reeactive_power",
            "active_power",
        ]
        self.system.export_component_to_csv(
            PowerLoad,
            fpath=self.output_folder / fname,
            fields=output_fields,
            unnest_key="number",
            key_mapping={"bus": "bus_id"},
            restval="0.0",
        )
        logger.info(f"File {fname} created.")

    def process_branch_data(self, fname: str = "branch.csv") -> None:
        """Create branch.csv file.

        Parameters
        ----------
        fname : str
            Name of the file to be created
        """
        output_fields = [
            "name",
            "connection_points_from",
            "connection_points_to",
            "r",
            "x",
            "b",
            "rate",
            "branch_type",
        ]

        # NOTE: We need to decide what we do if the user provides a rate or bi-directional rate
        # if "rate" in output_df.columns:
        #     output_df["rate"] = output_df["rate"].fillna(
        #         (output_df["rating_up"] + np.abs(output_df["rating_down"])) / 2
        #     )
        # else:
        #     output_df["rate"] = (output_df["rating_up"] + np.abs(output_df["rating_down"])) / 2

        self.system.export_component_to_csv(
            ACBranch,
            fpath=self.output_folder / fname,
            fields=output_fields,
            unnest_key="number",
            key_mapping={
                "from_bus": "connection_points_from",
                "to_bus": "connection_points_to",
                "class_type": "branch_type",
                "rating": "rate",
                "b": "primary_shunt",
            },
            # restval=0.0,
        )
        logger.info(f"File {fname} created.")

    def process_dc_branch_data(self, fname="dc_branch.csv") -> None:
        """Create dc_branch.csv file.

        Parameters
        ----------
        fname : str
            Name of the file to be created
        """
        output_fields = [
            "name",
            "connection_points_from",
            "connection_points_to",
            "rate",
            "loss",
        ]

        # NOTE: We need to decide what we do if the user provides a rate or bi-directional rate
        # if "rate" in output_df.columns:
        #     output_df["rate"] = output_df["rate"].fillna(
        #         (output_df["rating_up"] + np.abs(output_df["rating_down"])) / 2
        #     )
        # else:
        #     output_df["rate"] = (output_df["rating_up"] + np.abs(output_df["rating_down"])) / 2
        self.system.export_component_to_csv(
            DCBranch,
            fpath=self.output_folder / fname,
            fields=output_fields,
            unnest_key="number",
            key_mapping={
                "from_bus": "connection_points_from",
                "to_bus": "connection_points_to",
                "class_type": "branch_type",
                "rating_up": "rate",
            },
            restval="NA",
        )
        logger.info(f"File {fname} created.")
        return

    def process_gen_data(self, fname="gen.csv"):
        """Create gen.csv file.

        Parameters
        ----------
        fname : str
            Name of the file to be created
        """
        output_fields = [
            "name",
            "available",
            "prime_mover_type",
            "bus_id",
            "fuel",
            "base_mva",
            "rating",
            "unit_type",
            "active_power",
            "min_rated_capacity",
            "min_down_time",
            "min_up_time",
            "mean_time_to_repair",
            "forced_outage_rate",
            "planned_outage_rate",
            "ramp_up",
            "ramp_down",
            "category",
            "must_run",
            "pump_load",
            "vom_price",
            "operation_cost",
        ]

        key_mapping = {"bus": "bus_id"}
        self.system.export_component_to_csv(
            Generator,
            fpath=self.output_folder / fname,
            fields=output_fields,
            key_mapping=key_mapping,
            unnest_key="number",
            restval="NA",
        )
        logger.info(f"File {fname} created.")

    def process_reserves_data(self, fname="reserves.csv") -> None:
        """Create reserve.csv file.

        Parameters
        ----------
        fname : str
            Name of the file to be created
        """
        output_fields = [
            "name",
            "requirement",
            "timeframe",
            "eligible_region",
            "direction",
            "contributing_devices",
            "eligible_device_categories",
            "eligible_device_subcategories",
        ]

        # ReserveMap holds the eligible devices in the mapping.
        reserve_map_list: list[dict[str, Any]] = list(
            map(lambda component: component["mapping"], self.system.to_records(ReserveMap))
        )

        if not reserve_map_list:
            msg = "Reserve map class not found on the system. Skipping reserve contributing devices file."
            logger.warning(msg)
            return

        if len(reserve_map_list) > 1:
            logger.warning("We do not support multiple reserve maps per system")
            return
        reserve_map: dict = reserve_map_list[0]

        reserves: list[dict[str, Any]] = list(self.system.to_records(Reserve))

        output_data = []
        for reserve in reserves:
            # NOTE: Only export reserves that exists on the reserve map
            if reserve["name"] in reserve_map:
                output_dict = reserve
                output_dict["direction"] = reserve["direction"].name
                output_dict["eligible_device_categories"] = "(Generator,Storage)"
                contributing_devices = reserve_map.get(reserve["name"])
                output_dict["contributing_devices"] = str(tuple(contributing_devices)).replace(  # type: ignore
                    "'", ""
                )  # Sienna expects a tuple for this field that looks like "(val1,val2,val3)"
                output_data.append(output_dict)

        key_mapping = {"region": "eligible_region", "max_requirement": "requirement"} | self.property_map
        self.system._export_dict_to_csv(
            output_data,
            fpath=self.output_folder / fname,
            fields=output_fields,
            key_mapping=key_mapping,
            unnest_key="name",
            restval="NA",
        )
        logger.info(f"File {fname} created.")
        return

    def process_storage_data(self, fname="storage.csv") -> None:
        """Create storage.csv file.

        Parameters
        ----------
        fname : str
            Name of the file to be created
        """
        output_fields = [
            "name",
            "position",
            "available",
            "generator_name",
            "bus_id",
            "active_power",
            "rating",
            "input_efficiency",
            "output_efficiency",
            "storage_capacity",
            "min_storage_capacity",
            "max_storage_capacity",
            "input_active_power_limit_min",
            "output_active_power_limit_max",
            "output_active_power_limit_min",
            "unit_type",
        ]

        generic_storage = self.get_valid_records_properties(
            self.system.to_records(Storage),
            property_map=self.config.defaults["sienna_property_map"],
            unit_map=self.config.defaults["sienna_unit_map"],
            # valid_properties=output_fields,
        )
        hydro_pump = list(self.system.to_records(HydroPumpedStorage))
        storage_list = generic_storage + hydro_pump

        if storage_list is None:
            logger.warning("No storage devices found")
            return

        output_data = []
        for storage in storage_list:
            output_dict = storage
            output_dict["generator_name"] = storage["name"]
            output_dict["input_active_power_limit_max"] = output_dict["active_power"]
            output_dict["output_active_power_limit_max"] = output_dict["active_power"]
            # NOTE: If we need to change this in the future, we could probably
            # use the function max to check if the component has the field.
            output_dict["input_active_power_limit_min"] = 0  # output_dict["active_power"]
            output_dict["output_active_power_limit_min"] = 0  # output_dict["active_power"]
            output_dict["active_power"] = output_dict["active_power"]
            output_dict["bus_id"] = getattr(self.system.get_component_by_label(output_dict["bus"]), "number")
            output_dict["rating"] = output_dict["rating"]

            # NOTE: For pumped hydro storage we create a head and a tail
            # representation that keeps track of the upper and down reservoir
            # state of charge.
            # if storage["class_type"] == "HydroPumpedStorage":
            original_name = output_dict["name"]
            output_dict["name"] = original_name + "_head"
            output_dict["position"] = "head"
            output_data.append(output_dict)
            tail_copy = output_dict.copy()
            tail_copy["name"] = original_name + "_tail"
            output_dict["position"] = "tail"
            output_data.append(tail_copy)

        key_mapping = self.property_map
        self.system._export_dict_to_csv(
            output_data,
            fpath=self.output_folder / fname,
            fields=output_fields,
            key_mapping=key_mapping,
            unnest_key="number",
            restval="NA",
        )

        logger.info("File storage.csv created.")

    def create_timeseries_pointers(self) -> None:
        """Create timeseries_pointers.json file.

        Parameters
        ----------
        fname : str
            Name of the file to be created
        """
        ts_pointers_list = []

        for component_type, time_series in self.time_series_objects.items():
            csv_fpath = self.ts_directory / (
                f"{component_type}_{self.config.name}_{self.config.weather_year}.csv"
            )
            for i in range(len(time_series)):
                component_name = self.time_series_name_by_type[component_type][i]
                ts_instance = time_series[i]
                resolution = ts_instance.resolution.seconds
                variable_name = self.property_map.get(ts_instance.variable_name, ts_instance.variable_name)
                # TODO(pedro): check if the time series data is pre normalized
                # https://github.nrel.gov/PCM/R2X/issues/417
                ts_pointers = {
                    "category": component_type.split("_", maxsplit=1)[0],  # Component_name is the first
                    "component_name": component_name,
                    "data_file": str(csv_fpath),
                    "normalization_factor": 1.0,
                    "resolution": resolution,
                    "name": variable_name,
                    "scaling_factor_multiplier_module": None,
                    "scaling_factor_multiplier": None,
                }
                ts_pointers_list.append(ts_pointers)

        with open(os.path.join(self.output_folder, "timeseries_pointers.json"), mode="w") as f:
            json.dump(ts_pointers_list, f)

        logger.info("File timeseries_pointers.json created.")
        return

    def export_data(self) -> None:
        """Export csv data to specified folder from output_data attribute."""
        logger.debug("Saving Sienna data and timeseries files.")

        # First export all time series objects
        self.export_data_files()
        logger.info("Saving time series data.")
