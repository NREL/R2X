"""Exporter base class."""

from abc import ABC, abstractmethod
import string
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pint
import pandas as pd
import numpy as np
import infrasys
from loguru import logger
from infrasys.base_quantity import BaseQuantity

from r2x.api import System
from r2x.config import Scenario
from r2x.parser.handler import file_handler

OUTPUT_FNAME = "{self.weather_year}"


class BaseExporter(ABC):
    """Class that defines the shared methods of parsers.

    Note
    ----
    This class is meant to be use for developing new parsers. Do not use it directly.

    Attributes
    ----------
    config:
        Scenario configuration
    system:
        System class
    output_foder:
        output folder path

    Methods
    -------
    run()
        Sequentially run methods from the exporter
    get_data(key='load')
        Return the parsed data for load.
    read_file(fpath="load.csv")
        Read load data.
    export_data_files(time_series_folder="Data")
        Save time series data files.
    """

    def __init__(
        self, config: Scenario, system: System, output_folder: Path, ts_directory: str = "Data"
    ) -> None:
        self.config = config
        self.system = system
        self.output_folder = output_folder
        self.input_model = config.input_model
        self.ts_directory = Path(ts_directory)
        self.time_series_objects: dict[str, list[Any]] = defaultdict(list)
        self.time_series_name_by_type: dict[str, list[Any]] = defaultdict(list)
        self._handle_data_folder(config.output_folder, self.ts_directory)

    def _handle_data_folder(self, output_folder: str | Path, folder_name: str | Path) -> None:
        fpath = Path(output_folder) / folder_name
        if not fpath.exists():
            fpath.mkdir()
        return

    @abstractmethod
    def run(self, *args, **kwargs) -> "BaseExporter":
        """Run the exporter methods in sequence.

        This methods needs to be implemented by any exporter and it's content
        change based on the order the data is being exported.
        """

    def read_file(self, fpath: Path | str, filter_func: list[Callable] | None = None, **kwargs):
        """Read input model data from the file system.

        Currently supported formats:
            - .csv
            - .h5
        More to come!

        Parameters
        ----------
        fpath: Path, str
            Absolute location of the file in the system
        filter_func: List
            Filter functions to apply

        """
        data = file_handler(fpath, **kwargs)
        if data is None:
            return

        if isinstance(filter_func, list):
            for func in filter_func:
                data = func(data, **kwargs)
        return data

    def export_data_files(self, time_series_folder: str = "Data") -> None:
        """Export all time series objects attached to components.

        This method assumes that `self.config.weather_year and `self.output_folder` exist.

        Parameters
        ----------
        time_series_folder: str
            Folder name to save time series data
        """
        config_dict = self.config.__dict__
        for component in self.system.iter_all_components():
            if self.system.has_time_series(component):
                for ts_metadata in self.system.time_series.list_time_series_metadata(component):
                    ts_component_name = f"{component.__class__.__name__}_{ts_metadata.variable_name}"
                    self.time_series_objects[ts_component_name].append(
                        self.system.get_time_series(component, variable_name=ts_metadata.variable_name)
                    )
                    self.time_series_name_by_type[ts_component_name].append(component.name)

        assert self.config.weather_year is not None
        date_time_column = pd.date_range(
            start=f"1/1/{self.config.weather_year}",
            end=f"1/1/{self.config.weather_year + 1}",
            freq="1h",
            inclusive="left",
        )
        date_time_column = np.datetime_as_string(date_time_column, unit="m")
        # Remove leap day to match ReEDS convention
        # date_time_column = date_time_column[~((date_time_column.month == 2) & (date_time_column.day == 29))]
        if self.input_model == "reeds-US":
            date_time_column = date_time_column[:-24]

        csv_fpath = self.output_folder / time_series_folder

        # Use string substitution to dynamically change the output csv fnames
        csv_fname = config_dict.get("time_series_fname", "${component_type}_${name}_${weather_year}.csv")
        string_template = string.Template(csv_fname)

        for component_type, time_series in self.time_series_objects.items():
            time_series_arrays = list(map(lambda x: x.data.to_numpy(), time_series))

            config_dict["component_type"] = component_type
            csv_fname = string_template.safe_substitute(config_dict)
            csv_table = np.column_stack([date_time_column, *time_series_arrays])
            header = '"DateTime",' + ",".join(
                [f'"{name}"' for name in self.time_series_name_by_type[component_type]]
            )

            np.savetxt(
                csv_fpath / csv_fname,
                csv_table,
                header=header,
                delimiter=",",
                comments="",
                fmt="%s",
            )

        return

    def get_valid_records_properties(
        self,
        component_list,
        property_map: dict[str, str],
        unit_map: dict[str, str],
        valid_properties: list | None = None,
    ):
        """Return a validadted list of properties to the given property_map."""
        result = []
        component_list_mapped = [
            {property_map.get(key, key): value for key, value in d.items()} for d in component_list
        ]
        for component in component_list_mapped:
            component_dict = {"name": component["name"]}  # We need the name to match it with the membership.
            for property_name, property_value in component.items():
                if valid_properties is not None:
                    if property_name in valid_properties:
                        property_value = self.get_property_magnitude(
                            property_value, to_unit=unit_map.get(property_name)
                        )
                        component_dict[property_name] = property_value
                else:
                    property_value = self.get_property_magnitude(
                        property_value, to_unit=unit_map.get(property_name)
                    )
                    component_dict[property_name] = property_value
            result.append(component_dict)
        return result

    def get_property_magnitude(self, property_value, to_unit: str | None = None) -> float:
        """Return magnitude with the given units for a pint Quantity.

        Parameters
        ----------
        property_name

        property_value
            pint.Quantity to extract magnitude from
        to_unit
            String that contains the unit conversion desired. Unit must be compatible.
        """
        if not isinstance(property_value, pint.Quantity | BaseQuantity):
            return property_value
        if to_unit:
            unit = to_unit.replace("$", "usd")  # Dollars are named usd on pint
            property_value = property_value.to(unit)
        return property_value.magnitude


def get_exporter(
    config: Scenario,
    system: infrasys.system.System,
    exporter_class,
    filter_funcs: list[Callable] | None = None,
    **kwargs,
) -> BaseExporter:
    """Return exporter instance.


    Parameters
    ----------
    config
        Scenario configuration class
    system
        Infrasys system
    filter_func:
        Functions that will applied to read_data process

    Other Parameters
    ----------------
    kwargs
        year
            For filtering by solve year
        year_column
            To change the column to apply the filter
        column_mapping
            For renaming columns

    See Also
    --------
    BaseExporter
    pl_filter_year
    pl_lower_case
    pl_rename
    """
    logger.debug("Getting {} instance.", exporter_class.__name__)

    exporter: BaseExporter = exporter_class(
        config=config, system=system, output_folder=config.output_folder, **kwargs
    )

    # Functions relative to the parser.
    # NOTE: At some point we are going to migrate this out, but this sound like a good standard set
    # if filter_funcs is None:
    #     logger.trace("Using default filter functions")
    #     filter_funcs = [pl_lowercase, pl_rename, pl_filter_year]

    exporter.run(
        config=config,
        fmap=config.fmap,
        year=config.solve_year,
        filter_func=filter_funcs,
        **kwargs,
    )

    return exporter
