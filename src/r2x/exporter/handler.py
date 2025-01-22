"""Exporter base class."""

from abc import ABC, abstractmethod
import string
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np
import infrasys
from loguru import logger
from pint import Quantity

from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.exporter.utils import modify_components
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

    def export_data_files(self, year: int, time_series_folder: str = "Data") -> None:
        """Export all time series objects attached to components.

        This method assumes that `self.config.weather_year and `self.output_folder` exist.

        Parameters
        ----------
        time_series_folder: str
            Folder name to save time series data
        """
        assert year is not None
        config_dict = self.config.__dict__
        config_dict["year"] = year
        for component in self.system.iter_all_components():
            if self.system.has_time_series(component):
                for ts_metadata in self.system.time_series.list_time_series_metadata(component):
                    ts_component_name = f"{component.__class__.__name__}_{ts_metadata.variable_name}"
                    try:
                        self.time_series_objects[ts_component_name].append(
                            self.system.get_time_series(component, variable_name=ts_metadata.variable_name)
                        )
                    except:  # noqa: E722
                        continue
                    self.time_series_name_by_type[ts_component_name].append(component.name)

        component_lengths = {
            component_type: {ts.length}
            for component_type, time_series in self.time_series_objects.items()
            for ts in time_series
        }

        inconsistent_lengths = [
            (component_type, length_set)
            for component_type, length_set in component_lengths.items()
            if len(length_set) != 1
        ]
        if inconsistent_lengths:
            raise ValueError(f"Multiple lengths found for components time series: {inconsistent_lengths}")

        datetime_arrays = {
            component_type: (
                np.datetime_as_string(
                    pd.date_range(
                        start=f"1/1/{year}",
                        periods=ts.length,
                        freq=f"{int(ts.resolution.total_seconds() / 60)}min",  # Convert resolution to minutes
                    ),
                    unit="m",
                ),
                time_series,
            )
            for component_type, time_series in self.time_series_objects.items()
            for ts in time_series
        }
        csv_fpath = self.output_folder / time_series_folder

        # Use string substitution to dynamically change the output csv fnames
        csv_fname = config_dict.get("time_series_fname", "${component_type}_${name}_${year}.csv")
        logger.trace("Using {} as time_series name", csv_fname)
        string_template = string.Template(csv_fname)

        for component_type, (datetime_array, time_series) in datetime_arrays.items():
            time_series_arrays = list(
                map(lambda x: x.data.magnitude if isinstance(x.data, Quantity) else x.data, time_series)
            )
            config_dict["component_type"] = component_type
            csv_fname = string_template.safe_substitute(config_dict)
            csv_table = np.column_stack([datetime_array, *time_series_arrays])
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


def get_export_records(component_list: list[dict[str, Any]], *update_funcs: Callable) -> list[dict[str, Any]]:
    """Apply update functions to a list of components and return the modified list.

    Parameters
    ----------
    component_list : list[dict[str, Any]]
        A list of dictionaries representing components to be updated.
    *update_funcs : Callable
        Variable number of update functions to be applied to each component.

    Returns
    -------
    list[dict[str, Any]]
        A list of updated component dictionaries.

    Examples
    --------
    >>> def update_name(component):
    ...     component["name"] = component["name"].upper()
    ...     return component
    >>> def add_prefix(component):
    ...     component["id"] = f"PREFIX_{component['id']}"
    ...     return component
    >>> components = [{"id": "001", "name": "Component A"}, {"id": "002", "name": "Component B"}]
    >>> updated_components = get_export_records(components, update_name, add_prefix)
    >>> updated_components
    [{'id': 'PREFIX_001', 'name': 'COMPONENT A'}, {'id': 'PREFIX_002', 'name': 'COMPONENT B'}]
    """
    update_functions = modify_components(*update_funcs)
    return [update_functions(component) for component in component_list]


def get_export_properties(component, *update_funcs: Callable) -> dict[str, Any]:
    """Apply update functions to a single component and return the modified component.

    Parameters
    ----------
    component : dict[str, Any]
        A dictionary representing a component to be updated.
    *update_funcs : Callable
        Variable number of update functions to be applied to the component.

    Returns
    -------
    dict[str, Any]
        The updated component dictionary.

    Examples
    --------
    >>> def update_status(component):
    ...     component["status"] = "active"
    ...     return component
    >>> def add_timestamp(component):
    ...     from datetime import datetime
    ...
    ...     component["last_updated"] = datetime.now().isoformat()
    ...     return component
    >>> component = {"id": "003", "name": "Component C"}
    >>> updated_component = get_export_properties(component, update_status, add_timestamp)
    >>> updated_component
    {'id': '003', 'name': 'Component C', 'status': 'active', 'last_updated': '2024-09-27T10:30'}
    """
    update_functions = modify_components(*update_funcs)
    return update_functions(component)


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
        filter_func=filter_funcs,
        **{**config.input_config.__dict__, **kwargs},
    )

    return exporter
