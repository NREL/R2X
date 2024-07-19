"""R2X API for data model."""

import csv
from collections.abc import Callable
from os import PathLike
from pathlib import Path
from itertools import chain
from collections.abc import Iterable
import pandas as pd
import polars as pl
from loguru import logger

from infrasys.component import Component
from infrasys.system import System as ISSystem
from .__version__ import __data_model_version__
from .model import (
    Branch,
    Bus,
    Generator,
    LoadZone,
    Area,
)
from .utils import unnest_all


class System(ISSystem):
    """API to interact with the SystemModel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_format_version = __data_model_version__

    def __str__(self) -> str:
        return f"System(name={self.name}, DataModel Version={self.version})"

    def __repr__(self) -> str:
        return str(self)

    @property
    def version(self):
        """The version property."""
        return __data_model_version__

    def get_generators(self, attributes: list | None = None) -> pl.DataFrame | pd.DataFrame:
        """Return the list of generators in the system as a DataFrame."""
        generator_list = []

        generator_components = [
            component_class
            for component_class in self.get_component_types()
            if Generator in component_class.__mro__
        ]
        for model in generator_components:
            generator_list.extend(
                list(
                    map(
                        lambda component: component.model_dump(),
                        self.get_components(model),
                    )
                )
            )

        generators = pl.from_pandas(
            pd.json_normalize(generator_list).drop(columns="services", errors="ignore")
        )
        if attributes:
            generators = generators.select(pl.col(attributes))

        # NOTE: This can work in the short term. In a near future we might want to patch it.
        # We only incldue one nested attribut which is the bus
        generators = unnest_all(generators)

        return generators

    def get_load_zones(self, attributes: list | None = None) -> pl.DataFrame:
        """Return all LoadZone objects in the system."""
        load_zones = pl.DataFrame(
            map(lambda component: component.model_dump(), self.get_components(LoadZone))
        )

        if attributes:
            load_zones = load_zones.select(pl.col(attributes))

        load_zones = unnest_all(load_zones)
        return load_zones

    def get_areas(self, attributes: list | None = None) -> pl.DataFrame:
        """Return all Area objects in the system."""
        areas = pl.DataFrame(map(lambda component: component.model_dump(), self.get_components(Area)))

        if attributes:
            areas = areas.select(pl.col(attributes))

        areas = unnest_all(areas)
        return areas

    def get_buses(self, attributes: list | None = None) -> pl.DataFrame:
        """Return all Bus objects in the system."""
        buses = pl.DataFrame(map(lambda component: component.model_dump(), self.get_components(Bus)))

        if attributes:
            buses = buses.select(pl.col(attributes))

        buses = unnest_all(buses)
        return buses

    def get_branches(self, attributes: list | None = None) -> pl.DataFrame:
        """Get Branch objects in the system."""
        branches = pl.DataFrame(map(lambda component: component.model_dump(), self.get_components(Branch)))

        if attributes:
            branches = branches.select(pl.col(attributes))

        branches = unnest_all(branches)
        return branches

    def export_component_to_csv(
        self,
        component: type[Component],
        fields: list | None = None,
        filter_func: Callable | None = None,
        fpath: PathLike | None = None,
        key_mapping: dict | None = None,
        unnest_key: str = "name",
        **dict_writer_kwargs,
    ):
        """Export components into a csv.

        component:
            Component type to get from the system
        """
        # Get desired components to offload to csv
        components = map(
            lambda component: component.model_dump(
                exclude={"ext"}, exclude_none=True, mode="json", context={"magnitude_only": True}
            ),
            self.get_components(component, filter_func=filter_func),
        )
        if fpath is None:
            fpath = Path(f"{component.__name__}.csv")

        self._export_dict_to_csv(
            components,
            fpath=fpath,
            fields=fields,
            key_mapping=key_mapping,
            unnest_key=unnest_key,
            **dict_writer_kwargs,
        )

    def _export_dict_to_csv(
        self,
        data: Iterable[dict],
        fpath: PathLike,
        fields: list | None = None,
        key_mapping: dict | None = None,
        unnest_key: str = "name",
        **dict_writer_kwargs,
    ):
        # Remaping keys
        # NOTE: It does not work recursively for nested components
        if key_mapping:
            data = [
                {key_mapping.get(key, key): value for key, value in sub_dict.items()} for sub_dict in data
            ]
            if fields:
                fields = list(map(lambda key: key_mapping.get(key, key), fields))

        if fields is None:
            fields = list(set(chain.from_iterable(data)))

        with open(str(fpath), "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields, extrasaction="ignore", **dict_writer_kwargs)
            writer.writeheader()
            for row in data:
                filter_row = {
                    key: value if not isinstance(value, dict) else value.get(unnest_key)
                    for key, value in row.items()
                }
                writer.writerow(filter_row)


if __name__ == "__main__":
    from .logger import setup_logging
    from rich.console import Console

    setup_logging(level="TRACE")
    logger.enable("infra_sys")

    console = Console()

    # from tests.models.systems import ieee_5bus
    #
    # system = ieee_5bus()
    # logger.info("From ARTEX")
