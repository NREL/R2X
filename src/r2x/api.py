"""R2X API for data model."""

import csv
from collections.abc import Callable
from os import PathLike
from pathlib import Path
from collections.abc import Iterable
from loguru import logger

import inspect
from infrasys.component import Component
from infrasys.system import System as ISSystem

from .__version__ import __data_model_version__


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

    def to_json(self, filename: Path | str, overwrite=False, indent=None, data=None) -> None:  # noqa: D102
        return super().to_json(filename, overwrite=overwrite, indent=indent, data=data)

    @classmethod
    def from_json(cls, filename: Path | str, upgrade_handler: Callable | None = None, **kwargs) -> "System":  # noqa: D102
        return super().from_json(filename=filename, upgrade_handler=upgrade_handler, **kwargs)  # type: ignore

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
                exclude={},
                exclude_none=True,
                mode="json",
                context={"magnitude_only": True},
                # serialize_as_any=True,
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
        # key_mapping: dict | None = None,
        # unnest_key: str = "name",
        **dict_writer_kwargs,
    ):
        dict_writer_kwargs = {
            key: value
            for key, value in dict_writer_kwargs.items()
            if key in inspect.getfullargspec(csv.DictWriter).args
        }

        with open(str(fpath), "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields, extrasaction="ignore", **dict_writer_kwargs)  # type: ignore
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        return


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
