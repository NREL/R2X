"""R2X API for data model."""

import csv
from collections.abc import Callable
from os import PathLike
from pathlib import Path
from itertools import chain
from collections.abc import Iterable
from loguru import logger

from infrasys.component import Component
from infrasys.system import System as ISSystem
from .__version__ import __data_model_version__
import uuid
import infrasys.cost_curves


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

    def _add_operation_cost_data(  # noqa: C901
        self,
        data: Iterable[dict],
        fields: list | None = None,
    ):
        operation_cost_fields = set()
        for sub_dict in data:
            if "operation_cost" not in sub_dict.keys():
                continue

            operation_cost = sub_dict["operation_cost"]
            for cost_field_key, cost_field_value in operation_cost.items():
                if isinstance(cost_field_value, dict):
                    assert (
                        "uuid" in cost_field_value.keys()
                    ), f"Operation cost field {cost_field_key} was assumed to be a component but is not."
                    variable_cost = self.get_component_by_uuid(uuid.UUID(cost_field_value["uuid"]))
                    sub_dict["variable_cost"] = variable_cost.vom_units.function_data.proportional_term
                    if "fuel_cost" in variable_cost.model_fields:
                        # Note: We multiply the fuel price by 1000 to offset the division
                        # done by Sienna when it parses .csv files
                        sub_dict["fuel_price"] = variable_cost.fuel_cost * 1000
                        operation_cost_fields.add("fuel_price")

                    function_data = variable_cost.value_curve.function_data
                    if "constant_term" in function_data.model_fields:
                        sub_dict["heat_rate_a0"] = function_data.constant_term
                        operation_cost_fields.add("heat_rate_a0")
                    if "proportional_term" in function_data.model_fields:
                        sub_dict["heat_rate_a1"] = function_data.proportional_term
                        operation_cost_fields.add("heat_rate_a1")
                    if "quadratic_term" in function_data.model_fields:
                        sub_dict["heat_rate_a2"] = function_data.quadratic_term
                        operation_cost_fields.add("heat_rate_a2")
                    if "x_coords" in function_data.model_fields:
                        x_y_coords = dict(zip(function_data.x_coords, function_data.y_coords))
                        match type(variable_cost):
                            case infrasys.cost_curves.CostCurve:
                                for i, (x_coord, y_coord) in enumerate(x_y_coords.items()):
                                    output_point_col = f"output_point_{i}"
                                    sub_dict[output_point_col] = x_coord
                                    operation_cost_fields.add(output_point_col)

                                    cost_point_col = f"cost_point_{i}"
                                    sub_dict[cost_point_col] = y_coord
                                    operation_cost_fields.add(cost_point_col)

                            case infrasys.cost_curves.FuelCurve:
                                for i, (x_coord, y_coord) in enumerate(x_y_coords.items()):
                                    output_point_col = f"output_point_{i}"
                                    sub_dict[output_point_col] = x_coord
                                    operation_cost_fields.add(output_point_col)

                                    heat_rate_col = "heat_rate_avg_0" if i == 0 else f"heat_rate_incr_{i}"
                                    sub_dict[heat_rate_col] = y_coord
                                    operation_cost_fields.add(heat_rate_col)
                elif cost_field_key not in sub_dict.keys():
                    sub_dict[cost_field_key] = cost_field_value
                    operation_cost_fields.add(cost_field_key)
                else:
                    pass

        fields.remove("operation_cost")  # type: ignore
        fields.extend(list(operation_cost_fields))  # type: ignore

        return data, fields

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

        if "operation_cost" in fields:
            data, fields = self._add_operation_cost_data(data, fields)

        with open(str(fpath), "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields, extrasaction="ignore", **dict_writer_kwargs)  # type: ignore
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
