"""Cost related functions."""

from operator import attrgetter
from typing import Annotated

from infrasys.cost_curves import CostCurve, FuelCurve, UnitSystem
from infrasys.models import InfraSysBaseModel
from infrasys.value_curves import LinearCurve
from pydantic import Field, computed_field

from r2x.units import Currency


class OperationalCost(InfraSysBaseModel):
    @computed_field  # type: ignore[prop-decorator]
    @property
    def class_type(self) -> str:
        """Create attribute that holds the class name."""
        return type(self).__name__

    @computed_field  # type: ignore[prop-decorator]
    @property
    def variable_type(self) -> str | None:
        """Create attribute that holds the class name."""
        if not getattr(self, "variable", None):
            return None
        return type(getattr(self, "variable")).__name__

    @computed_field  # type: ignore[prop-decorator]
    @property
    def value_curve_type(self) -> str | None:
        """Create attribute that holds the class name."""
        try:
            return type(attrgetter("variable.value_curve")(self)).__name__
        except AttributeError:
            return None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def function_data_type(self) -> str | None:
        """Create attribute that holds the class name."""
        try:
            return type(attrgetter("variable.value_curve.function_data")(self)).__name__
        except AttributeError:
            return None


class RenewableGenerationCost(OperationalCost):
    curtailment_cost: CostCurve | None = None
    variable: CostCurve | None = None


class HydroGenerationCost(OperationalCost):
    fixed: Annotated[
        Currency | None,
        Field(
            description=(
                "Fixed cost of keeping the unit online. "
                "For some cost represenations this field can be duplicative"
            )
        ),
    ] = Currency(0, "usd")
    variable: CostCurve | None = None


class ThermalGenerationCost(OperationalCost):
    """An operational cost for thermal generators.

    It includes fixed cost, variable cost, shut-down cost, and multiple options for start up costs.

    References
    ----------
    .. [1] National Renewable Energy Laboratory. "Thermal Generation Cost Model Library."
       Available: https://nrel-sienna.github.io/PowerSystems.jl/stable/model_library/thermal_generation_cost/
    """

    fixed: Annotated[Currency, Field(description="Cost of using fuel in $ or $/hr.")] = Currency(0, "usd")
    shut_down: Annotated[Currency | None, Field(description="Cost to turn the unit off")] = Currency(
        0.0, "usd"
    )
    start_up: Annotated[Currency | None, Field(description="Cost to start the unit.")] = Currency(0, "usd")
    variable: Annotated[CostCurve | FuelCurve | None, Field(description="Variable production cost")] = None

    @classmethod
    def example(cls) -> "ThermalGenerationCost":
        return ThermalGenerationCost(
            fixed=Currency(0, "usd"),
            shut_down=Currency(100, "usd"),
            start_up=Currency(100, "usd"),
            variable=FuelCurve(value_curve=LinearCurve(10), power_units=UnitSystem.NATURAL_UNITS),
        )


class StorageCost(OperationalCost):
    charge_variable_cost: CostCurve | None = None
    discharge_variable_cost: CostCurve | None = None
    energy_shortage_cost: Annotated[
        Currency, Field(description="Cost incurred by the model for being short of the energy target")
    ] = Currency(0.0, "usd")
    energy_surplus_cost: Annotated[Currency, Field(description="Cost of using fuel in $/MWh.")] = Currency(
        0.0, "usd"
    )
    fixed: Annotated[Currency, Field(description=" Fixed cost of operating the storage system")] = Currency(
        0.0, "usd"
    )
    shut_down: Annotated[Currency | None, Field(description="Cost to turn the unit off")] = Currency(
        0.0, "usd"
    )
    start_up: Annotated[Currency | None, Field(description="Cost to start the unit.")] = Currency(0, "usd")
