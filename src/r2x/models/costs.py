"""Cost related functions."""

# from infrasys import Component
from infrasys.models import InfraSysBaseModelWithIdentifers

from typing import Annotated
from pydantic import Field
from infrasys.cost_curves import ProductionVariableCostCurve
from r2x.units import Currency, FuelPrice


class OperationalCost(InfraSysBaseModelWithIdentifers):
    name: Annotated[str, Field(frozen=True)] = ""


class RenewableGenerationCost(OperationalCost):
    curtailment_cost: ProductionVariableCostCurve | None = None
    variable: ProductionVariableCostCurve | None = None


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
    variable: ProductionVariableCostCurve | None = None


class ThermalGenerationCost(OperationalCost):
    fixed: Annotated[FuelPrice, Field(description="Cost of using fuel in $/MWh.")] = FuelPrice(0.0, "usd/MWh")
    shut_down: Annotated[Currency | None, Field(description="Cost to turn the unit off")] = Currency(
        0.0, "usd"
    )
    start_up: Annotated[Currency | None, Field(description="Cost to start the unit.")] = Currency(0, "usd")
    variable: ProductionVariableCostCurve | None = None


class StorageCost(OperationalCost):
    charge_variable_cost: ProductionVariableCostCurve | None = None
    discharge_variable_cost: ProductionVariableCostCurve | None = None
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
