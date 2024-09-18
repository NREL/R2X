"""Cost related functions."""

from infrasys import Component
from typing import Annotated
from pydantic import Field
from infrasys.cost_curves import ProductionVariableCostCurve
from r2x.units import FuelPrice


class OperationalCost(Component):
    name: Annotated[str, Field(frozen=True)] = ""


class RenewableGenerationCost(OperationalCost):
    curtailment_cost: ProductionVariableCostCurve | None = None
    variable: ProductionVariableCostCurve | None = None


class HydroGenerationCost(OperationalCost):
    fixed: Annotated[float, Field(ge=0, description="Cost of using fuel in $.")] = 0.0
    variable: ProductionVariableCostCurve | None = None


class ThermalGenerationCost(OperationalCost):
    start_up: Annotated[float, Field(ge=0, description="Cost of using fuel in $.")] = 0.0
    fixed: Annotated[float, Field(ge=0, description="Cost of using fuel in $.")] = 0.0
    shut_down: Annotated[float, Field(ge=0, description="Cost of using fuel in $.")] = 0.0
    variable: ProductionVariableCostCurve | None = None


class StorageCost(OperationalCost):
    start_up: Annotated[float, Field(ge=0, description="Cost of using fuel in $.")] = 0.0
    fixed: Annotated[float, Field(ge=0, description="Cost of using fuel in $.")] = 0.0
    shut_down: Annotated[float, Field(ge=0, description="Cost of using fuel in $.")] = 0.0
    energy_surplus_cost: Annotated[FuelPrice, Field(description="Cost of using fuel in $/MWh.")] = FuelPrice(
        0.0, "usd/MWh"
    )
    energy_storage_cost: Annotated[FuelPrice, Field(description="Cost of using fuel in $/MWh.")] = FuelPrice(
        0.0, "usd/MWh"
    )
    charge_variable_cost: ProductionVariableCostCurve | None = None
    discharge_variable_cost: ProductionVariableCostCurve | None = None
