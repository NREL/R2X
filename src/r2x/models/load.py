"""Electric load related models."""

from typing import Annotated

from pydantic import Field, NonNegativeFloat

from r2x.models.core import StaticInjection
from r2x.models.named_tuples import Complex
from r2x.models.topology import ACBus, Bus
from r2x.units import ActivePower, ApparentPower


class ElectricLoad(StaticInjection):
    """Supertype for all electric loads."""

    bus: Bus = Field(description="Point of injection.")


class StaticLoad(ElectricLoad):
    """Supertype for static loads."""


class ControllableLoad(ElectricLoad):
    """Abstract class for controllable loads."""


class PowerLoad(StaticLoad):
    """Class representing a Load object."""

    active_power: (
        Annotated[
            ActivePower,
            Field(gt=0, description="Initial steady-state active power demand."),
        ]
        | None
    ) = None
    reactive_power: (
        Annotated[float, Field(gt=0, description="Reactive Power of Load at the bus in MW.")] | None
    ) = None
    max_active_power: Annotated[ActivePower, Field(gt=0, description="Max Load at the bus in MW.")] | None = (
        None
    )
    max_reactive_power: (
        Annotated[ActivePower, Field(gt=0, description=" Initial steady-state reactive power demand.")] | None
    ) = None
    base_power: Annotated[
        ApparentPower | None,
        Field(
            gt=0,
            description="Base power of the unit (MVA) for per unitization.",
        ),
    ] = None
    operation_cost: float | None = None

    @classmethod
    def example(cls) -> "PowerLoad":
        return PowerLoad(name="ExampleLoad", bus=ACBus.example(), active_power=ActivePower(1000, "MW"))


class StandardLoad(StaticLoad):
    base_power: NonNegativeFloat
    constant_active_power: float
    constant_reactive_power: float
    impedance_active_power: float
    impedance_reactive_power: float
    current_active_power: float
    current_reactive_power: float
    max_constant_active_power: float
    max_constant_reactive_power: float
    max_impedance_active_power: float
    max_impedance_reactive_power: float
    max_current_active_power: float
    max_current_reactive_power: float


class InterruptiblePowerLoad(ControllableLoad):
    """A static interruptible power load."""

    base_power: Annotated[
        ApparentPower | None,
        Field(
            gt=0,
            description="Base power of the unit (MVA) for per unitization.",
        ),
    ] = None
    active_power: (
        Annotated[
            ActivePower,
            Field(gt=0, description="Initial steady-state active power demand."),
        ]
        | None
    ) = None
    reactive_power: (
        Annotated[float, Field(gt=0, description="Reactive Power of Load at the bus in MW.")] | None
    ) = None
    max_active_power: Annotated[ActivePower, Field(ge=0, description="Max Load at the bus in MW.")] | None = (
        None
    )
    max_reactive_power: (
        Annotated[ActivePower, Field(gt=0, description=" Initial steady-state reactive power demand.")] | None
    ) = None
    operation_cost: float | None = None


class FixedAdmittance(ElectricLoad):
    """A fixed admittance."""

    Y: Complex
