"""Model related to branches."""

from typing import Annotated

from infrasys.value_curves import InputOutputCurve
from pydantic import Field, NonNegativeFloat, NonPositiveFloat

from r2x.models.core import Device, FromTo_ToFrom, MinMax
from r2x.models.topology import ACBus, Arc, Area, DCBus
from r2x.units import ActivePower, Percentage


class Branch(Device):
    """Class representing a connection between components."""

    @classmethod
    def example(cls) -> "Branch":
        return Branch(name="ExampleBranch")


class ACBranch(Branch):
    """Class representing an AC connection between components."""

    arc: Annotated[Arc | None, Field(description="The branch's connections.")] = None
    from_bus: Annotated[ACBus, Field(description="Bus connected upstream from the arc.")]
    to_bus: Annotated[ACBus, Field(description="Bus connected downstream from the arc.")]
    r: Annotated[float | None, Field(description=("Resistance of the branch"))] = None
    x: Annotated[float | None, Field(description=("Reactance of the branch"))] = None
    rating: Annotated[ActivePower, Field(ge=0, description="Thermal rating of the line.")] | None = None


class MonitoredLine(ACBranch):
    """Class representing an AC transmission line."""

    b: Annotated[FromTo_ToFrom | None, Field(description="Shunt susceptance in pu")] = None
    g: Annotated[FromTo_ToFrom | None, Field(description="Shunt conductance in pu")] = None
    rating_up: Annotated[ActivePower, Field(ge=0, description="Forward rating of the line.")] | None = None
    rating_down: Annotated[ActivePower, Field(le=0, description="Reverse rating of the line.")] | None = None
    losses: Annotated[Percentage, Field(description="Power losses on the line.")] | None = None

    @classmethod
    def example(cls) -> "MonitoredLine":
        return MonitoredLine(
            name="ExampleMonitoredLine",
            from_bus=ACBus.example(),
            to_bus=ACBus.example(),
            losses=Percentage(10, "%"),
            rating_up=ActivePower(100, "MW"),
            rating_down=ActivePower(-100, "MW"),
            rating=ActivePower(100, "MW"),
        )


class Line(ACBranch):
    """Class representing an AC transmission line."""

    b: Annotated[FromTo_ToFrom | None, Field(description="Shunt susceptance in pu")] = None
    g: Annotated[FromTo_ToFrom | None, Field(description="Shunt conductance in pu")] = None
    active_power_flow: NonNegativeFloat
    reactive_power_flow: NonNegativeFloat
    angle_limits: MinMax

    @classmethod
    def example(cls) -> "Line":
        return Line(
            name="ExampleLine",
            from_bus=ACBus.example(),
            to_bus=ACBus.example(),
            rating=ActivePower(100, "MW"),
            active_power_flow=100,
            reactive_power_flow=100,
            angle_limits=MinMax(min=-0.03, max=0.03),
        )


class Transformer2W(ACBranch):
    """Class representing a 2-W transformer."""

    active_power_flow: NonNegativeFloat
    reactive_power_flow: NonNegativeFloat
    primary_shunt: float | None = None

    @classmethod
    def example(cls) -> "Transformer2W":
        return Transformer2W(
            name="Example2WTransformer",
            rating=ActivePower(100, "MW"),
            from_bus=ACBus.example(),
            to_bus=ACBus.example(),
            active_power_flow=100,
            reactive_power_flow=100,
        )


class TapTransformer(ACBranch):
    active_power_flow: NonNegativeFloat
    reactive_power_flow: NonNegativeFloat
    primary_shunt: float | None = None
    tap: Annotated[
        NonNegativeFloat,
        Field(
            ge=0,
            le=2.0,
            description=(
                "Normalized tap changer position for voltage control, varying between 0 and 2, with 1"
                "centered at the nominal voltage"
            ),
        ),
    ]


class PhaseShiftingTransformer(ACBranch):
    active_power_flow: NonNegativeFloat
    reactive_power_flow: NonNegativeFloat
    primary_shunt: float | None = None
    tap: Annotated[
        NonNegativeFloat,
        Field(
            ge=0,
            le=2.0,
            description=(
                "Normalized tap changer position for voltage control, varying between 0 and 2, with 1"
                "centered at the nominal voltage"
            ),
        ),
    ]
    α: Annotated[float, Field(ge=-1.571, le=1.571)]
    # primary_shunt: float | None = None
    phase_angle_limits: MinMax


class DCBranch(Branch):
    """Class representing a DC connection between components."""

    from_bus: Annotated[DCBus, Field(description="Bus connected upstream from the arc.")]
    to_bus: Annotated[DCBus, Field(description="Bus connected downstream from the arc.")]


class AreaInterchange(Branch):
    """Collection of branches that make up an interfece or corridor for the transfer of power."""

    active_power_flow: NonNegativeFloat
    flow_limits: FromTo_ToFrom
    from_area: Annotated[Area, Field(description="Area containing the bus.")] | None = None
    to_area: Annotated[Area, Field(description="Area containing the bus.")] | None = None

    @classmethod
    def example(cls) -> "AreaInterchange":
        return AreaInterchange(
            name="ExampleAreaInterchange",
            active_power_flow=10.0,
            flow_limits=FromTo_ToFrom(from_to=100, to_from=-100),
            from_area=Area.example(),
            to_area=Area.example(),
        )


class TModelHVDCLine(DCBranch):
    """Class representing a DC transmission line."""

    rating_up: Annotated[NonNegativeFloat, Field(description="Forward rating of the line.")] | None = None
    rating_down: Annotated[NonPositiveFloat, Field(description="Reverse rating of the line.")] | None = None
    losses: Annotated[NonNegativeFloat, Field(description="Power losses on the line.")] = 0
    resistance: Annotated[NonNegativeFloat, Field(description="Resistance of the line in p.u.")] | None = 0
    inductance: Annotated[NonNegativeFloat, Field(description="Inductance of the line in p.u.")] | None = 0
    capacitance: Annotated[NonNegativeFloat, Field(description="Capacitance of the line in p.u.")] | None = 0

    @classmethod
    def example(cls) -> "TModelHVDCLine":
        return TModelHVDCLine(
            name="ExampleDCLine",
            from_bus=DCBus.example(),
            to_bus=DCBus.example(),
            rating_up=100,
            rating_down=80,
        )


class TwoTerminalHVDCLine(ACBranch):
    """Class representing a DC transmission line."""

    active_power_flow: NonNegativeFloat
    active_power_limits_from: MinMax
    active_power_limits_to: MinMax
    reactive_power_limits_from: MinMax
    reactive_power_limits_to: MinMax
    loss: InputOutputCurve
