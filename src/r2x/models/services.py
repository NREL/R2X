"""Models related to services."""

from typing import Annotated

from pydantic import Field, NonNegativeFloat, PositiveFloat

from r2x.enums import ReserveDirection, ReserveType
from r2x.models.core import Service
from r2x.models.named_tuples import MinMax
from r2x.models.topology import LoadZone
from r2x.units import Percentage


class Reserve(Service):
    """Class representing a reserve contribution."""

    time_frame: Annotated[
        PositiveFloat,
        Field(description="Timeframe in which the reserve is required in seconds"),
    ] = 1e30
    region: (
        Annotated[
            LoadZone,
            Field(description="LoadZone where reserve requirement is required."),
        ]
        | None
    ) = None
    vors: Annotated[
        float,
        Field(description="Value of reserve shortage in $/MW. Any positive value as as soft constraint."),
    ] = -1
    duration: (
        Annotated[
            PositiveFloat,
            Field(description="Time over which the required response must be maintained in seconds."),
        ]
        | None
    ) = None
    reserve_type: ReserveType
    load_risk: (
        Annotated[
            Percentage,
            Field(
                description="Proportion of Load that contributes to the requirement.",
            ),
        ]
        | None
    ) = None
    # ramp_rate: float | None = None  # NOTE: Maybe we do not need this.
    max_requirement: float = 0  # Should we specify which variable is the time series for?
    direction: ReserveDirection

    @classmethod
    def example(cls) -> "Reserve":
        return Reserve(
            name="ExampleReserve",
            region=LoadZone.example(),
            direction=ReserveDirection.UP,
            reserve_type=ReserveType.REGULATION,
        )


class VariableReserve(Reserve):
    time_frame: Annotated[
        NonNegativeFloat,
        Field(description="Timeframe in which the reserve is required in seconds"),
    ] = 0.0
    requirement: Annotated[
        NonNegativeFloat | None,
        Field(
            description="the value of required reserves in p.u (SYSTEM_BASE), validation range: (0, nothing)"
        ),
    ]
    sustained_time: Annotated[
        NonNegativeFloat,
        Field(description="the time in seconds reserve contribution must sustained at a specified level"),
    ] = 3600.0
    max_output_fraction: Annotated[
        NonNegativeFloat,
        Field(
            ge=0.0,
            le=1.0,
            description="the time in seconds reserve contribution must sustained at a specified level",
        ),
    ] = 1.0
    max_participation_factor: Annotated[
        NonNegativeFloat,
        Field(
            ge=0.0,
            le=1.0,
            description="the maximum portion [0, 1.0] of the reserve that can be contributed per device",
        ),
    ] = 1.0
    deployed_fraction: Annotated[
        NonNegativeFloat,
        Field(
            ge=0.0,
            le=1.0,
            description="Fraction of service procurement that is assumed to be actually deployed.",
        ),
    ] = 3600.0


class TransmissionInterface(Service):
    """Component representing a collection of branches that make up an interface or corridor.

    It can be specified between different :class:`Area` or :class:`LoadZone`.
    The interface can be used to constrain the power flow across it
    """

    active_power_flow_limits: Annotated[
        MinMax, Field(description="Minimum and maximum active power flow limits on the interface (MW)")
    ]
    direction_mapping: Annotated[
        dict[str, int],
        Field(
            description=(
                "Dictionary of the line names in the interface and their direction of flow (1 or -1) "
                "relative to the flow of the interface"
            )
        ),
    ]

    @classmethod
    def example(cls) -> "TransmissionInterface":
        return TransmissionInterface(
            name="ExampleTransmissionInterface",
            active_power_flow_limits=MinMax(min=-100, max=100),
            direction_mapping={"line-01": 1, "line-02": -2},
        )
