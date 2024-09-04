"""Models related to services."""

from typing import Annotated

from pydantic import Field, NonNegativeFloat, PositiveFloat

from r2x.enums import ReserveDirection, ReserveType
from r2x.models.core import MinMax, Service
from r2x.models.topology import LoadZone
from r2x.units import EmissionRate


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
            NonNegativeFloat,
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


class Emission(Service):
    """Class representing an emission object that is attached to generators."""

    rate: Annotated[EmissionRate, Field(description="Amount of emission produced in kg/MWh.")]
    emission_type: Annotated[str, Field(description="Type of emission. E.g., CO2, NOx.")]
    generator_name: Annotated[str, Field(description="Generator emitting.")]

    @classmethod
    def example(cls) -> "Emission":
        return Emission(
            name="ExampleEmission",
            generator_name="gen1",
            rate=EmissionRate(105, "kg/MWh"),
            emission_type="CO2",
        )


class TransmissionInterface(Service):
    """A collection of branches that make up an interface or corridor for the transfer of power
    such as between different :class:Area or :class:LoadZone.

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
            active_power_flow_limits=MinMax(-100, 100),
            direction_mapping={"line-01": 1, "line-02": -2},
        )
