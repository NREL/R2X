"""R2X data model.

This script contains the Sienna data model for power systems relevant to CEM to PCM translations.
It uses `infrasys.py` to store the components and pydantic for validating the fields.

Mapping for models:
    Zonal model:
        - Buses = ReEDS BA
        - LoadZone = ReEDS transmission region or Plexos Zones
        - Area = States
    Nodal model:
        - Buses = Nodes
        - LoadZone = ReEDS BA or Sienna Load Zones or Plexos Regions
        - Area = Plexos Zones or Sienna Areas
"""
# ruff: noqa: D102

from collections import defaultdict
from typing import Annotated, DefaultDict  # noqa: UP035

from infrasys.component import Component
from pydantic import (
    Field,
    NonNegativeFloat,
    NonPositiveFloat,
    PositiveFloat,
    PositiveInt,
    confloat,
)

from r2x.units import (
    ActivePower,
    EmissionRate,
    Energy,
    FuelPrice,
    HeatRate,
    Percentage,
    PowerRate,
    Time,
    ureg,
    Voltage,
)

from .enums import ACBusTypes, PrimeMoversType, ReserveDirection, ReserveType

unit_size = confloat(ge=0, le=1)

NonNegativeFloatType = NonNegativeFloat
NonPositiveFloatType = NonPositiveFloat
PositiveFloatType = PositiveFloat


class BaseComponent(Component):
    """Infrasys base component with additional fields for R2X."""

    ext: dict = Field(default_factory=dict, description="Additional information of the component.")
    available: Annotated[bool, Field(description="If the component is available.")] = True
    category: Annotated[str, Field(description="Category that this component belongs to.")] | None = None

    @property
    def class_type(self) -> str:
        """Create attribute that holds the class name."""
        return type(self).__name__


class Service(BaseComponent):
    """Base class for Service attached to components."""


class Device(BaseComponent):
    """Abstract class for devices."""

    services: (
        Annotated[
            list[Service],
            Field(description="Services that this component contributes to.", default_factory=list),
        ]
        | None
    ) = None


class Topology(BaseComponent):
    """Abstract type to represent the structure and interconnectedness of the system."""

    services: (
        Annotated[
            list[Service],
            Field(description="Services that this component contributes to.", default_factory=list),
        ]
        | None
    ) = None


class AggregationTopology(Topology):
    """Base class for area-type components."""


class Area(AggregationTopology):
    """Collection of buses in a given region."""

    @classmethod
    def example(cls) -> "Area":
        return Area(name="New York")


class LoadZone(AggregationTopology):
    """Collection of buses for electricity price analysis."""

    @classmethod
    def example(cls) -> "LoadZone":
        return LoadZone(name="ExampleLoadZone")


class Bus(Topology):
    """Power-system Bus abstract class."""

    id: Annotated[PositiveInt, Field(description="ID/number associated to the bus.")]
    load_zone: Annotated[LoadZone, Field(description="the load zone containing the DC bus.")] | None = None
    area: Annotated[Area, Field(description="Area containing the bus.")] | None = None
    lpf: Annotated[float, Field(description="Load participation factor of the bus.", ge=0, le=1)] | None = (
        None
    )
    base_voltage: Annotated[Voltage, Field(gt=0, description="Base voltage in kV.")] | None = None
    magnitude: (
        Annotated[PositiveFloatType, Field(description="Voltage as a multiple of base_voltage.")] | None
    ) = None
    bus_type: Annotated[ACBusTypes, Field(description="Type of category of bus")] | None = None

    @classmethod
    def example(cls) -> "Bus":
        return Bus(
            id=1,
            name="ExampleBus",
            load_zone=LoadZone.example(),
            area=Area.example(),
            lpf=1,
        )


class DCBus(Bus):
    """Power-system DC Bus."""

    @classmethod
    def example(cls) -> "DCBus":
        return DCBus(
            name="ExampleDCBus",
            id=1,
            load_zone=LoadZone.example(),
            area=Area.example(),
            base_voltage=100 * ureg.kV,
            lpf=1,
        )


class ACBus(Bus):
    """Power-system AC bus."""

    @classmethod
    def example(cls) -> "ACBus":
        return ACBus(
            name="ExampleACBus",
            id=1,
            load_zone=LoadZone.example(),
            area=Area.example(),
            base_voltage=100 * ureg.kV,
        )


class PowerLoad(BaseComponent):
    """Class representing a Load object."""

    bus: Bus | LoadZone = Field(description="Point of injection.")
    max_active_power: Annotated[ActivePower, Field(gt=0, description="Max Load at the bus in MW")] | None = (
        None
    )

    @classmethod
    def example(cls) -> "PowerLoad":
        return PowerLoad(name="ExampleLoad", bus=Bus.example())


class FixedLoad(PowerLoad):
    """A static PowerLoad that is not interruptible."""

    @classmethod
    def example(cls) -> "PowerLoad":
        return PowerLoad(name="ExampleLoad", bus=Bus.example(), max_active_power=100 * ureg.MW)


class InterruptiblePowerLoad(PowerLoad):
    """A static interruptible power load."""

    base_power: Annotated[ActivePower, Field(gt=0, description="Active power of the load type.")]
    operation_cost: float | None = None


class TransmissionInterface(Service):
    """Collection of branches that make up an interfece or corridor for the transfer of power."""

    max_power_flow: Annotated[ActivePower, Field(ge=0, description="Maximum allowed flow.")]
    min_power_flow: Annotated[ActivePower, Field(le=0, description="Minimum allowed flow.")]
    ramp_up: (
        Annotated[PowerRate, Field(ge=0, description="Maximum ramp allowed on the positve direction.")] | None
    ) = None
    ramp_down: (
        Annotated[PowerRate, Field(ge=0, description="Minimum ramp allowed on the negative direction.")]
        | None
    ) = None

    @classmethod
    def example(cls) -> "TransmissionInterface":
        return TransmissionInterface(
            name="ExampleTransmissionInterface",
            max_power_flow=ActivePower(100, "MW"),
            min_power_flow=ActivePower(-100, "MW"),
        )


class TransmissionInterfaceMap(BaseComponent):  # noqa: D101
    mapping: DefaultDict[str, list] = defaultdict(list)  # noqa: UP006, RUF012


class Reserve(Service):
    """Class representing a reserve contribution."""

    time_frame: Annotated[
        PositiveFloatType,
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
            PositiveFloatType,
            Field(description="Time over which the required response must be maintained in seconds."),
        ]
        | None
    ) = None
    reserve_type: ReserveType
    load_risk: (
        Annotated[
            NonNegativeFloatType,
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
            direction=ReserveDirection.Up,
            reserve_type=ReserveType.Regulation,
        )


class ReserveMap(BaseComponent):  # noqa: D101
    mapping: DefaultDict[str, list] = defaultdict(list)  # noqa: UP006, RUF012


class Branch(Device):
    """Class representing a connection between components."""

    # arc: Annotated[Arc, Field(description="The branch's connections.")]
    from_bus: Annotated[Bus, Field(description="Bus connected upstream from the arc.")]
    to_bus: Annotated[Bus, Field(description="Bus connected downstream from the arc.")]


class ACBranch(Branch):
    """Class representing an AC connection between components."""

    r: Annotated[float, Field(description=("Resistance of the branch"))] = 0
    x: Annotated[float, Field(description=("Reactance of the branch"))] = 0
    b: Annotated[float, Field(description=("Shunt susceptance of the branch"))] = 0


class MonitoredLine(ACBranch):
    """Class representing an AC transmission line."""

    rating_up: Annotated[ActivePower, Field(ge=0, description="Forward rating of the line.")] | None = None
    rating_down: Annotated[ActivePower, Field(le=0, description="Reverse rating of the line.")] | None = None
    losses: Annotated[Percentage, Field(description="Power losses on the line.")] | None = None

    @classmethod
    def example(cls) -> "MonitoredLine":
        return MonitoredLine(
            name="ExampleLine",
            from_bus=Bus.example(),
            to_bus=Bus.example(),
            losses=Percentage(10, "%"),
            rating_up=ActivePower(100, "MW"),
            rating_down=ActivePower(-100, "MW"),
        )


class Transformer2W(ACBranch):
    """Class representing a 2-W transformer."""

    rate: Annotated[NonNegativeFloatType, Field(description="Rating of the transformer.")]

    @classmethod
    def example(cls) -> "Transformer2W":
        return Transformer2W(
            name="Example2WTransformer",
            rate=100,
            from_bus=Bus.example(),
            to_bus=Bus.example(),
        )


class DCBranch(Branch):
    """Class representing a DC connection between components."""


class TModelHVDCLine(DCBranch):
    """Class representing a DC transmission line."""

    rating_up: Annotated[NonNegativeFloatType, Field(description="Forward rating of the line.")] | None = None
    rating_down: Annotated[NonPositiveFloatType, Field(description="Reverse rating of the line.")] | None = (
        None
    )
    losses: Annotated[NonNegativeFloatType, Field(description="Power losses on the line.")] = 0
    resistance: (
        Annotated[NonNegativeFloatType, Field(description="Resistance of the line in p.u.")] | None
    ) = 0
    inductance: (
        Annotated[NonNegativeFloatType, Field(description="Inductance of the line in p.u.")] | None
    ) = 0
    capacitance: (
        Annotated[NonNegativeFloatType, Field(description="Capacitance of the line in p.u.")] | None
    ) = 0

    @classmethod
    def example(cls) -> "TModelHVDCLine":
        return TModelHVDCLine(
            name="ExampleDCLine",
            from_bus=Bus.example(),
            to_bus=Bus.example(),
            rating_up=100,
            rating_down=80,
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


class Generator(Device):
    """Abstract generator class."""

    bus: Annotated[ACBus, Field(description="Bus where the generator is connected.")] | None = None
    base_power: Annotated[ActivePower, Field(description="Active power generation in MW.")]
    must_run: Annotated[int | None, Field(description="If we need to force the dispatch of the device.")] = (
        None
    )
    vom_price: Annotated[FuelPrice, Field(description="Variable operational price $/MWh.")] | None = None
    prime_mover_type: (
        Annotated[PrimeMoversType, Field(description="Prime mover technology according to EIA 923.")] | None
    ) = None
    min_rated_capacity: Annotated[ActivePower, Field(description="Minimum rated power generation.")] = (
        0 * ureg.MW
    )
    ramp_up: (
        Annotated[
            PowerRate,
            Field(description="Ramping rate on the positve direction."),
        ]
        | None
    ) = None
    ramp_down: (
        Annotated[
            PowerRate,
            Field(description="Ramping rate on the negative direction."),
        ]
        | None
    ) = None
    min_up_time: (
        Annotated[
            Time,
            Field(ge=0, description="Minimum up time in hours for UC decision."),
        ]
        | None
    ) = None
    min_down_time: (
        Annotated[
            Time,
            Field(ge=0, description="Minimum down time in hours for UC decision."),
        ]
        | None
    ) = None
    mean_time_to_repair: (
        Annotated[
            Time,
            Field(gt=0, description="Total hours to repair after outage occur."),
        ]
        | None
    ) = None
    forced_outage_rate: (
        Annotated[
            Percentage,
            Field(description="Expected level of unplanned outages in percent."),
        ]
        | None
    ) = None
    planned_outage_rate: (
        Annotated[
            Percentage,
            Field(description="Expected level of planned outages in percent."),
        ]
        | None
    ) = None
    startup_cost: (
        Annotated[NonNegativeFloatType, Field(description="Cost in $ of starting a unit.")] | None
    ) = None

    @classmethod
    def example(cls) -> "Generator":
        return Generator(name="gen01", base_power=100.0 * ureg.MW, prime_mover_type=PrimeMoversType.PV)


class RenewableGen(Generator):
    """Abstract class for renewable generators."""


class RenewableDispatch(RenewableGen):
    """Curtailable renewable generator.

    This type of generator have a hourly capacity factor profile.
    """


class RenewableFix(RenewableGen):
    """Non-curtailable renewable generator.

    Renewable technologies w/o operational cost.
    """


class HydroGen(Generator):
    """Hydroelectric generator."""


class HydroDispatch(HydroGen):
    """Class representing flexible hydro generators."""

    ramp_up: (
        Annotated[
            PowerRate,
            Field(ge=0, description="Ramping rate on the positve direction."),
        ]
        | None
    ) = None
    ramp_down: (
        Annotated[
            PowerRate,
            Field(ge=0, description="Ramping rate on the negative direction."),
        ]
        | None
    ) = None


class HydroFix(HydroGen):
    """Class representing unflexible hydro."""


class HydroEnergyReservoir(HydroGen):
    """Class representing hydro system with reservoirs."""

    initial_energy: (
        Annotated[NonNegativeFloatType, Field(description="Initial water volume or percentage.")] | None
    ) = 0
    storage_capacity: (
        Annotated[
            Energy,
            Field(description="Total water volume or percentage."),
        ]
        | None
    ) = None
    min_storage_capacity: (
        Annotated[
            Energy,
            Field(description="Minimum water volume or percentage."),
        ]
        | None
    ) = None
    storage_target: (
        Annotated[
            Energy,
            Field(description="Maximum energy limit."),
        ]
        | None
    ) = None


class HydroPumpedStorage(HydroGen):
    """Class representing pumped hydro generators."""

    storage_duration: (
        Annotated[
            Time,
            Field(description="Storage duration in hours."),
        ]
        | None
    ) = None
    initial_volume: (
        Annotated[Energy, Field(gt=0, description="Initial water volume or percentage.")] | None
    ) = None
    storage_capacity: Annotated[
        Energy,
        Field(gt=0, description="Total water volume or percentage."),
    ]
    min_storage_capacity: (
        Annotated[
            Energy,
            Field(description="Minimum water volume or percentage."),
        ]
        | None
    ) = None
    pump_efficiency: Annotated[Percentage, Field(ge=0, le=1, description="Pumping efficiency.")] | None = None
    pump_load: (
        Annotated[
            ActivePower,
            Field(description="Load related to the usage of the pump."),
        ]
        | None
    ) = None

    @classmethod
    def example(cls) -> "HydroPumpedStorage":
        return HydroPumpedStorage(
            name="HydroStorage",
            base_power=ActivePower(100, "MW"),
            pump_load=ActivePower(100, "MW"),
            bus=ACBus.example(),
            prime_mover_type=PrimeMoversType.PS,
            storage_duration=Time(10, "h"),
            storage_capacity=Energy(1000, "MWh"),
            min_storage_capacity=Energy(10, "MWh"),
            pump_efficiency=Percentage(85, "%"),
            initial_volume=Energy(500, "MWh"),
            ext={"description": "Pumped hydro unit with 10 hour of duration"},
        )


class ThermalGen(Generator):
    """Class representing fuel based thermal generator."""

    fuel: Annotated[str, Field(description="Fuel category")] | None = None
    fuel_price: Annotated[FuelPrice, Field(description="Cost of using fuel in $/MWh.")] = FuelPrice(
        0.0, "usd/MWh"
    )
    heat_rate: Annotated[HeatRate | None, Field(description="Heat rate")] = None

    @classmethod
    def example(cls) -> "ThermalGen":
        return ThermalGen(
            name="ThermalGen",
            bus=ACBus.example(),
            fuel="gas",
            base_power=100 * ureg.MW,
            fuel_price=FuelPrice(5, "usd/MWh"),
            ext={"Additional data": "Additional value"},
        )


class ThermalStandard(ThermalGen):
    """Standard representation of thermal device."""


class ThermalMultiStart(ThermalGen):
    """We will fill this class once we have the need for it."""


class Storage(Generator):
    """Default Storage class."""

    storage_duration: (
        Annotated[
            Time,
            Field(description="Storage duration in hours."),
        ]
        | None
    ) = None
    storage_capacity: Annotated[
        Energy,
        Field(description="Maximum allowed volume or state of charge."),
    ]
    initial_energy: Annotated[Percentage, Field(description="Initial state of charge.")] | None = None
    min_storage_capacity: Annotated[Percentage, Field(description="Minimum state of charge")] = Percentage(
        0, "%"
    )
    max_storage_capacity: Annotated[Percentage, Field(description="Minimum state of charge")] = Percentage(
        100, "%"
    )


class GenericBattery(Storage):
    """Battery energy storage model."""

    charge_efficiency: Annotated[Percentage, Field(ge=0, description="Charge efficiency.")] | None = None
    discharge_efficiency: Annotated[Percentage, Field(ge=0, description="Discharge efficiency.")] | None = (
        None
    )


class HybridSystem(Device):
    """Representation of hybrid system with renewable generation, load, thermal generation and storage.

    This class is just a link between two components.
    For the implementation see:
    - https://github.com/NREL-Sienna/PowerSystems.jl/blob/main/src/models/HybridSystem.jl
    """

    storage_unit: Storage | None = None
    renewable_unit: RenewableGen | None = None
    thermal_unit: ThermalGen | None = None
    electric_load: PowerLoad | None = None
