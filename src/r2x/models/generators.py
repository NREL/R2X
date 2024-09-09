"""Models for generator devices."""

from typing import Annotated

from pydantic import Field, NonNegativeFloat

from r2x.models.core import Device
from r2x.models.costs import HydroGenerationCost, RenewableGenerationCost, ThermalGenerationCost, StorageCost
from r2x.models.topology import ACBus
from r2x.models.load import PowerLoad
from r2x.enums import PrimeMoversType
from r2x.units import (
    ActivePower,
    FuelPrice,
    Percentage,
    PowerRate,
    ApparentPower,
    ureg,
    Time,
    Energy,
)


class Generator(Device):
    """Abstract generator class."""

    bus: Annotated[ACBus, Field(description="Bus where the generator is connected.")] | None = None
    rating: Annotated[
        ApparentPower | None,
        Field(ge=0, description="Maximum output power rating of the unit (MVA)."),
    ] = None
    active_power: Annotated[
        ActivePower,
        Field(
            description=(
                "Initial active power set point of the unit in MW. For power flow, this is the steady "
                "state operating point of the system."
            ),
        ),
    ]
    operation_cost: (
        ThermalGenerationCost | RenewableGenerationCost | HydroGenerationCost | StorageCost | None
    ) = None
    base_power: Annotated[
        ApparentPower | None,
        Field(
            gt=0,
            description="Base power of the unit (MVA) for per unitization.",
        ),
    ] = None
    must_run: Annotated[int | None, Field(description="If we need to force the dispatch of the device.")] = (
        None
    )
    vom_price: Annotated[FuelPrice, Field(description="Variable operational price $/MWh.")] | None = None
    prime_mover_type: (
        Annotated[PrimeMoversType, Field(description="Prime mover technology according to EIA 923.")] | None
    ) = None
    unit_type: Annotated[
        PrimeMoversType | None, Field(description="Prime mover technology according to EIA 923.")
    ] = None
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
    startup_cost: Annotated[NonNegativeFloat, Field(description="Cost in $ of starting a unit.")] | None = (
        None
    )
    shutdown_cost: (
        Annotated[NonNegativeFloat, Field(description="Cost in $ of shuting down a unit.")] | None
    ) = None


class RenewableGen(Generator):
    """Abstract class for renewable generators."""


class RenewableDispatch(RenewableGen):
    """Curtailable renewable generator.

    This type of generator have a hourly capacity factor profile.
    """


class RenewableNonDispatch(RenewableGen):
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
        Annotated[NonNegativeFloat, Field(description="Initial water volume or percentage.")] | None
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
            name="HydroPumpedStorage",
            active_power=ActivePower(100, "MW"),
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


class ThermalStandard(ThermalGen):
    """Standard representation of thermal device."""

    @classmethod
    def example(cls) -> "ThermalStandard":
        return ThermalStandard(
            name="ThermalStandard",
            bus=ACBus.example(),
            fuel="gas",
            active_power=100.0 * ureg.MW,
            ext={"Additional data": "Additional value"},
        )


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
