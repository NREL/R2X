"""R2X enums."""

from enum import auto, StrEnum


class ReserveType(StrEnum):
    """Class representing different types of Reserves."""

    SPINNING = auto()
    FLEXIBILITY = auto()
    REGULATION = auto()


class ReserveDirection(StrEnum):
    """Class representing different Reserve Direction."""

    UP = auto()
    DOWN = auto()


class ACBusTypes(StrEnum):
    """Enum to define quantities for load flow calculation and categorize buses.

    For PCM translations, must of the buses are `PV`.
    """

    PV = auto()
    PQ = auto()
    REF = auto()


class PrimeMoversType(StrEnum):
    """EIA prime mover codes."""

    BA = auto()
    BT = auto()
    CA = auto()
    CC = auto()
    CE = auto()
    CP = auto()
    CS = auto()
    CT = auto()
    ES = auto()
    FC = auto()
    FW = auto()
    GT = auto()
    HA = auto()
    HB = auto()
    HK = auto()
    HY = auto()
    IC = auto()
    PS = auto()
    OT = auto()
    ST = auto()
    PV = auto()
    WT = auto()
    WS = auto()
    RTPV = auto()


class ThermalFuels(StrEnum):
    """Thermal fuels that reflect options in the EIA annual energy review."""

    COAL = auto()
    WASTE_COAL = auto()
    DISTILLATE_FUEL_OIL = auto()
    WASTE_OIL = auto()
    PETROLEUM_COKE = auto()
    RESIDUAL_FUEL_OIL = auto()
    NATURAL_GAS = auto()
    OTHER_GAS = auto()
    NUCLEAR = auto()
    AG_BIPRODUCT = auto()
    MUNICIPAL_WASTE = auto()
    WOOD_WASTE = auto()
    GEOTHERMAL = auto()
    OTHER = auto()
