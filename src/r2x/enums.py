"""Definition of enums used on R2X.

In R2X most of the enums are `StrEnums` to be able to export it to csv easier.
"""

from enum import StrEnum


class ReserveType(StrEnum):
    """Class representing different types of Reserves."""

    SPINNING = "SPINNING"
    FLEXIBILITY = "FLEXIBILITY"
    REGULATION = "REGULATION"


class ReserveDirection(StrEnum):
    """Class representing different Reserve Direction."""

    UP = "UP"
    DOWN = "DOWN"


class ACBusTypes(StrEnum):
    """Enum to define quantities for load flow calculation and categorize buses.

    For PCM translations, must of the buses are `PV`.
    """

    PV = "PV"
    PQ = "PQ"
    REF = "REF"


class PrimeMoversType(StrEnum):
    """EIA prime mover codes."""

    BA = "BA"
    BT = "BT"
    CA = "CA"
    CC = "CC"
    CE = "CE"
    CP = "CP"
    CS = "CSV"
    CT = "CT"
    ES = "ES"
    FC = "FC"
    FW = "FW"
    GT = "GT"
    HA = "HA"
    HB = "HB"
    HK = "HK"
    HY = "HY"
    IC = "IC"
    PS = "PS"
    OT = "OT"
    ST = "ST"
    PV = "PV"
    WT = "WT"
    WS = "WS"
    RTPV = "RTPV"


class ThermalFuels(StrEnum):
    """Thermal fuels that reflect options in the EIA annual energy review."""

    COAL = "COAL"
    WASTE_COAL = "WASTE_COAL"
    DISTILLATE_FUEL_OIL = "DISTILLATE_FUEL_OIL"
    WASTE_OIL = "WASTE_OIL"
    PETROLEUM_COKE = "PETROLEUM_COKE"
    RESIDUAL_FUEL_OIL = "RESIDUAL_FUEL_OIL"
    NATURAL_GAS = "NATURAL_GAS"
    OTHER_GAS = "OTHER_GAS"
    NUCLEAR = "NUCLEAR"
    AG_BIOPRODUCT = "AG_BIOPRODUCT"
    MUNICIPAL_WASTE = "MUNICIPAL_WASTE"
    WOOD_WASTE = "WOOD_WASTE"
    GEOTHERMAL = "GEOTHERMAL"
    OTHER = "OTHER"
