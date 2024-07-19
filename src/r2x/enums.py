"""R2X enums."""

from enum import Enum, auto


class StrEnum(Enum):
    """Class to allow printing of Enums.

    NOTE: This class could be deprecated once most people migrate to
    python3.11. For the mean time, we are going to maintain this for backwards
    compatibility.
    """

    def __str__(self) -> str:
        return str(self.value)


class ReserveType(StrEnum):
    """Class representing different types of Reserves."""

    Spinning = auto()
    Flexibility = auto()
    Regulation = auto()


class ReserveDirection(StrEnum):
    """Class representing different Reserve Direction."""

    Up = auto()
    Down = auto()


class ACBusTypes(StrEnum):
    """Enum to define quantities for load flow calculation and categorize buses.

    For PCM translations, must of the buses are `PV`.
    """

    PV = "PV"


class PrimeMoversType(StrEnum):
    """EIA prime mover codes."""

    BA = "BA"
    BT = "BT"
    CA = "CA"
    CC = "CC"
    CE = "CE"
    CP = "CP"
    CS = "CS"
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
