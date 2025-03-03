"""Supplemental attributes models."""

from typing import Annotated

from infrasys import SupplementalAttribute
from pydantic import Field

from r2x.enums import EmissionType
from r2x.units import EmissionRate


class Emission(SupplementalAttribute):
    """Class representing an emission object that is attached to generators."""

    rate: Annotated[EmissionRate, Field(description="Amount of emission produced in kg/MWh.")]
    emission_type: Annotated[EmissionType, Field(description="Type of emission. E.g., CO2, NOx.")]

    @classmethod
    def example(cls) -> "Emission":
        return Emission(
            rate=EmissionRate(105, "kg/MWh"),
            emission_type=EmissionType.CO2,
        )
