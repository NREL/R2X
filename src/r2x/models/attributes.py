"""Supplemental attributes models."""

from typing import Annotated

from infrasys import SupplementalAttribute
from pydantic import Field

from r2x.enums import EmissionType
from r2x.models.named_tuples import GeoLocation
from r2x.units import EmissionRate


class GeographicInfo(SupplementalAttribute):
    """Supplemental attribute that capture location."""

    geo_json: GeoLocation

    @classmethod
    def example(cls) -> "GeographicInfo":
        return GeographicInfo(geo_json=GeoLocation(Latitude=10.5, Longitude=-100))


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
