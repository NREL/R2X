"""Useful function for models."""

from collections import defaultdict, namedtuple

from infrasys.models import InfraSysBaseModelWithIdentifers
from loguru import logger

from r2x.models.core import BaseComponent

from .costs import (
    HydroGenerationCost,
    OperationalCost,
    RenewableGenerationCost,
    StorageCost,
    ThermalGenerationCost,
)
from .generators import Generator, HydroGen, RenewableGen, Storage, ThermalGen

GeoLocation = namedtuple("GeoLocation", ["Latitude", "Longitude"])


class GeographicInfo(InfraSysBaseModelWithIdentifers):
    """Supplemental attribute that capture location."""

    geo_json: GeoLocation


class Constraint(BaseComponent): ...


class ConstraintMap(BaseComponent):
    mapping: defaultdict[str, list] = defaultdict(list)  # noqa: RUF012


def get_operational_cost(model: type["Generator"]) -> type["OperationalCost"] | None:
    """Return operational cost for the type of generator model."""
    match model:
        case _ if issubclass(model, ThermalGen):
            return ThermalGenerationCost
        case _ if issubclass(model, HydroGen):
            return HydroGenerationCost
        case _ if issubclass(model, RenewableGen):
            return RenewableGenerationCost
        case _ if issubclass(model, Storage):
            return StorageCost
        case _:
            msg = (
                f"{model=} does not have an operational cost. "
                "Check that a operational cost exist for the model."
            )
            logger.warning(msg)
            return None
