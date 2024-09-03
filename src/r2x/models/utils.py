"""Useful function for models."""

from .generators import Generator, ThermalGen, HydroGen, Storage, RenewableGen
from .costs import (
    OperationalCost,
    ThermalGenerationCost,
    StorageCost,
    HydroGenerationCost,
    RenewableGenerationCost,
)
from loguru import logger


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
