"""Utilities for the configuration."""

from functools import singledispatch

# TODO consider moving elsewhere.
from .config_models import (
    BaseModelConfig,
    PlexosConfig,
    SiennaConfig,
)

@singledispatch
def get_year(model_class: BaseModelConfig):
    """Extract year variable from `BaseModelConfig`."""
    raise NotImplementedError


@get_year.register
def _(model_class: SiennaConfig):
    return model_class.model_year


@get_year.register
def _(model_class: PlexosConfig):
    return model_class.horizon_year
