"""Utilities for the configuration."""

import inspect
from loguru import logger

from .config_models import BaseModelConfig, MODEL_CONFIGS, Models, ExporterModels, ParserModels

from .utils import read_json, read_fmap


def get_input_defaults(model_enum: Models) -> dict:
    """Return configuration dicitonary based on the input model."""
    defaults_dict = read_json("r2x/defaults/config.json")
    plugins_dict = read_json("r2x/defaults/plugins_config.json")

    defaults_dict = defaults_dict | plugins_dict
    match model_enum:
        case Models.INFRASYS:
            logger.debug("Returning infrasys defaults")
        case Models.REEDS:
            defaults_dict = defaults_dict | read_json("r2x/defaults/reeds_input.json")
            logger.debug("Returning reeds defaults")
        case Models.SIENNA:
            defaults_dict = defaults_dict | read_json("r2x/defaults/sienna_config.json")
            logger.debug("Returning sienna defaults")
        case Models.PLEXOS:
            defaults_dict = defaults_dict | read_json("r2x/defaults/plexos_input.json")
            logger.debug("Returning input_model {} defaults", model_enum)
        case _:
            msg = (
                f"Unsupported input model: {model_enum}. "
                f"Supported models: {[str(model) for model in ParserModels]}"
            )
            raise ValueError(msg)
    return defaults_dict


def get_output_defaults(model_enum: Models) -> dict:
    """Return configuration dicitonary based on the output model."""
    match model_enum:
        case Models.INFRASYS:
            # NOTE: Here we will add any infrasys configuration if we need in the future.
            defaults_dict = None
            pass
        case Models.PLEXOS:
            defaults_dict = (
                read_json("r2x/defaults/plexos_output.json")
                | read_json("r2x/defaults/plexos_simulation_objects.json")
                | read_json("r2x/defaults/plexos_horizons.json")
                | read_json("r2x/defaults/plexos_models.json")
            )
            logger.debug("Returning output_model {} defaults", model_enum)
        case Models.SIENNA:
            defaults_dict = read_json("r2x/defaults/sienna_config.json")
            logger.debug("Returning sienna defaults")
        case _:
            msg = (
                f"Unsupported input model: {model_enum}. "
                f"Supported models: {[str(model) for model in ExporterModels]}"
            )
            raise ValueError(msg)

    if not defaults_dict:
        return {}

    return defaults_dict


def get_input_model_fmap(model_enum: Models) -> dict:
    """Return input model file mape based on the model_name."""
    match model_enum:
        case Models.INFRASYS:
            fmap = {}
        case Models.REEDS:
            fmap = read_fmap("r2x/defaults/reeds_us_mapping.json")
        case Models.SIENNA:
            fmap = read_fmap("r2x/defaults/sienna_mapping.json")
        case Models.PLEXOS:
            fmap = read_fmap("r2x/defaults/plexos_mapping.json")
        case _:
            raise ValueError(f"Input model {model_enum=} not valid")
    return fmap


def get_model_config_class(model_enum: Models, **kwargs) -> BaseModelConfig:
    """Get the appropriate model configuration from the model name."""
    if model_enum not in MODEL_CONFIGS:
        msg = f"Unsupported model: {model_enum}. Supported models: {list(MODEL_CONFIGS.keys())}"
        raise ValueError(msg)

    model_config = MODEL_CONFIGS[model_enum]

    # Extract fields for the given config
    cls_fields = {field for field in inspect.signature(model_config).parameters}
    model_kwargs = {key: value for key, value in kwargs.items() if key in cls_fields}
    model_config_class = model_config(**model_kwargs)

    # Add fmap and defaults
    model_config_class.fmap = get_input_model_fmap(model_enum)
    return model_config_class
