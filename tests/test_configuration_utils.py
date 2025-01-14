import pytest

from r2x.config_models import BaseModelConfig, Models, ReEDSConfig
from r2x.config_utils import (
    get_input_defaults,
    get_input_model_fmap,
    get_model_config_class,
    get_output_defaults,
)


def test_get_input_defaults():
    model = Models.REEDS

    defaults = get_input_defaults(model)
    assert isinstance(defaults, dict)

    with pytest.raises(ValueError):
        _ = get_input_defaults(Models.PRAS)


def test_get_output_defaults():
    model = Models.PLEXOS
    defaults = get_output_defaults(model)
    assert isinstance(defaults, dict)

    with pytest.raises(ValueError):
        _ = get_output_defaults(Models.REEDS)


def test_get_input_model_fmap():
    model = Models.REEDS
    defaults = get_input_model_fmap(model)
    assert isinstance(defaults, dict)

    with pytest.raises(ValueError):
        _ = get_input_model_fmap(Models.PRAS)


def test_get_model_config_class():
    model = Models.REEDS

    config = get_model_config_class(model)
    assert isinstance(config, BaseModelConfig)
    assert isinstance(config, ReEDSConfig)
