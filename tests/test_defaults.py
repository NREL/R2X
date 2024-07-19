"""Test that we can read defaults data files and configurations."""

import json
import pytest
import importlib.resources as resources

DEFAULTS_PATH = "r2x.defaults"
DEFAULT_MAPS_NAME = ["plexos_input.json", "sienna_config.json"]


@pytest.fixture(scope="module")
def json_data() -> list[dict[str, str]]:
    loaded_data = []
    file_paths = [resources.files(DEFAULTS_PATH).joinpath(f) for f in DEFAULT_MAPS_NAME]
    for path in file_paths:
        with path.open() as file:
            loaded_data.append(json.load(file))
    return loaded_data


def test_json_data(json_data: list[dict[str, str]]) -> None:
    assert len(json_data) > 0
    for data in json_data:
        assert isinstance(data, dict)
        assert len(data) > 0
