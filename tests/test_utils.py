import json
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
import yaml

from r2x.utils import check_file_exists, haskey, override_dict, read_user_dict


@pytest.mark.utils
@pytest.mark.parametrize(
    "original_dict, path, expected",
    [
        ({"a": {"b": {"c": 1}}}, ["a", "b", "c"], True),  # valid path
        ({"a": {"b": {"c": 1}}}, ["a", "b"], True),  # valid partial path
        ({"a": {"b": {"c": 1}}}, ["a", "x"], False),  # invalid key in path
        ({"a": {"b": {"c": 1}}}, ["a", "b", "d"], False),  # invalid path at last level
        ({}, ["a"], False),  # empty dictionary
        ({"a": {"b": {"c": 1}}}, ["a", "b", "c", "d"], False),  # path too long
    ],
)
def test_haskey(original_dict, path, expected):
    assert haskey(original_dict, path) == expected


def test_read_user_dict(tmp_path):
    user_dict = '{"fmap": true}'
    parsed_dict = read_user_dict(user_dict)

    assert isinstance(parsed_dict, dict)
    assert parsed_dict.get("fmap", None) is not None

    user_dict = '{"fmap": true : 2}'
    with pytest.raises(ValueError):
        _ = read_user_dict(user_dict)

    user_dict = '[{"fmap": true}]'
    with pytest.raises(ValueError):
        _ = read_user_dict(user_dict)

    sample_data = {"name": "test", "fmap": True, "input_model": "plexos"}

    yaml_file = tmp_path / "data.yaml"
    with yaml_file.open("w") as f:
        yaml.dump(sample_data, f)

    assert yaml_file.exists()
    parsed_dict = read_user_dict(str(yaml_file))
    assert isinstance(parsed_dict, dict)
    assert parsed_dict.get("fmap", None) is not None
    assert parsed_dict["input_model"] == "plexos"

    json_file = tmp_path / "data.json"
    with json_file.open("w") as f:
        json.dump(sample_data, f, indent=4)

    assert json_file.exists()
    parsed_dict = read_user_dict(str(json_file))
    assert isinstance(parsed_dict, dict)
    assert parsed_dict.get("fmap", None) is not None
    assert parsed_dict["input_model"] == "plexos"

    yaml_file = tmp_path / "missing_file.yaml"
    with pytest.raises(FileNotFoundError):
        _ = read_user_dict(str(yaml_file))


@pytest.mark.parametrize(
    "original, override, expected",
    [
        (
            {"bio_fuel_price": {"fname": "oldfile.csv", "old-key": True}},
            {"bio_fuel_price": {"fname": "repbioprice_2030.csv"}},
            {"bio_fuel_price": {"fname": "repbioprice_2030.csv", "old-key": True}},
        ),
        (
            {"bio_fuel_price": {"fname": "oldfile.csv"}},
            {"bio_fuel_price": {"fname": "repbioprice_2030.csv", "new_key": True}},
            {"bio_fuel_price": {"fname": "repbioprice_2030.csv", "new_key": True}},
        ),
        (
            {"plexos_device_map": {"old-key": True}},
            {"plexos_device_map": {"Lone Mountain": {"fuel": "GAS"}}},
            {"plexos_device_map": {"Lone Mountain": {"fuel": "GAS"}, "old-key": True}},
        ),
        (
            {"tech_to_fuel_pm": {"gas": {"fuel": "GAS", "type": "BA"}}},
            {"tech_to_fuel_pm": {"coal": {"fuel": None, "type": "BA"}}},
            {
                "tech_to_fuel_pm": {
                    "gas": {"fuel": "GAS", "type": "BA"},
                    "coal": {"fuel": None, "type": "BA"},
                }
            },
        ),
        (
            {"plugins": ["default_plugin"]},
            {"plugins": {"_replace": True, "list": ["break_gens", "pcm_defaults"]}},
            {"plugins": {"list": ["break_gens", "pcm_defaults"]}},
        ),
        (
            {"plexos_device_map": {"Old Plant": {"fuel": "COAL"}}},
            {"plexos_device_map": {"_replace": True, "Lone Mountain": {"fuel": "GAS"}}},
            {"plexos_device_map": {"Lone Mountain": {"fuel": "GAS"}}},
        ),
        (
            {"existing_key": {"sub_key": "value1"}},
            {"new_section": {"new_sub_key": "value2"}},
            {
                "existing_key": {"sub_key": "value1"},
                "new_section": {"new_sub_key": "value2"},
            },
        ),
        (
            {"key1": "value1"},
            None,
            {"key1": "value1"},
        ),
        (
            {"key1": {"value1": True}},
            {"key1": {"value1": False}},
            {"key1": {"value1": False}},
        ),
        (
            {"key1": "value1"},
            {},
            {"key1": "value1"},
        ),
    ],
    ids=[
        "override-existing",
        "override-existing-merge-new",
        "merge-new-keys-empty",
        "merge-new-keys-existing",
        "replace",
        "replace-nested",
        "replace-new-key",
        "no-override",
        "override-single-key",
        "full-replace-empty",
    ],
)
def test_update_dict(original, override, expected):
    result = override_dict(original, override)
    assert result == expected


def test_check_file_exist():
    csv_file_contents = "name,value\nTemp,25.5\nLoad,1200\nPressure,101.3"
    with NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as temp_file:
        temp_file.write(csv_file_contents)
        temp_file.seek(0)

        path = Path(temp_file.name)
        folder = path.parent
        fpath = check_file_exists(path.name, folder, folder=folder)
        assert path == fpath
