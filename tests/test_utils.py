import json

import pytest
import yaml

from r2x.utils import haskey, read_user_dict


@pytest.mark.utils
@pytest.mark.parametrize(
    "base_dict, path, expected",
    [
        ({"a": {"b": {"c": 1}}}, ["a", "b", "c"], True),  # valid path
        ({"a": {"b": {"c": 1}}}, ["a", "b"], True),  # valid partial path
        ({"a": {"b": {"c": 1}}}, ["a", "x"], False),  # invalid key in path
        ({"a": {"b": {"c": 1}}}, ["a", "b", "d"], False),  # invalid path at last level
        ({}, ["a"], False),  # empty dictionary
        ({"a": {"b": {"c": 1}}}, ["a", "b", "c", "d"], False),  # path too long
    ],
)
def test_haskey(base_dict, path, expected):
    assert haskey(base_dict, path) == expected


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
