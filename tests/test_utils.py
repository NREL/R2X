import pytest
from r2x.utils import haskey


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
