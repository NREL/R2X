import pytest
import polars as pl

from r2x.parser.parser_helpers import field_filter, prepare_ext_field, resample_data_to_hourly


@pytest.mark.parametrize(
    "property_fields, eligible_fields, expected_valid, expected_extra",
    [
        (
            {"field1": 10, "field2": None, "field3": "hello"},
            {"field1", "field2"},
            {"field1": 10},
            {"field3": "hello"},
        ),
        ({"field1": 10, "field3": "hello"}, {"field2"}, {}, {"field1": 10, "field3": "hello"}),
        ({}, {"field1", "field2"}, {}, {}),
        ({"field1": 10, "field2": 20}, {"field1", "field2"}, {"field1": 10, "field2": 20}, {}),
        ({"field1": None, "field2": 20, "field3": None}, {"field1", "field2"}, {"field2": 20}, {}),
    ],
)
def test_field_filter(property_fields, eligible_fields, expected_valid, expected_extra):
    valid, extra = field_filter(property_fields, eligible_fields)
    assert valid == expected_valid
    assert extra == expected_extra


def test_prepare_ext_field():
    # Test case 1: With extra fields containing eligible and non-eligible types
    valid_fields = {"field1": 10, "field2": "hello"}
    extra_fields = {"field3": [1, 2, 3], "field4": 42, "field5": None}
    result = prepare_ext_field(valid_fields, extra_fields)
    assert result == {"field1": 10, "field2": "hello", "ext": {"field4": 42}}

    # Test case 2: No extra fields
    valid_fields = {"field1": 10, "field2": "hello"}
    extra_fields = {}
    result = prepare_ext_field(valid_fields, extra_fields)
    assert result == {"field1": 10, "field2": "hello", "ext": {}}

    # Test case 3: Various types including eligible and non-eligible
    valid_fields = {"field1": 10, "field2": "hello"}
    extra_fields = {
        "field3": [1, 2, 3],  # Not eligible
        "field4": 42,  # Eligible
        "field5": "world",  # Eligible
        "field6": None,  # Not eligible
        "field7": 3.14,  # Eligible
    }
    result = prepare_ext_field(valid_fields, extra_fields)
    assert result == {
        "field1": 10,
        "field2": "hello",
        "ext": {"field4": 42, "field5": "world", "field7": 3.14},
    }

    # Test case 4: All non-eligible extra fields
    valid_fields = {"field1": 10, "field2": "hello"}
    extra_fields = {
        "field3": [1, 2, 3],  # Not eligible
        "field4": {1: "a"},  # Not eligible
        "field5": None,  # Not eligible
    }
    result = prepare_ext_field(valid_fields, extra_fields)
    assert result == {"field1": 10, "field2": "hello", "ext": {}}


def test_resample_data_to_hourly():
    """Test resampling of half-hourly data to hourly data."""
    # Test case 1: Resampling from half-hourly to hourly
    input_data_1 = pl.DataFrame(
        {
            "year": [2020, 2020],
            "month": [2, 2],
            "day": [28, 28],
            "hour": [0, 0],
            "minute": [0, 30],
            "value": [1, 2],
        }
    )
    result_1 = resample_data_to_hourly(input_data_1)
    assert len(result_1) == 1  # Expecting 1 hourly value
    assert result_1["value"].to_list() == [1.5]  # Expected average value

    input_data_2 = pl.DataFrame(
        {
            "year": [2020, 2020, 2020],
            "month": [2, 2, 2],
            "day": [28, 28, 28],
            "hour": [0, 1, 1],
            "minute": [0, 0, 30],
            "value": [1, None, 3],
        }
    )

    result_2 = resample_data_to_hourly(input_data_2)

    # Check the result length and values
    assert len(result_2) == 2  # Expecting 2 hourly values
    assert result_2["value"].to_list() == [1.0, 3.0]  # Expected filled values
