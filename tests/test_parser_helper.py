import pytest
import polars as pl
from datetime import datetime

from r2x.parser.parser_helpers import (
    field_filter,
    fill_missing_timestamps,
    prepare_ext_field,
    reconcile_timeseries,
    resample_data_to_hourly,
)


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
            "period": [0, 1],
            "value": [1, 2],
        }
    )
    result_1 = resample_data_to_hourly(input_data_1)
    assert len(result_1) == 1  # Expecting 1 hourly value
    assert result_1["value"].to_list() == [1.5]  # Expected average value

    input_data_2 = pl.DataFrame(
        {
            "year": [2024] * 48,  # Single day, multiple periods
            "month": [10] * 48,
            "day": [24] * 48,
            "period": list(range(1, 49)),  # 1-48 represents 30-minute intervals
            "value": [10] * 48,  # Example values for each period
        }
    )

    result_2 = resample_data_to_hourly(input_data_2)

    # Check the result length and values
    assert len(result_2) == 24  # Expecting 24 hourly values
    assert result_2["value"].to_list() == [10] * 24  # Expected filled values


@pytest.fixture
def hourly_leap_year():
    year = 2020
    return pl.datetime_range(
        datetime(year, 1, 1), datetime(year + 1, 1, 1), interval="1h", eager=True, closed="left"
    ).to_frame("datetime")


@pytest.fixture
def hourly_non_leap_year():
    year = 2021
    return pl.datetime_range(
        datetime(year, 1, 1), datetime(year + 1, 1, 1), interval="1h", eager=True, closed="left"
    ).to_frame("datetime")


def test_fill_missing_timestamps():
    """Test filling missing timestamps and forward filling nulls."""
    year = 2020
    data_file = pl.DataFrame(
        {
            "year": [2020, 2020],
            "month": [1, 1],
            "day": [1, 1],
            "hour": [0, 1],  # Missing hour 1
            "value": [1, 3],
        }
    )

    hourly_time_index = pl.datetime_range(
        datetime(year, 1, 1), datetime(year, 1, 2), interval="1h", eager=True, closed="left"
    ).to_frame("datetime")

    # Call the function
    result = fill_missing_timestamps(data_file, hourly_time_index)

    # Assert that the result contains 24 rows (for each hour of the day)
    assert len(result) == 24

    data_file = pl.DataFrame({"name": ["testname"], "year": [2020], "month": [1], "value": [1]})
    result = fill_missing_timestamps(data_file, hourly_time_index)

    data_file = pl.DataFrame({"year": [2020], "value": [1]})
    with pytest.raises(ValueError):
        _ = fill_missing_timestamps(data_file, hourly_time_index)


def test_reconcile_timeseries_non_leap_year(hourly_non_leap_year, hourly_leap_year):
    # Extract year, month, day, and hour from the datetime column
    data_file = hourly_leap_year.with_columns(
        [
            pl.col("datetime").dt.year().alias("year"),
            pl.col("datetime").dt.month().alias("month"),
            pl.col("datetime").dt.day().alias("day"),
            pl.col("datetime").dt.hour().alias("hour"),
            (pl.arange(0, hourly_leap_year.height)).alias("value"),  # Sequential values
        ]
    )

    # Adjust data
    result = reconcile_timeseries(data_file, hourly_non_leap_year)

    # Expected result should remove Feb 29 data (1416 to 1440)
    assert result.height == 8760
    assert (result["value"] == list(range(1416)) + list(range(1440, 8784))).all()


def test_reconcile_timeseries_leap_year(hourly_non_leap_year, hourly_leap_year):
    # Data file with non-leap year length (8760 hours), leap year hourly_time_index
    data_file = pl.DataFrame(
        {
            "year": [2021] * 8760,
            "month": [2] * 8760,
            "day": [28] * 8760,
            "hour": list(range(8760)),
            "value": list(range(8760)),
        }
    )

    # Leap year hourly_time_index (8784 hours)
    hourly_time_index = pl.datetime_range(
        datetime(2020, 1, 1), datetime(2021, 1, 1), interval="1h", eager=True, closed="left"
    ).to_frame("datetime")

    # Adjust data
    result = reconcile_timeseries(data_file, hourly_time_index)

    # Check that the result has added Feb 29th data
    assert result.height == 8784


def test_reconcile_timeseries_raises_assertion():
    # Empty hourly_time_index
    hourly_time_index = pl.DataFrame()

    # Data file with arbitrary data
    data_file = pl.DataFrame({"year": [2021], "month": [2], "day": [28], "hour": [0], "value": [1]})

    # Check that AssertionError is raised
    with pytest.raises(AssertionError):
        reconcile_timeseries(data_file, hourly_time_index)
