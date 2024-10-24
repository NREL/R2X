import pytest
from datetime import datetime, timedelta
import polars as pl
from r2x.parser.plexos_utils import DATAFILE_COLUMNS, get_column_enum, time_slice_handler


def test_get_column_enum():
    """Test multiple cases for get_column_enum function."""
    columns = ["year", "random_column", "random_column_2"]
    assert get_column_enum(columns) == DATAFILE_COLUMNS.Y

    columns = ["name", "value"]
    assert get_column_enum(columns) == DATAFILE_COLUMNS.NV

    # Case 2: Exact match for TS_YMDPV
    columns = ["year", "month", "day", "period", "value"]
    assert get_column_enum(columns) == DATAFILE_COLUMNS.TS_YMDPV

    # Case 3: Subset match for TS_YM (with an extra column)
    columns = ["year", "month", "extra"]
    assert get_column_enum(columns) == DATAFILE_COLUMNS.TS_YM

    # Case 4: No match (completely unrelated columns)
    columns = ["random", "columns"]
    assert get_column_enum(columns) is None

    # Case 5: Partial match for NV (extra column in input)
    columns = ["name", "value", "extra"]
    assert get_column_enum(columns) == DATAFILE_COLUMNS.NV

    # Case 6: Exact match for TS_NMDH (large set of columns)
    columns = [
        "name",
        "month",
        "day",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "23",
        "24",
    ]
    assert get_column_enum(columns) == DATAFILE_COLUMNS.TS_NMDH


def test_time_slice_handler():
    records = [{"pattern": "M1-2", "value": 200}, {"pattern": "M3-12", "value": 100}]

    year = 2012
    hourly_time_index = pl.datetime_range(
        datetime(year, 1, 1), datetime(year + 1, 1, 1), interval="1h", eager=True, closed="left"
    ).to_frame("datetime")
    result_polars = time_slice_handler(records, hourly_time_index)

    assert all(result_polars[:100] == 200)
    assert all(result_polars[-100:] == 100)

    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)
    delta = timedelta(hours=1)
    datetime_index = tuple(start + i * delta for i in range((end - start) // delta))
    result_datetime = time_slice_handler(records, datetime_index)
    assert all(result_datetime == result_polars)


def test_time_slice_handler_raises():
    year = 2020
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)
    delta = timedelta(hours=1)
    datetime_index = tuple(start + i * delta for i in range((end - start) // delta))
    records = [{"pattern": "M1-2", "value": 200}, {"pattern": "M3-12", "value": 100}, [1, 2]]
    with pytest.raises(TypeError):
        _ = time_slice_handler(records, datetime_index)

    records = [{"pattern": "H1-2", "value": 200}, {"pattern": "M3-12", "value": 100}]
    with pytest.raises(NotImplementedError):
        _ = time_slice_handler(records, datetime_index)
