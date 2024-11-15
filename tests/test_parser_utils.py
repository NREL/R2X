import pytest
import polars as pl
from pathlib import Path
from polars.testing import assert_frame_equal
from tempfile import NamedTemporaryFile
from r2x.parser.handler import csv_handler


@pytest.fixture
def sample_csv_basic():
    data = "ID,Name,Age\n1,Alice,30\n2,Bob,24"
    return data


@pytest.fixture
def temp_csv_file(sample_csv_basic):
    with NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as temp_file:
        temp_file.write(sample_csv_basic)
        temp_file.flush()
        return Path(temp_file.name)


def test_csv_handler_basic(temp_csv_file):
    """Test if csv_handler correctly reads a CSV and converts column names to lowercase."""
    fpath = temp_csv_file

    df_test = csv_handler(fpath)
    assert df_test is not None

    expected_df = pl.DataFrame({"id": [1, 2], "name": ["alice", "bob"], "age": [30, 24]})

    assert_frame_equal(df_test, expected_df)

    with pytest.raises(FileNotFoundError):
        _ = csv_handler(Path("non_existent_file.csv"))


@pytest.fixture
def temp_xml_file():
    with NamedTemporaryFile(mode="w", delete=False, suffix=".xml") as temp_file:
        return Path(temp_file.name)


def test_find_xml():
    pass
