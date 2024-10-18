from pathlib import Path
import pytest

from tempfile import NamedTemporaryFile

import polars as pl
from r2x.parser.handler import csv_handler
from r2x.parser.plexos_utils import (
    DATAFILE_COLUMNS,
    get_column_enum,
)


@pytest.mark.parametrize(
    "expected_enum,csv_content",
    [
        (DATAFILE_COLUMNS.NV, "name,value\nTemp,25.5\nLoad,1200\nPressure,101.3"),
        (DATAFILE_COLUMNS.TS_NYV, "name,year,value\nTemp,2030,25.5\nLoad,2030,1200\nPressure,2030,101.3"),
    ],
)
def test_csv_handler(expected_enum, csv_content):
    with NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as temp_file:
        temp_file.write(csv_content)
        temp_file.seek(0)

        df_csv = csv_handler(Path(temp_file.name))
        assert isinstance(df_csv, pl.DataFrame)
        column_type = get_column_enum(df_csv.columns)
        assert column_type == expected_enum
