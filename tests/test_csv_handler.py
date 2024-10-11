import pytest

from tempfile import NamedTemporaryFile

import polars as pl
from r2x.parser.parser_helpers import csv_handler
from r2x.parser.plexos import (
    PROPERTY_SV_COLUMNS_BASIC,
    PROPERTY_SV_COLUMNS_NAMEYEAR,
    PROPERTY_TS_COLUMNS_BASIC,
    PROPERTY_TS_COLUMNS_MDH,
    PROPERTY_TS_COLUMNS_MDP,
    PROPERTY_TS_COLUMNS_MONTH_PIVOT,
    PROPERTY_TS_COLUMNS_MULTIZONE,
    PROPERTY_TS_COLUMNS_PIVOT,
    PROPERTY_TS_COLUMNS_YM,
)


@pytest.mark.parametrize(
    "columns_case, csv_content",
    [
        (PROPERTY_SV_COLUMNS_BASIC, "name,value\nTemp,25.5\nLoad,1200\nPressure,101.3"),
        (
            PROPERTY_SV_COLUMNS_NAMEYEAR,
            "name,year,month,day,period,value\nTemp,2024,10,7,1,22.5\nLoad,2024,10,7,2,1150",
        ),
        # Test case 3: PROPERTY_TS_COLUMNS_BASIC
        (
            PROPERTY_TS_COLUMNS_BASIC,
            "year,month,day,period,value\n2024,10,7,1,23.0\n2024,10,7,2,24.5",
        ),
        # Test case 4: PROPERTY_TS_COLUMNS_MULTIZONE
        (PROPERTY_TS_COLUMNS_MULTIZONE, "year,month,day,period\n2024,10,7,1\n2024,10,7,2"),
        # Test case 5: PROPERTY_TS_COLUMNS_PIVOT
        (
            PROPERTY_TS_COLUMNS_PIVOT,
            "name,year,month,day,value\nTemp,2024,10,7,23.5\nLoad,2024,10,7,1150",
        ),
        # Test case 6: PROPERTY_TS_COLUMNS_YM
        (PROPERTY_TS_COLUMNS_YM, "year,month\n2024,10\n2023,9"),
        # Test case 7: PROPERTY_TS_COLUMNS_MDP
        (PROPERTY_TS_COLUMNS_MDP, "month,day,period\n10,7,1\n9,5,2"),
        # Test case 8: PROPERTY_TS_COLUMNS_MDH
        (
            PROPERTY_TS_COLUMNS_MDH,
            "name,month,day,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24\nRating,10,7,100,110,120,130,140,150,160,170,180,190,200,210,220,230,240,250,260,270,280,290,300,310,320",
        ),
        # Test case 9: PROPERTY_TS_COLUMNS_MONTH_PIVOT
        (
            PROPERTY_TS_COLUMNS_MONTH_PIVOT,
            "name,m01,m02,m03,m04,m05,m06,m07,m08,m09,m10,m11,m12\nRating,100,110,120,130,140,150,160,170,180,190,200,210",
        ),
    ],
)
def test_csv_handler(columns_case, csv_content):
    with NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as temp_file:
        temp_file.write(csv_content)
        temp_file.seek(0)

        df_csv = csv_handler(temp_file.name)
        assert isinstance(df_csv, pl.DataFrame)
