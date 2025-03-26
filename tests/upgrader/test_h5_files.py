import pandas as pd

from r2x.upgrader.checks import (
    check_if_columm_is_datetime,
    check_if_h5_has_correct_index_names,
    check_if_h5_is_pandas_format,
)
from r2x.upgrader.helpers import add_datetime_index, pandas_to_h5py, rename_index_names_from_h5


def test_pandas_checks(pandas_h5_weather_year):
    fpath = pandas_h5_weather_year
    assert check_if_h5_is_pandas_format(fpath)


def test_convert_weather_year_from_pandas(pandas_h5_weather_year, tmp_path):
    output_fpath = tmp_path / "output.h5"

    original_h5 = pd.read_hdf(pandas_h5_weather_year)

    assert pandas_to_h5py(original_h5, output_fpath)
    assert output_fpath.exists()
    assert check_if_columm_is_datetime(output_fpath)
    assert check_if_h5_has_correct_index_names(output_fpath)


def test_convert_solve_year_weather_year_from_pandas(pandas_h5_solve_year_and_weather_year, tmp_path):
    output_fpath = tmp_path / "output.h5"
    original_h5 = pd.read_hdf(pandas_h5_solve_year_and_weather_year)
    assert pandas_to_h5py(original_h5, output_fpath)
    assert output_fpath.exists()
    assert check_if_columm_is_datetime(output_fpath)
    assert check_if_h5_has_correct_index_names(output_fpath)


def test_convert_h5_no_datetime(h5_without_index_names, tmp_path):
    output_fpath = h5_without_index_names
    assert output_fpath.exists()
    assert not check_if_h5_has_correct_index_names(output_fpath)
    assert rename_index_names_from_h5(output_fpath)
    assert check_if_h5_has_correct_index_names(output_fpath)


def test_add_datetime_index(h5_with_index_names_no_datetime, tmp_path):
    output_fpath = h5_with_index_names_no_datetime
    assert output_fpath.exists()
    assert check_if_h5_has_correct_index_names(output_fpath)
    assert not check_if_columm_is_datetime(output_fpath)
    assert add_datetime_index(output_fpath)
    assert check_if_columm_is_datetime(output_fpath)
