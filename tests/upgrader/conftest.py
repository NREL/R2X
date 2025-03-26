import h5py
import numpy as np
import pandas as pd
import pytest
from _pytest.logging import LogCaptureFixture
from loguru import logger

WEATHER_YEARS = 7


@pytest.fixture
def caplog(caplog: LogCaptureFixture):
    handler_id = logger.add(
        caplog.handler,
        format="{message}",
        level=0,
        filter=lambda record: record["level"].no >= caplog.handler.level,
        enqueue=False,  # Set to 'True' if your test is spawning child processes.
    )
    yield caplog
    logger.remove(handler_id)


@pytest.fixture(scope="function")
def pandas_h5_weather_year(tmp_path):
    fpath = tmp_path / "pandas.h5"
    index = pd.Index(range(WEATHER_YEARS * 8760), name="index")
    data = {f"upv_{number}": np.random.rand(WEATHER_YEARS * 8760) for number in range(3)}
    pandas_data = pd.DataFrame(index=index, data=data)
    pandas_data.to_hdf(fpath, key="df", mode="w", format="fixed")
    return fpath


@pytest.fixture(scope="function")
def pandas_h5_solve_year_and_weather_year(tmp_path):
    fpath = tmp_path / "pandas.h5"

    unique_year_sorted = sorted(set(range(2030, 2050, 10)))
    unique_datetime_sorted = sorted(set(range(WEATHER_YEARS * 8760)))
    multilevel_index = pd.MultiIndex.from_product(
        [unique_year_sorted, unique_datetime_sorted], names=["year", "datetime"]
    )
    data_size = len(multilevel_index)
    data = {f"upv_{number}": np.random.rand(data_size) for number in range(3)}
    pandas_data = pd.DataFrame(index=multilevel_index, data=data)
    pandas_data.to_hdf(fpath, key="df", mode="w", format="fixed")
    return fpath


@pytest.fixture(scope="function")
def h5_without_index_names(tmp_path):
    fpath = tmp_path / "h5_no_index.h5"
    with h5py.File(fpath, "w") as f:
        f.create_dataset("index_0", data=[0])
        f.create_dataset("index_1", data=[0])
        f.create_dataset("columns", data=[0])
        f.create_dataset("index_names", data=[0])
        f.create_dataset("data", data=[0])
    return fpath


@pytest.fixture(scope="function")
def h5_with_index_names_no_datetime(tmp_path):
    fpath = tmp_path / "h5_no_index.h5"
    unique_year_sorted = sorted(set(range(2030, 2050, 10)))
    unique_datetime_sorted = sorted(set(range(WEATHER_YEARS * 8760)))
    data_keys = ["upv_1", "upv_2", "upv_3"]
    data = np.random.rand(WEATHER_YEARS * 8760, len(data_keys))
    with h5py.File(fpath, "w") as f:
        f.create_dataset("index_year", data=unique_year_sorted)
        f.create_dataset("index_datetime", data=unique_datetime_sorted)
        f.create_dataset("columns", data=data_keys)
        f.create_dataset("index_names", data=["year", "datetime"])
        f.create_dataset("data", data=data)
    return fpath
