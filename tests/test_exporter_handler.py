from datetime import datetime, timedelta
from pathlib import Path

import pytest
from infrasys import SingleTimeSeries

from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.exporter import SiennaExporter
from r2x.models import ACBus, RenewableDispatch


@pytest.fixture
def scenario_instance(data_folder, tmp_folder):
    return Scenario.from_kwargs(
        name="TestScenario",
        run_folder=data_folder,
        output_folder=tmp_folder,
        input_model="infrasys",
        output_model="sienna",
        model_year=2010,
        reference_year=2010,
    )


@pytest.fixture
def sienna_exporter(scenario_instance, infrasys_test_system, tmp_folder):
    return SiennaExporter(config=scenario_instance, system=infrasys_test_system, output_folder=tmp_folder)


def test_export_data_files(scenario_instance, tmp_folder, sienna_exporter):
    system = sienna_exporter.system
    default_ts_directory = "Data"
    sienna_exporter.time_series_to_csv(
        scenario_instance,
        system,
        output_folder=tmp_folder,
        time_series_folder=default_ts_directory,
        time_series_fname=getattr(scenario_instance, "time_series_fname", None),
    )

    assert scenario_instance.output_folder
    assert (scenario_instance.output_folder / default_ts_directory).exists()
    time_series_fpath = scenario_instance.output_folder / default_ts_directory
    assert sum(1 for _ in Path(time_series_fpath).iterdir() if _.is_file()) == 3  # Count files


def test_time_series_to_csv_with_user_attributes(scenario_instance, tmp_folder):
    system = System()
    bus = ACBus(name="TestBus", number=1)
    gen1 = RenewableDispatch(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = RenewableDispatch(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1, gen2)

    length = 10
    initial_time = datetime(year=2020, month=1, day=1)
    time_array = [initial_time + timedelta(hours=i) for i in range(length)]
    data = range(length)
    variable_name = "active_power"
    ts = SingleTimeSeries.from_time_array(data, variable_name, time_array)
    system.add_time_series(ts, gen1, model_year=2020)

    default_ts_directory = "Data"
    _ = SiennaExporter.time_series_to_csv(
        scenario_instance, system, output_folder=tmp_folder, model_year=2025
    )
    assert scenario_instance.output_folder
    assert (scenario_instance.output_folder / default_ts_directory).exists()
    time_series_fpath = scenario_instance.output_folder / default_ts_directory
    assert sum(1 for _ in Path(time_series_fpath).iterdir() if _.is_file()) == 0  # Count files


def test_time_series_to_csv_with_multiple_lengths(scenario_instance):
    system = System()
    bus = ACBus(name="TestBus", number=1)
    gen1 = RenewableDispatch(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = RenewableDispatch(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1, gen2)

    # Length 10 time series
    length = 10
    initial_time = datetime(year=2020, month=1, day=1)
    time_array = [initial_time + timedelta(hours=i) for i in range(length)]
    data = range(length)
    variable_name = "active_power"
    ts = SingleTimeSeries.from_time_array(data, variable_name, time_array)
    system.add_time_series(ts, gen1)

    length = 20
    initial_time = datetime(year=2020, month=1, day=1)
    time_array = [initial_time + timedelta(hours=i) for i in range(length)]
    data = range(length)
    variable_name = "active_power"
    ts2 = SingleTimeSeries.from_time_array(data, variable_name, time_array)
    system.add_time_series(ts2, gen2)
    with pytest.raises(NotImplementedError):
        _ = SiennaExporter.time_series_to_csv(scenario_instance, system)
