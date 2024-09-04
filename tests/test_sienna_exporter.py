import pytest
from r2x.config import Scenario
from r2x.exporter.sienna import SiennaExporter
from .models import ieee5bus


@pytest.fixture
def infrasys_test_system():
    return ieee5bus()


@pytest.fixture
def scenario_instance(data_folder, tmp_folder):
    return Scenario(
        name="Test Scenario",
        run_folder=data_folder,
        output_folder=tmp_folder,
        input_model="infrasys",
        output_model="sienna",
        weather_year=2010,
    )


@pytest.fixture
def sienna_exporter(scenario_instance, infrasys_test_system, tmp_folder):
    return SiennaExporter(config=scenario_instance, system=infrasys_test_system, output_folder=tmp_folder)


def test_sienna_exporter_instance(sienna_exporter):
    assert isinstance(sienna_exporter, SiennaExporter)


def test_sienna_exporter_run(sienna_exporter, tmp_folder):
    exporter = sienna_exporter.run()

    output_files = [
        "gen.csv",
        "bus.csv",
        "timeseries_pointers.json",
        "storage.csv",
        # "reserves.csv",  # Reserve could be optional
        "dc_branch.csv",
        "branch.csv",
    ]

    for file in output_files:
        assert (tmp_folder / file).exists(), f"File {file} was not created properly."

    # Check that time series was created correctly
    ts_directory = tmp_folder / exporter.ts_directory
    assert any(ts_directory.iterdir())
