import pytest
from r2x.config_scenario import Scenario
from r2x.exporter.plexos import PlexosExporter
from r2x.parser.handler import get_parser_data
from r2x.parser.reeds import ReEDSParser


@pytest.fixture
def scenario_instance(reeds_data_folder, default_scenario, tmp_folder):
    return Scenario.from_kwargs(
        name=default_scenario,
        input_model="reeds-US",
        run_folder=reeds_data_folder,
        output_model="plexos",
        output_folder=tmp_folder,
        solve_year=2050,
        weather_year=2012,
    )


@pytest.fixture
def reeds_parser_instance(scenario_instance):
    return get_parser_data(scenario_instance, parser_class=ReEDSParser)


@pytest.fixture
def reeds_system(reeds_parser_instance):
    return reeds_parser_instance.build_system()


@pytest.fixture
def plexos_exporter(scenario_instance, reeds_system, tmp_folder):
    return PlexosExporter(config=scenario_instance, system=reeds_system, output_folder=tmp_folder)


@pytest.mark.plexos
def test_plexos_exporter_instance(plexos_exporter):
    assert isinstance(plexos_exporter, PlexosExporter)


@pytest.mark.plexos
def test_plexos_exporter_run(plexos_exporter, default_scenario, tmp_folder):
    exporter = plexos_exporter.run()

    output_files = [
        f"{default_scenario}.xml",
    ]

    for file in output_files:
        assert (tmp_folder / file).exists()

    # Check that time series was created correctly
    ts_directory = tmp_folder / exporter.ts_directory
    assert any(ts_directory.iterdir())


@pytest.mark.plexos
def test_plexos_operational_cost(reeds_system, plexos_exporter): ...
