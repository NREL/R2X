import pytest
from infrasys.time_series_models import SingleTimeSeries

from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.models import MonitoredLine, Emission, Generator, PowerLoad
from r2x.parser.handler import get_parser_data
from r2x.parser.reeds import ReEDSParser


@pytest.fixture
def scenario_instance(reeds_data_folder, default_scenario, tmp_folder):
    return Scenario.from_kwargs(
        name=default_scenario,
        input_model="reeds-US",
        output_model="plexos",
        run_folder=reeds_data_folder,
        output_folder=tmp_folder,
        solve_year=2050,
        weather_year=2012,
    )


@pytest.fixture
def reeds_parser_instance(scenario_instance):
    return get_parser_data(scenario_instance, parser_class=ReEDSParser)


def test_reeds_parser_instance(reeds_parser_instance):
    assert isinstance(reeds_parser_instance, ReEDSParser)


def test_parser_has_data(reeds_parser_instance):
    assert len(reeds_parser_instance.data) != 0


def test_system_creation(reeds_parser_instance):
    system = reeds_parser_instance.build_system()
    assert isinstance(system, System)


def test_construct_generators(reeds_parser_instance):
    reeds_parser_instance.system = System(name="Test", auto_add_composed_components=True)
    reeds_parser_instance._construct_buses()
    reeds_parser_instance._construct_reserves()
    reeds_parser_instance._construct_generators()
    generators = [component for component in reeds_parser_instance.system.get_components(Generator)]
    assert all(isinstance(component, Generator) for component in generators)
    assert (
        len([generator for generator in generators]) == 335
    )  # Total number of devices for the pacific scenario for 2050


def test_construct_load_time_series(reeds_parser_instance):
    load_df = reeds_parser_instance.get_data("load").collect()
    reeds_parser_instance.system = System(name="Test")
    reeds_parser_instance._construct_buses()
    reeds_parser_instance._construct_load()
    loads = [component for component in reeds_parser_instance.system.get_components(PowerLoad)]
    assert all(reeds_parser_instance.system.has_time_series(load) for load in loads)

    single_load = loads[0]
    ts: SingleTimeSeries = reeds_parser_instance.system.get_time_series(single_load)
    if len(load_df) > 8760:
        end_idx = 8760 * (reeds_parser_instance.config.weather_year - 2007) + 1  # +1 to be inclusive.
    else:
        end_idx = 8760
    assert len(ts.data) == 8760
    assert len(ts.data) == len(load_df[single_load.bus.name][end_idx - 8760 : end_idx])


@pytest.fixture
def reeds_system(reeds_parser_instance):
    return reeds_parser_instance.build_system()


def test_construct_emissions(reeds_system):
    emission_objects = reeds_system.get_components(Emission)
    emission_objects = [component for component in reeds_system.get_components(Emission)]
    assert all(isinstance(component, Emission) for component in emission_objects)
    assert (
        len(emission_objects) == 136
    )  # Total number of emissions (co2, nox) for all the emitting devices for 2050


def test_construct_branches(reeds_system):
    branch_objects = [component for component in reeds_system.get_components(MonitoredLine)]
    assert all(isinstance(component, MonitoredLine) for component in branch_objects)
    assert len(branch_objects) == 17  # With rating on both direction
