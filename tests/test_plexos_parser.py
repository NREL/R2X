from plexosdb.sqlite import PlexosSQLite
import pytest
from plexosdb import XMLHandler
from r2x.api import System
from r2x.config import Scenario
from r2x.exceptions import ParserError
from r2x.parser.handler import get_parser_data
from r2x.parser.plexos import PlexosParser

DB_NAME = "2-bus_example.xml"
MODEL_NAME = "main_model"


@pytest.fixture
def plexos_scenario(tmp_path, data_folder):
    return Scenario.from_kwargs(
        name="plexos_test",
        input_model="plexos",
        run_folder=data_folder,
        output_folder=tmp_path,
        model=MODEL_NAME,
        solve_year=2035,
        weather_year=2012,
        fmap={"xml_file": {"fname": DB_NAME, "model": "default"}},
    )


@pytest.fixture
def plexos_parser_instance(plexos_scenario):
    plexos_device_map = {"SolarPV_01": "RenewableFix", "ThermalCC": "ThermalStandard"}
    plexos_scenario.defaults["plexos_device_map"] = plexos_device_map
    return get_parser_data(plexos_scenario, parser_class=PlexosParser)


def test_plexos_parser_instance(plexos_parser_instance):
    assert isinstance(plexos_parser_instance, PlexosParser)
    assert len(plexos_parser_instance.data) == 1  # Plexos parser just parses a single file
    assert isinstance(plexos_parser_instance.data["xml_file"], XMLHandler)
    assert isinstance(plexos_parser_instance.db, PlexosSQLite)


@pytest.mark.skip(reason="We need a better test model")
def test_build_system(plexos_parser_instance):
    system = plexos_parser_instance.build_system()
    assert isinstance(system, System)


def test_raise_if_no_map_provided(tmp_path, data_folder):
    scenario = Scenario.from_kwargs(
        name="plexos_test",
        input_model="plexos",
        run_folder=data_folder,
        output_folder=tmp_path,
        solve_year=2035,
        model=MODEL_NAME,
        weather_year=2012,
        fmap={"xml_file": {"fname": DB_NAME}},
    )
    with pytest.raises(ParserError):
        _ = get_parser_data(scenario, parser_class=PlexosParser)
