import pytest
from plexosdb import XMLHandler
from plexosdb.sqlite import PlexosSQLite

from r2x.api import System
from r2x.config_scenario import Scenario
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
        output_model="sienna",
        run_folder=data_folder,
        output_folder=tmp_path,
        model_year=2035,
        fmap={"xml_file": {"fname": DB_NAME, "model_name": MODEL_NAME}},
    )


@pytest.fixture
def pjm_scenario(tmp_path, data_folder):
    return Scenario.from_kwargs(
        name="plexos_test",
        input_model="plexos",
        output_model="sienna",
        run_folder=data_folder / "pjm_2area",
        output_folder=tmp_path,
        model_year=2035,
        fmap={"xml_file": {"fname": "system.xml", "model_name": "default"}},
    )


@pytest.fixture
def plexos_parser_instance(plexos_scenario):
    plexos_device_map = {"SolarPV_01": "RenewableFix", "ThermalCC": "ThermalStandard"}
    plexos_scenario.input_config.defaults["plexos_device_map"] = plexos_device_map
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


def test_parser_system(pjm_scenario):
    plexos_category_map = {
        "thermal": {"fuel": "NATURAL_GAS", "type": "CC"},
        "solar": {"fuel": None, "type": "WT"},
        "wind": {"fuel": None, "type": "PV"},
    }
    pjm_scenario.input_config.model_name = "model_2012"

    with pytest.raises(ParserError):
        parser = get_parser_data(pjm_scenario, parser_class=PlexosParser)

    pjm_scenario.input_config.defaults["plexos_category_map"] = plexos_category_map

    parser = get_parser_data(pjm_scenario, parser_class=PlexosParser)
    system = parser.build_system()
    assert isinstance(system, System)

    # PJM system has 48 components
    total_components = sum(1 for _ in system.iter_all_components())
    assert total_components == 48
