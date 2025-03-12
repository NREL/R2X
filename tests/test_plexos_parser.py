import pytest
from plexosdb import XMLHandler
from plexosdb.sqlite import PlexosSQLite

from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.exceptions import R2XParserError
from r2x.parser.handler import get_parser_data
from r2x.parser.plexos import PlexosParser
from r2x.models import Generator

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
        user_dict={
            "fmap": {"xml_file": {"fname": DB_NAME, "model_name": MODEL_NAME}},
        },
    )


@pytest.fixture
def pjm_scenario(tmp_path, data_folder):
    return Scenario.from_kwargs(
        name="plexos_test",
        input_model="plexos",
        output_model="sienna",
        run_folder=data_folder / "pjm_2area",
        output_folder=tmp_path,
        model_year=2024,
        user_dict={
            "fmap": {"xml_file": {"fname": "pjm_2area.xml", "model_name": MODEL_NAME}},
        },
    )


@pytest.fixture
def five_bus_variables_scenario(tmp_path, data_folder):
    return Scenario.from_kwargs(
        name="plexos_test",
        input_model="plexos",
        output_model="sienna",
        run_folder=data_folder / "5_bus_system_variables",
        output_folder=tmp_path,
        model_year=2035,
        fmap={"xml_file": {"fname": "5_bus_system_variables.xml", "model": "Base"}},
        user_dict={
            "fmap": {"xml_file": {"fname": "5_bus_system_variables.xml", "model": "Base"}},
        },
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
        "solar": {"fuel": None, "type": "PVe"},
        "wind": {"fuel": None, "type": "WT"},
    }
    pjm_scenario.input_config.model_name = "model_2012"

    with pytest.raises(R2XParserError):
        parser = get_parser_data(pjm_scenario, parser_class=PlexosParser)

    pjm_scenario.input_config.defaults["plexos_category_map"] = plexos_category_map

    parser = get_parser_data(pjm_scenario, parser_class=PlexosParser)
    system = parser.build_system()
    assert isinstance(system, System)

    # PJM system has 48 components
    total_components = sum(1 for _ in system.iter_all_components())
    assert total_components == 48


def test_variable_parsing(five_bus_variables_scenario):
    plexos_category_map = {
        "thermal": {"fuel": "NATURAL_GAS", "type": "CC"},
        "solar": {"fuel": None, "type": "WT"},
        "wind": {"fuel": None, "type": "PV"},
    }
    five_bus_variables_scenario.input_config.model_name = "Base"

    five_bus_variables_scenario.input_config.defaults["plexos_category_map"] = plexos_category_map

    parser = get_parser_data(five_bus_variables_scenario, parser_class=PlexosParser)
    system = parser.build_system()
    assert isinstance(system, System)

    record_ts = {}
    for component in system.get_components(Generator):
        if not system.has_time_series(component):
            continue
        ts_metadata = system.get_time_series(component, "max_active_power")
        record_ts[component.name] = ts_metadata

    assert sum(record_ts["SolarPV1"].data.tolist()) == 4224.0
    assert "SolarPV2" not in record_ts
