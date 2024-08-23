import pytest
from plexosdb import XMLHandler
from r2x.config import Scenario
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
    return get_parser_data(plexos_scenario, parser_class=PlexosParser)


def test_plexos_parser_instance(plexos_parser_instance):
    assert isinstance(plexos_parser_instance, PlexosParser)
    assert len(plexos_parser_instance.data) == 1  # Plexos parser just parses a single file
    assert isinstance(plexos_parser_instance.data["xml_file"], XMLHandler)
