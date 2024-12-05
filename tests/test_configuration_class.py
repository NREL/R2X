"""Testing for the configuration and Scenario class."""

import pytest
from r2x.config import Scenario, Configuration, get_config
from r2x.utils import read_fmap


@pytest.fixture
def scenario_instance(data_folder, tmp_folder):
    return Scenario(
        name="Test Scenario",
        run_folder=data_folder,
        output_folder=tmp_folder,
        input_model="plexos",
        output_model="sienna",
    )


def test_scenario_instance(scenario_instance):
    assert isinstance(scenario_instance, Scenario)


def test_scenario_run_folder(scenario_instance, data_folder):
    assert scenario_instance.run_folder == data_folder


def test_scenario_outputfolder(scenario_instance, tmp_folder):
    assert scenario_instance.output_folder == tmp_folder


def test_scenario_mkdirs(scenario_instance, tmp_folder):
    assert scenario_instance.output_folder.exists()
    assert scenario_instance.output_folder == tmp_folder


@pytest.mark.parametrize(
    "input_model, expected_fmap",
    [
        (None, {}),
        ("plexos", read_fmap("r2x/defaults/plexos_mapping.json")),
        ("sienna", read_fmap("r2x/defaults/sienna_mapping.json")),
    ],
)
def test_scenario_fmap(input_model, expected_fmap):
    scenario = Scenario.from_kwargs(name=f"test-{input_model}", input_model=input_model)
    assert scenario.fmap == expected_fmap


def test_scenario_from_kwargs(tmp_folder):
    kwargs = {
        "name": "Test",
        "weather_year": 2015,
        "solve_year": 2055,
        "output_folder": tmp_folder,
        "input_model": "plexos",
        "output_model": "sienna",
        "feature_flags": {"cool-feature": True},
    }
    scenario = Scenario.from_kwargs(**kwargs)
    assert isinstance(scenario, Scenario)
    assert scenario.weather_year == kwargs["weather_year"]
    assert scenario.solve_year == kwargs["solve_year"]
    assert scenario.input_model == kwargs["input_model"]
    assert scenario.output_model == kwargs["output_model"]
    assert scenario.feature_flags == kwargs["feature_flags"]


def test_config_from_cli():
    cli_input = {
        "name": "Test",
        "weather_year": 2015,
        "solve_year": 2055,
        "input_model": "plexos",
        "output_model": "sienna",
        "feature_flags": {"cool-feature": True},
    }
    scenario_mgr = Configuration.from_cli(cli_args=cli_input)

    assert isinstance(scenario_mgr, Configuration)
    assert len(scenario_mgr) == 1


def test_config_from_scenarios():
    user_dict = {
        "input_model": "reeds-US",
        "output_model": "sienna",
        "scenarios": [
            {
                "name": "test2030",
                "weather_year": 2015,
                "solve_year": 2030,
            },
            {
                "name": "test2050",
                "weather_year": 2015,
                "solve_year": 2055,
            },
        ],
    }

    config = Configuration.from_scenarios({}, user_dict)
    assert config is not None
    assert len(config) == 2
    assert config["test2030"]
    assert config["test2030"].solve_year == 2030


@pytest.mark.parametrize(
    "cli_input,user_dict",
    [
        (
            {
                "name": "Test",
                "weather_year": 2015,
                "solve_year": [2055],
                "input_model": "plexos",
                "output_model": "sienna",
            },
            {},
        ),
        (
            {},
            {
                "input_model": "reeds-US",
                "output_model": "sienna",
                "scenarios": [
                    {
                        "name": "test2030",
                        "weather_year": 2015,
                        "solve_year": 2030,
                    },
                    {
                        "name": "test2050",
                        "weather_year": 2015,
                        "solve_year": 2055,
                    },
                ],
            },
        ),
        (
            {
                "name": "Test",
                "weather_year": 2015,
                "input_model": "plexos",
                "output_model": "sienna",
            },
            {
                "scenarios": [
                    {
                        "name": "test2030",
                        "solve_year": 2030,
                    },
                    {
                        "name": "test2050",
                        "solve_year": 2055,
                    },
                ],
            },
        ),
    ],
    ids=["no-user-dict", "no-cli", "both"],
)
def test_get_config(cli_input, user_dict):
    config = get_config(cli_input, user_dict)
    assert config is not None


def test_get_config_cli_override():
    cli_args = {"name": "TestConfig", "input_model": "reeds-US"}
    user_dict = {"input_model": "plexos"}
    config = get_config(cli_args, user_dict)
    assert config is not None
    assert config["TestConfig"].input_model == "reeds-US"


def test_config_override(scenario_instance):
    user_dict = {"fmap": {"xml_file": {"fname": "test_override"}}}
    cli_args = {"output_model": "sienna"}
    scenario = Configuration.override(scenario_instance.__dict__, user_dict=user_dict, cli_args=cli_args)
    assert isinstance(scenario, Scenario)
    assert scenario.output_model == "sienna"
    assert scenario.fmap["xml_file"]["fname"] == "test_override"


def test_configuration_printing(scenario_instance):
    scenario_instance.info()
