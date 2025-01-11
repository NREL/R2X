"""Testing for the configuration and Scenario class."""

import pytest
from r2x.config_models import PlexosConfig, SiennaConfig
from r2x.config_scenario import Scenario, Configuration, get_scenario_configuration
from r2x.utils import read_fmap


@pytest.fixture
def scenario(data_folder, tmp_folder):
    return Scenario(
        name="Test Scenario",
        run_folder=data_folder,
        output_folder=tmp_folder,
        input_model="plexos",
        output_model="sienna",
    )


def test_scenario_instance(scenario, data_folder, tmp_folder):
    assert isinstance(scenario, Scenario)
    assert scenario.name == "Test Scenario"
    assert scenario.input_model == "plexos"
    assert isinstance(scenario.input_config, PlexosConfig)
    assert scenario.output_model == "sienna"
    assert isinstance(scenario.output_config, SiennaConfig)
    assert scenario.run_folder == data_folder
    assert scenario.output_folder == tmp_folder


def test_scenario_mkdirs(scenario, tmp_folder):
    assert scenario.output_folder.exists()
    assert scenario.output_folder == tmp_folder


@pytest.mark.parametrize(
    "input_model, output_model, expected_fmap",
    [
        ("reeds-US", "plexos", read_fmap("r2x/defaults/reeds_us_mapping.json")),
        ("reeds-US", "sienna", read_fmap("r2x/defaults/reeds_us_mapping.json")),
        ("plexos", "sienna", read_fmap("r2x/defaults/plexos_mapping.json")),
        ("sienna", "plexos", read_fmap("r2x/defaults/sienna_mapping.json")),
    ],
    ids=["R2P", "R2S", "P2S", "SP"],
)
def test_scenario_fmap(input_model, output_model, expected_fmap):
    scenario = Scenario.from_kwargs(
        name=f"test-{input_model}", input_model=input_model, output_model=output_model
    )
    assert scenario.input_config
    assert scenario.input_config.fmap == expected_fmap


def test_scenario_from_kwargs(tmp_folder):
    kwargs = {
        "name": "Test",
        "weather_year": 2015,
        "solve_year": 2055,
        "output_folder": tmp_folder,
        "input_model": "reeds-US",
        "output_model": "sienna",
        "feature_flags": {"cool-feature": True},
    }
    scenario = Scenario.from_kwargs(**kwargs)
    assert isinstance(scenario, Scenario)
    assert scenario.input_model == "reeds-US"
    assert scenario.output_model == "sienna"
    assert scenario.feature_flags
    assert scenario.feature_flags.get("cool-feature", None)
    assert scenario.input_config
    assert hasattr(scenario.input_config, "weather_year")
    assert getattr(scenario.input_config, "weather_year", None) == 2015
    assert hasattr(scenario.input_config, "solve_year")
    assert getattr(scenario.input_config, "solve_year", None) == 2055


#
def test_config_from_cli():
    cli_input = {
        "name": "test",
        "weather_year": 2015,
        "solve_year": 2055,
        "input_model": "plexos",
        "output_model": "sienna",
        "feature_flags": {"cool-feature": True},
    }
    config = Configuration.from_cli(cli_args=cli_input)

    assert isinstance(config, Configuration)
    assert len(config) == 1
    assert config["test"].input_config
    assert isinstance(config["test"].input_config, PlexosConfig)

    user_dict = {"fmap": {"xml_file": {"fname": "path.xml", "another_field": True}}}
    config = Configuration.from_cli(cli_args=cli_input, user_dict=user_dict)
    assert isinstance(config, Configuration)
    assert len(config) == 1
    scenario = config["test"]
    assert scenario.input_config
    assert scenario.input_config.fmap["xml_file"]["fname"] == user_dict["fmap"]["xml_file"]["fname"]
    assert scenario.input_config.fmap["xml_file"]["another_field"]


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
    assert config["test2030"].input_config
    assert config["test2030"].input_config
    assert hasattr(config["test2030"].input_config, "weather_year")
    assert getattr(config["test2030"].input_config, "weather_year") == 2015


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
    config = get_scenario_configuration(cli_input, user_dict)
    assert config is not None


def test_get_config_cli_override():
    cli_args = {"name": "TestConfig", "input_model": "reeds-US", "output_model": "plexos"}
    user_dict = {"input_model": "plexos"}
    config = get_scenario_configuration(cli_args, user_dict)
    assert config is not None
    assert isinstance(config, Configuration)
    assert config["TestConfig"].input_model == "reeds-US"


def test_configuration_printing(scenario):
    scenario.info()
