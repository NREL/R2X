import numpy
import polars
import pytest
from pint import Quantity

from r2x.api import System
from r2x.config_models import SiennaConfig
from r2x.config_scenario import Scenario
from r2x.enums import EmissionType
from r2x.exceptions import R2XModelError
from r2x.models import Emission
from r2x.models.generators import ThermalStandard
from r2x.models.utils import Constraint, ConstraintMap
from r2x.plugins.emission_cap import add_precombustion, update_system
from r2x.runner import run_parser, run_plugins
from r2x.units import EmissionRate


@pytest.fixture(scope="function")
def test_system(reeds_data_folder, tmp_path):
    config = Scenario.from_kwargs(
        name="ReEDS-TestSystem",
        run_folder=reeds_data_folder,
        output_folder=tmp_path,
        input_model="reeds-US",
        output_model="plexos",
        solve_year=2035,
        weather_year=2012,
        plugins=["emission_cap"],
    )
    system, parser = run_parser(config)
    return config, system, parser


def test_update_system_default(test_system):
    config, system, parser = test_system

    new_system = update_system(config=config, parser=parser, system=system)
    assert isinstance(new_system, System)

    constraint = next(iter(new_system.get_components(Constraint)))
    assert constraint
    assert isinstance(constraint, Constraint)
    assert constraint.ext is not None
    assert constraint.ext.get("RHS Year") is not None
    assert isinstance(constraint.ext["RHS Year"], Quantity)
    assert numpy.isclose(constraint.ext["RHS Year"].magnitude, 1.14e9, rtol=1e-02)

    constraint_map = next(iter(new_system.get_components(ConstraintMap)))
    assert constraint_map
    assert isinstance(constraint_map, ConstraintMap)
    assert EmissionType.CO2 in constraint_map.mapping[constraint.name]

    system, parser = run_parser(config)
    new_system = update_system(config=config, parser=parser, system=system, emission_cap=0.0)
    assert isinstance(new_system, System)
    constraint = next(iter(new_system.get_components(Constraint)))
    assert constraint
    assert isinstance(constraint, Constraint)
    assert constraint.ext is not None
    assert constraint.ext.get("RHS Year") is not None
    assert isinstance(constraint.ext["RHS Year"], Quantity)
    assert constraint.ext["RHS Year"].magnitude == 0

    # Test invalid models
    config.output_config = SiennaConfig()
    with pytest.raises(NotImplementedError):
        _ = update_system(config=config, parser=parser, system=system)


def test_no_emission(caplog, infrasys_test_system):
    config = Scenario.from_kwargs(
        name="Pacific",
        input_model="reeds-US",
        output_model="plexos",
        solve_year=2035,
        weather_year=2012,
        emission_cap=0.0,
        plugins=["emission_cap"],
    )

    _ = update_system(config=config, system=infrasys_test_system)
    assert "Did not find any emission" in caplog.text


def test_emission_but_no_cap(caplog):
    system = System(name="Test")
    gen1 = ThermalStandard.example()
    emission = Emission(rate=10, emission_type=EmissionType.CO2)
    system.add_supplemental_attribute(gen1, emission)
    config = Scenario.from_kwargs(
        name="Pacific",
        input_model="reeds-US",
        output_model="plexos",
        solve_year=2035,
        weather_year=2012,
        plugins=["emission_cap"],
    )

    _ = update_system(config=config, system=system)
    assert "Could not set emission cap value" in caplog.text


def test_multiple_constraint_maps():
    system = System(name="Test")
    gen1 = ThermalStandard.example()
    emission = Emission(rate=10, emission_type=EmissionType.CO2)
    constraint_map = ConstraintMap(name="Constraints")
    constraint_map2 = ConstraintMap(name="Constraints")
    system.add_components(constraint_map, constraint_map2)
    system.add_supplemental_attribute(gen1, emission)
    config = Scenario.from_kwargs(
        name="Pacific",
        input_model="reeds-US",
        output_model="plexos",
        solve_year=2035,
        weather_year=2012,
        plugins=["emission_cap"],
    )

    with pytest.raises(NotImplementedError):
        _ = update_system(config=config, system=system, emission_cap=0.0)


def test_update_system_using_cli(test_system):
    config, system, parser = test_system
    config.emission_cap = 0.0
    new_system = run_plugins(config=config, parser=parser, system=system)
    assert isinstance(new_system, System)
    constraint = next(iter(new_system.get_components(Constraint)))
    assert constraint
    assert isinstance(constraint, Constraint)
    assert constraint.ext is not None
    assert constraint.ext.get("RHS Year") is not None
    assert isinstance(constraint.ext["RHS Year"], Quantity)
    assert constraint.ext["RHS Year"].magnitude == 0


def test_emission_source(test_system, caplog):
    config, system, parser = test_system

    # Manually adding the switch
    # NOTE: We might need to modify this if we change the parsing of this file
    # https://github.com/NREL/R2X/issues/177
    adding_precombustion_switch = polars.DataFrame([{"aws": "gsw_precombustion", "0": "true"}])
    parser.data["switches"] = polars.concat([parser.data["switches"], adding_precombustion_switch])
    # Adding precombustion to the first generator
    adding_precombustion_entry = polars.DataFrame(
        [
            {
                "emission_type": "so2",
                "emission_source": "precombustion",
                "tech": "biopower",
                "region": "p10",
                "year": 2035,
                "tech_vintage": "init-2",
                "rate": -0.000642,  # Value to make it 0 for testing purposes
            }
        ]
    )
    parser.data["emission_rates"] = polars.concat([parser.data["emission_rates"], adding_precombustion_entry])
    new_system = update_system(config=config, parser=parser, system=system)
    assert "precombustion" in caplog.text

    # Check operation was correct
    generator_component = new_system.get_component(ThermalStandard, name="biopower_init-2_p10")
    assert generator_component
    attribute = new_system.get_supplemental_attributes_with_component(
        generator_component, Emission, filter_func=lambda x: x.emission_type == "SO2"
    )
    assert attribute
    assert len(attribute) == 1
    assert attribute[0].rate == 0.0


def test_add_precombustion(caplog):
    system = System(auto_add_composed_components=True)
    gen1 = ThermalStandard.example()
    system.add_component(gen1)
    emission_rates = polars.DataFrame(
        [{"generator_name": "ThermalStandard", "emission_type": "CO2", "rate": 10}]
    )
    assert not add_precombustion(system, emission_rates)
    assert "object not found" in caplog.text

    emission = Emission(rate=EmissionRate(10, "kg/MWh"), emission_type=EmissionType.CO2)
    system.add_supplemental_attribute(gen1, emission)
    assert add_precombustion(system, emission_rates)
    assert emission.rate.magnitude == 20

    emission_02 = Emission(rate=EmissionRate(10, "kg/MWh"), emission_type=EmissionType.CO2)
    system.add_supplemental_attribute(gen1, emission_02)
    with pytest.raises(R2XModelError):
        add_precombustion(system, emission_rates)
