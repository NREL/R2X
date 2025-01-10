import numpy
import pytest
from pint import Quantity
from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.enums import EmissionType
from r2x.models.services import Emission
from r2x.models.utils import Constraint, ConstraintMap
from r2x.plugins.emission_cap import update_system
from r2x.runner import run_parser, run_plugins


def test_update_system_default(reeds_data_folder, tmp_folder):
    config = Scenario.from_kwargs(
        name="5bus",
        run_folder=reeds_data_folder,
        output_folder=tmp_folder,
        input_model="reeds-US",
        output_model="plexos",
        solve_year=2035,
        weather_year=2012,
    )

    system, parser = run_parser(config)

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
    config.output_model = "sienna"
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
    emission = Emission(
        name="co2_emission", rate=10, generator_name="test_generator", emission_type=EmissionType.CO2
    )
    system.add_component(emission)
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
    emission = Emission(
        name="co2_emission", rate=10, generator_name="test_generator", emission_type=EmissionType.CO2
    )
    constraint_map = ConstraintMap(name="Constraints")
    constraint_map2 = ConstraintMap(name="Constraints")
    system.add_components(constraint_map, constraint_map2)
    system.add_component(emission)
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


def test_update_system_using_cli(reeds_data_folder, tmp_folder):
    config = Scenario.from_kwargs(
        name="Pacific",
        input_model="reeds-US",
        output_model="plexos",
        run_folder=reeds_data_folder,
        output_folder=tmp_folder,
        solve_year=2035,
        weather_year=2012,
        emission_cap=0.0,
        plugins=["emission_cap"],
    )

    system, parser = run_parser(config)
    new_system = run_plugins(config=config, parser=parser, system=system)
    assert isinstance(new_system, System)
    constraint = next(iter(new_system.get_components(Constraint)))
    assert constraint
    assert isinstance(constraint, Constraint)
    assert constraint.ext is not None
    assert constraint.ext.get("RHS Year") is not None
    assert isinstance(constraint.ext["RHS Year"], Quantity)
    assert constraint.ext["RHS Year"].magnitude == 0
