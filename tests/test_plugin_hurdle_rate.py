import pytest
from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.models.branch import MonitoredLine
from r2x.models.topology import ACBus, LoadZone
from r2x.plugins.hurdle_rate import update_system
from r2x.runner import run_parser


def test_hurdle_rate():
    system = System(name="TestSystem")
    region_1 = LoadZone(name="region_1")
    region_2 = LoadZone(name="region_2")
    system.add_components(region_1, region_2)

    bus_1 = ACBus(number=1, name="Bus1", load_zone=region_1)
    bus_2 = ACBus(number=2, name="Bus2", load_zone=region_1)
    bus_3 = ACBus(number=3, name="Bus3", load_zone=region_2)
    system.add_components(bus_1, bus_2, bus_3)

    # Line inside regions
    line_1_2 = MonitoredLine(
        name="1-2",
        from_bus=bus_1,
        to_bus=bus_2,
        ext={"Wheeling Charge": 0.001, "Wheeling Charge Back": 0.001},
    )
    # Line inside regions
    line_2_3 = MonitoredLine(name="2-3", from_bus=bus_2, to_bus=bus_3)
    # Line between regions
    line_1_3 = MonitoredLine(
        name="1-3",
        from_bus=bus_1,
        to_bus=bus_3,
        ext={"Wheeling Charge": 0.001, "Wheeling Charge Back": 0.001},
    )
    system.add_components(line_1_2, line_2_3, line_1_3)

    config = Scenario.from_kwargs(
        name="5bus",
        input_model="reeds-US",
        output_model="plexos",
        solve_year=2035,
        weather_year=2012,
    )
    hurdle_rate_value = 0.006
    new_system = update_system(config=config, system=system, hurdle_rate=hurdle_rate_value)
    assert isinstance(new_system, System)

    # Check line between regions
    updated_line_1_3 = system.get_component(MonitoredLine, name="1-3")
    assert isinstance(updated_line_1_3, MonitoredLine)
    assert updated_line_1_3.ext.get("Wheeling Charge")
    assert updated_line_1_3.ext["Wheeling Charge"] == hurdle_rate_value

    # Check line inside regions did not changed
    updated_line_1_2 = system.get_component(MonitoredLine, name="1-2")
    assert isinstance(updated_line_1_3, MonitoredLine)
    assert updated_line_1_2.ext.get("Wheeling Charge")
    assert updated_line_1_2.ext["Wheeling Charge"] == 0.001

    # Test invalid models
    config.output_model = "sienna"
    with pytest.raises(NotImplementedError):
        _ = update_system(config=config, system=system, hurdle_rate=hurdle_rate_value)


def test_hurdle_rate_with_parser(reeds_data_folder, tmp_folder):
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

    hurdle_rate_value = 0.006
    new_system = update_system(config=config, parser=parser, system=system, hurdle_rate=hurdle_rate_value)
    assert isinstance(new_system, System)
