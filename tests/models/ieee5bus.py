"""Script that creates simple systems for testing."""

from datetime import datetime, timedelta

from infrasys.cost_curves import FuelCurve
from infrasys.function_data import LinearFunctionData
from infrasys.time_series_models import SingleTimeSeries
from infrasys.value_curves import InputOutputCurve
from r2x.api import System
from r2x.enums import PrimeMoversType
from r2x.models import (
    ACBus,
    Area,
    GenericBattery,
    LoadZone,
    MonitoredLine,
    RenewableDispatch,
    ThermalStandard,
)
from r2x.models.costs import ThermalGenerationCost
from r2x.units import Energy, Percentage, Time, ureg


def ieee5bus() -> System:
    """Return an instance of the IEE 5-bus system."""
    system = System(name="IEEE 5-bus System", auto_add_composed_components=True)

    area_1 = Area(name="region1")
    load_zone_1 = LoadZone(name="LoadZone1")
    bus_1 = ACBus(number=1, name="node_a", base_voltage=100 * ureg.volt, area=area_1, load_zone=load_zone_1)
    bus_2 = ACBus(number=2, name="node_b", base_voltage=100 * ureg.volt, area=area_1, load_zone=load_zone_1)
    bus_3 = ACBus(number=3, name="node_c", base_voltage=100 * ureg.volt, area=area_1, load_zone=load_zone_1)
    bus_4 = ACBus(number=4, name="node_d", base_voltage=100 * ureg.volt, area=area_1, load_zone=load_zone_1)
    bus_5 = ACBus(number=5, name="node_e", base_voltage=100 * ureg.volt, area=area_1, load_zone=load_zone_1)
    # Append buses generator
    for bus in [bus_1, bus_2, bus_3, bus_4, bus_5]:
        system.add_component(bus)

    # Solar generators
    initial_time = datetime(year=2012, month=1, day=1)
    ts = SingleTimeSeries.from_array(
        data=range(0, 8760),
        initial_time=initial_time,
        resolution=timedelta(hours=1),
        variable_name="rated_capacity",
    )
    solar_pv_01 = RenewableDispatch(
        name="SolarPV1",
        bus=bus_3,
        prime_mover_type=PrimeMoversType.PV,
        active_power=384 * ureg.MW,
        category="solar",
    )
    solar_pv_02 = RenewableDispatch(
        name="SolarPV2",
        bus=bus_4,
        prime_mover_type=PrimeMoversType.PV,
        active_power=384 * ureg.MW,
        category="solar",
    )
    system.add_component(solar_pv_01)
    system.add_component(solar_pv_02)
    system.add_time_series(ts, solar_pv_01, solar_pv_02)

    # Storage
    storage = GenericBattery(
        name="Battery1",
        bus=bus_2,
        prime_mover_type=PrimeMoversType.BA,
        active_power=200 * ureg.MW,
        charge_efficiency=Percentage(85, "%"),
        storage_capacity=Energy(800, "MWh"),
        storage_duration=Time(4, "h"),
        category="storage",
    )
    system.add_component(storage)

    # Thermal generators
    alta = ThermalStandard(
        name="Alta",
        fuel="gas",
        prime_mover_type=PrimeMoversType.CC,
        active_power=40 * ureg.MW,
        min_rated_capacity=10 * ureg.MW,
        bus=bus_1,
        category="thermal",
    )
    system.add_component(alta)
    brighton = ThermalStandard(
        name="Brighton",
        fuel="Gas",
        prime_mover_type=PrimeMoversType.CC,
        active_power=600 * ureg.MW,
        min_rated_capacity=150 * ureg.MW,
        bus=bus_5,
        category="thermal",
    )
    system.add_component(brighton)
    park_city = ThermalStandard(
        name="Park City",
        fuel="Gas",
        prime_mover_type=PrimeMoversType.CC,
        active_power=170 * ureg.MW,
        min_rated_capacity=20 * ureg.MW,
        operation_cost=ThermalGenerationCost(
            variable=FuelCurve(
                value_curve=InputOutputCurve(
                    function_data=LinearFunctionData(proportional_term=10, constant_term=0)
                ),
                fuel_cost=15,
            )
        ),
        bus=bus_1,
        category="thermal",
    )
    system.add_component(park_city)
    solitude = ThermalStandard(
        name="Solitude",
        fuel="Gas",
        prime_mover_type=PrimeMoversType.CC,
        active_power=520 * ureg.MW,
        operation_cost=ThermalGenerationCost(
            variable=FuelCurve(
                value_curve=InputOutputCurve(
                    function_data=LinearFunctionData(proportional_term=10, constant_term=0)
                ),
                fuel_cost=15,
            )
        ),
        bus=bus_3,
        category="thermal",
    )
    system.add_component(solitude)
    sundance = ThermalStandard(
        name="Sundance",
        fuel="Gas",
        prime_mover_type=PrimeMoversType.CC,
        active_power=400 * ureg.MW,
        min_rated_capacity=80 * ureg.MW,
        bus=bus_4,
        category="thermal",
    )
    system.add_component(sundance)

    # Branch
    branch_ab = MonitoredLine(name="line_ab", from_bus=bus_1, to_bus=bus_2, rating_up=400 * ureg.MW)
    system.add_component(branch_ab)

    branch_ad = MonitoredLine(name="line_ad", from_bus=bus_1, to_bus=bus_4, rating_up=400 * ureg.MW)
    system.add_component(branch_ad)

    branch_ae = MonitoredLine(name="line_ae", from_bus=bus_1, to_bus=bus_5, rating_up=400 * ureg.MW)
    system.add_component(branch_ae)

    branch_bc = MonitoredLine(name="line_bc", from_bus=bus_2, to_bus=bus_3, rating_up=400 * ureg.MW)
    system.add_component(branch_bc)

    branch_cd = MonitoredLine(name="line_cd", from_bus=bus_3, to_bus=bus_4, rating_up=400 * ureg.MW)
    system.add_component(branch_cd)

    branch_ed = MonitoredLine(name="line_ed", from_bus=bus_5, to_bus=bus_4, rating_up=240 * ureg.MW)
    system.add_component(branch_ed)

    return system
