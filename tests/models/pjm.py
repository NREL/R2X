"""Script that creates simple 2-area pjm systems for testing."""

from datetime import datetime, timedelta
import pathlib

from infrasys.time_series_models import SingleTimeSeries
from r2x.api import System
from r2x.enums import ACBusTypes, PrimeMoversType
from r2x.models.branch import AreaInterchange, Line, MonitoredLine
from r2x.models.generators import RenewableDispatch, ThermalStandard
from r2x.models.load import PowerLoad
from r2x.models.topology import ACBus, Area, LoadZone
from r2x.units import ActivePower, Percentage, Time, Voltage, ureg
from r2x.utils import read_json


def pjm_2area() -> System:
    """Return the PJM 2-area test system."""
    fpath = pathlib.Path(__file__).parent.parent  # So it points to tests
    fname = "data/pjm_2area_data.json"
    pjm_2area_components = read_json(str(fpath / fname))

    system = System(
        name="pjm 2-area system", auto_add_composed_components=True, description="Test system for PJM"
    )

    # Add topology elements
    system.add_component(Area(name="init"))
    system.add_component(Area(name="Area2"))
    system.add_component(LoadZone(name="LoadZone1"))

    for bus in pjm_2area_components["bus"]:
        system.add_component(
            ACBus(
                name=bus["name"],
                number=bus["number"],
                bus_type=ACBusTypes(bus["bustype"]),
                base_voltage=Voltage(bus["base_voltage"], "kV"),
                area=system.get_component(Area, bus["area"]),
                magnitude=bus["magnitude"],
            )
        )

    # Add branches
    for branch in pjm_2area_components["branch"]:
        busf = system.get_component(ACBus, branch["FromBus"])
        bust = system.get_component(ACBus, branch["ToBus"])
        system.add_component(
            Line(
                name=branch["Name"],
                from_bus=busf,
                to_bus=bust,
                x=branch["x"],
                r=branch["r"],
                rating=branch["MaxRating"] * ureg.MW,
            )
        )
    # Add MonitoredLine
    busf = system.get_component(ACBus, "Bus_nodeC_1")
    bust = system.get_component(ACBus, "Bus_nodeC_2")
    branch_monitored = MonitoredLine(
        name="inter_area_line",
        from_bus=busf,
        r=0.003,
        x=0.03,
        to_bus=bust,
        rating_up=1000 * ureg.MW,
        rating=1000 * ureg.MW,
    )
    system.add_component(branch_monitored)

    # Add area interchange
    system.add_component(
        AreaInterchange(
            name="1_2",
            max_power_flow=150.0 * ureg.MW,
            min_power_flow=-150.0 * ureg.MW,
            from_area=system.get_component(Area, "init"),
            to_area=system.get_component(Area, "Area2"),
        )
    )

    # Add thermal generators
    for gen in pjm_2area_components["thermal"]:
        system.add_component(
            ThermalStandard(
                name=gen["Name"],
                fuel=gen["fuel"].lower(),
                prime_mover_type=PrimeMoversType.ST,
                unit_type=PrimeMoversType.ST,
                rating=gen["Rating"] * ureg.MVA,
                active_power=ActivePower(100, "MW"),
                min_rated_capacity=gen["Pmin"] * ureg.MW,
                startup_cost=gen["StartupCost"] * ureg.Unit("usd"),
                shutdown_cost=gen["ShutDnCost"] * ureg.Unit("usd"),
                vom_price=gen["VOM"] * ureg.Unit("usd/MWh"),
                min_down_time=gen["MinTimeDn"],
                min_up_time=gen["MinTimeUp"],
                mean_time_to_repair=Time(10.0, "hour"),
                forced_outage_rate=Percentage(0.0),
                planned_outage_rate=Percentage(0.0),
                ramp_up=gen["RampLimitsUp"],
                ramp_down=gen["RampLimitsDn"],
                bus=system.get_component(ACBus, gen["BusName"]),
            )
        )

    # Solar generators
    solar_pv_01 = RenewableDispatch(
        name="PVBus5",
        bus=system.get_component(ACBus, "Bus_nodeC_1"),
        prime_mover_type=PrimeMoversType.PV,
        unit_type=PrimeMoversType.PV,
        active_power=384 * ureg.MW,
        category="solar",
    )
    system.add_component(solar_pv_01)

    wind_01 = RenewableDispatch(
        name="WindBus1",
        bus=system.get_component(ACBus, "Bus_nodeA_2"),
        prime_mover_type=PrimeMoversType.WT,
        unit_type=PrimeMoversType.WT,
        active_power=451.0 * ureg.MW,
        category="wind",
    )
    system.add_component(wind_01)

    # Add renewable profiles
    initial_time = datetime(year=2024, month=1, day=1)
    solar_array = pjm_2area_components["solar_ts"]  # Data is hourly resolution
    wind_array = pjm_2area_components["wind_ts"]  # Data is hourly resolution
    solar_ts = SingleTimeSeries.from_array(
        data=solar_array,
        initial_time=initial_time,
        resolution=timedelta(hours=1),
        variable_name="max_active_power",
    )
    wind_ts = SingleTimeSeries.from_array(
        data=wind_array,
        initial_time=initial_time,
        resolution=timedelta(hours=1),
        variable_name="rated_capacity",
    )
    #
    system.add_time_series(solar_ts, solar_pv_01)
    system.add_time_series(wind_ts, wind_01)

    initial_time = datetime(year=2024, month=1, day=1)
    for load in pjm_2area_components["load"]:
        load_component = PowerLoad(
            name=load["Name"],
            bus=system.get_component(ACBus, load["BusName"]),
            max_active_power=load["MaxLoad"] * ureg.MW,
        )
        ld_ts = SingleTimeSeries.from_array(
            data=load["ts"],
            initial_time=initial_time,
            resolution=timedelta(hours=1),
            variable_name="max_active_power",
        )
        system.add_component(load_component)
        system.add_time_series(ld_ts, load_component)
    return system
