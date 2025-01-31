"""Script that creates simple 2-area pjm systems for testing."""

import pathlib
from datetime import datetime, timedelta

from infrasys.cost_curves import CostCurve, UnitSystem
from infrasys.time_series_models import SingleTimeSeries
from infrasys.value_curves import LinearCurve

from r2x.api import System
from r2x.enums import ACBusTypes, PrimeMoversType, ReserveDirection, ReserveType, ThermalFuels
from r2x.models.branch import AreaInterchange, Line, MonitoredLine
from r2x.models.core import FromTo_ToFrom, MinMax, ReserveMap, UpDown
from r2x.models.costs import ThermalGenerationCost
from r2x.models.generators import RenewableDispatch, ThermalStandard
from r2x.models.load import PowerLoad
from r2x.models.services import Reserve
from r2x.models.topology import ACBus, Area, LoadZone
from r2x.units import ActivePower, Percentage, Time, Voltage, ureg
from r2x.utils import get_enum_from_string, read_json


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
    system.add_component(LoadZone(name="init"))
    system.add_component(LoadZone(name="Area2"))

    for bus in pjm_2area_components["bus"]:
        system.add_component(
            ACBus(
                name=bus["name"],
                number=bus["number"],
                bustype=ACBusTypes(bus["bustype"]),
                base_voltage=Voltage(bus["base_voltage"], "kV"),
                area=system.get_component(Area, bus["area"]),
                load_zone=system.get_component(LoadZone, bus["area"]),
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
                active_power_flow=0.0,
                reactive_power_flow=0.0,
                angle_limits=MinMax(min=-0.7, max=0.7),
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
        rating_down=-1000 * ureg.MW,
    )
    system.add_component(branch_monitored)

    # Add area interchange
    system.add_component(
        AreaInterchange(
            name="1_2",
            flow_limits=FromTo_ToFrom(from_to=-150, to_from=150),
            from_area=system.get_component(Area, "init"),
            to_area=system.get_component(Area, "Area2"),
            active_power_flow=0.0,
        )
    )

    # Add thermal generators
    for gen in pjm_2area_components["thermal"]:
        system.add_component(
            ThermalStandard(
                name=gen["Name"],
                fuel=get_enum_from_string(gen["fuel"].lower(), ThermalFuels),
                prime_mover_type=PrimeMoversType.ST,
                unit_type=PrimeMoversType.ST,
                active_power=ActivePower(100, "MW"),
                min_down_time=Time(gen["MinTimeDn"], "hour"),
                min_up_time=Time(gen["MinTimeUp"], "hour"),
                mean_time_to_repair=Time(10.0, "hour"),
                forced_outage_rate=Percentage(0.0),
                planned_outage_rate=Percentage(0.0),
                ramp_limits=UpDown(up=gen["RampLimitsUp"], down=gen["RampLimitsDn"]),
                bus=system.get_component(ACBus, gen["BusName"]),
                category="thermal",
                operation_cost=ThermalGenerationCost(
                    shut_down=gen["ShutDnCost"],
                    start_up=gen["StartupCost"],
                    variable=CostCurve(
                        value_curve=LinearCurve(14.0),
                        power_units=UnitSystem.NATURAL_UNITS,
                        vom_cost=LinearCurve(gen["VOM"]),
                    ),
                ),
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

    # Create reserve
    reserve_map = ReserveMap(name="pjm_reserve_map")
    reserve = Reserve(
        name="SpinUp-pjm",
        region=system.get_component(LoadZone, "init"),
        reserve_type=ReserveType.SPINNING,
        vors=0.05,
        duration=3600.0,
        load_risk=0.5,
        time_frame=3600,
        direction=ReserveDirection.UP,
    )
    reserve_map.mapping[ReserveType.SPINNING.name].append(wind_01.name)
    reserve_map.mapping[ReserveType.SPINNING.name].append(solar_pv_01.name)
    system.add_components(reserve, reserve_map)

    return system
