"""Script that creates a reduced 2 nodes system of 2 regions (FRCC, SERTP) with aggregated generation."""

from r2x.api import System
from r2x.enums import PrimeMoversType, ThermalFuels
from r2x.models import (
    Generator,
    ACBus,
    Area,
    LoadZone,
    MonitoredLine,
    RenewableDispatch,
    ThermalStandard,
    RenewableNonDispatch,
    HydroDispatch,
    HydroEnergyReservoir,
    HydroPumpedStorage,
)

from r2x.units import Energy, Time, Percentage, ActivePower, ureg
from r2x.models.named_tuples import UpDown

technology_mapping = {
    "biopower": {"type": PrimeMoversType.GT, "fuel": ThermalFuels.WOOD_WASTE, "device_type": ThermalStandard},
    "coal-igcc": {"type": PrimeMoversType.CC, "fuel": ThermalFuels.COAL, "device_type": ThermalStandard},
    "coal-new": {"type": PrimeMoversType.ST, "fuel": ThermalFuels.COAL, "device_type": ThermalStandard},
    "coaloldscr": {"type": PrimeMoversType.ST, "fuel": ThermalFuels.COAL, "device_type": ThermalStandard},
    "coalolduns": {"type": PrimeMoversType.ST, "fuel": ThermalFuels.COAL, "device_type": ThermalStandard},
    "gas-cc": {"type": PrimeMoversType.CC, "fuel": ThermalFuels.NATURAL_GAS, "device_type": ThermalStandard},
    "gas-ct": {"type": PrimeMoversType.CT, "fuel": ThermalFuels.NATURAL_GAS, "device_type": ThermalStandard},
    "lfill-gas": {
        "type": PrimeMoversType.CT,
        "fuel": ThermalFuels.MUNICIPAL_WASTE,
        "device_type": ThermalStandard,
    },
    "o-g-s": {"type": PrimeMoversType.GT, "fuel": ThermalFuels.OTHER, "device_type": ThermalStandard},
    "nuclear": {"type": PrimeMoversType.ST, "fuel": ThermalFuels.NUCLEAR, "device_type": ThermalStandard},
    "hyded": {"type": PrimeMoversType.HY, "fuel": "null", "device_type": HydroDispatch},
    "hydud": {"type": PrimeMoversType.HY, "fuel": "null", "device_type": HydroDispatch},
    "upv": {"type": PrimeMoversType.PVe, "fuel": "null", "device_type": RenewableDispatch},
    "wind-ons": {"type": PrimeMoversType.WT, "fuel": "null", "device_type": RenewableDispatch},
    "distpv": {"type": PrimeMoversType.RTPV, "fuel": "null", "device_type": RenewableNonDispatch},
    "pumped-hydro": {"type": PrimeMoversType.PS, "fuel": "null", "device_type": HydroPumpedStorage},
    "hydund": {"type": PrimeMoversType.HY, "fuel": None, "device_type": HydroEnergyReservoir},  # HYDRO FUEL
    "hydnpnd": {"type": PrimeMoversType.HY, "fuel": None, "device_type": HydroEnergyReservoir},  # HYDRO FUEL
    "hydend": {"type": PrimeMoversType.HY, "fuel": None, "device_type": HydroEnergyReservoir},  # HYDRO FUEL
}

frcc_node_gens = {
    'biopower': 200.0,
    'coal-igcc': 220.0,
    'coal-new': 467.0,
    'coaloldscr': 3522.0,
    'distpv': 894.8154541700001,
    'gas-cc': 39234.2,
    'gas-ct': 15647.599999999999,
    'hyded': 43.5,
    'hydend': 11.0,
    'lfill-gas': 472.4,
    'o-g-s': 5268.5,
    'upv': 3978.8062442299997,
    'nuclear': 3666.0
}

sertp_node_gens = {
    'coal-new': 3654.0,
    'distpv': 755.0390904799999,
    'gas-cc': 38813.2,
    'gas-ct': 37910.4,
    'lfill-gas': 198.4,
    'upv': 12410.511124050001,
    'coaloldscr': 27265.624000000003,
    'hyded': 6851.2,
    'hydend': 5023.7,
    'nuclear': 28019.399999999998,
    'o-g-s': 5093.2,
    'coalolduns': 1338.0,
    'biopower': 433.2,
    'pumped-hydro': 6475.7029999999995,
    'wind-ons': 237.1,
    'hydnpnd': 81.4,
    'hydud': 112.0,
    'hydund': 1.5
}

def create_generator(tech_name: str, power: float, tech_info: dict, bus: ACBus) -> Generator:
    """Create a generator based on its device type."""
    device_type = tech_info["device_type"]
    
    if device_type == ThermalStandard:
        return ThermalStandard(
            name=tech_name,
            fuel=tech_info["fuel"],
            prime_mover_type=tech_info["type"],
            active_power=power * ureg.MW,
            min_rated_capacity=power * 0.1 * ureg.MW,
            bus=bus,
            category="thermal",
        )
    
    if device_type == RenewableDispatch:
        category = "solar" if tech_info["type"] == PrimeMoversType.PVe else "wind"
        return RenewableDispatch(
            name=tech_name,
            bus=bus,
            prime_mover_type=tech_info["type"],
            active_power=power * ureg.MW,
            category=category,
        )
    
    if device_type == RenewableNonDispatch:
        return RenewableNonDispatch(
            name=tech_name,
            bus=bus,
            prime_mover_type=tech_info["type"],
            active_power=power * ureg.MW,
            power_factor=1.0,
            category="renewable"
        )
    
    if device_type == HydroPumpedStorage:
        return HydroPumpedStorage(
            name=tech_name,
            bus=bus,
            prime_mover_type=tech_info["type"],
            active_power=power * ureg.MW,
            storage_duration=Time(10, "h"),
            initial_volume=Energy(power * 5, "MWh"),
            storage_capacity=UpDown(up=power, down=power),
            min_storage_capacity=Energy(power * 0.1, "MWh"),
            pump_efficiency=Percentage(85, "%"),
            pump_load=ActivePower(power * 0.85, "MW"),
            category="hydro"
        )
    
    if device_type == HydroEnergyReservoir:
        return HydroEnergyReservoir(
            name=tech_name,
            bus=bus,
            prime_mover_type=tech_info["type"],
            active_power=power * ureg.MW,
            ramp_limits=UpDown(up=power * 0.1, down=power * 0.1),
            time_limits=UpDown(up=24, down=24),
            inflow=0.0,
            initial_energy=0,
            storage_capacity=Energy(power * 24, "MWh"),
            min_storage_capacity=Energy(power * 2.4, "MWh"),
            storage_target=Energy(power * 20, "MWh"),
            category="hydro"
        )
    
    if device_type == HydroDispatch:
        return HydroDispatch(
            name=tech_name,
            bus=bus,
            prime_mover_type=tech_info["type"],
            active_power=power * ureg.MW,
            category="hydro",
            ramp_up=100 * ureg.MW / ureg.h,
            ramp_down=100 * ureg.MW / ureg.h
        )

def add_generators_to_bus(system: System, node_gens: dict, tech_mapping: dict, bus: ACBus) -> None:
    """Add all types of generators to a bus."""
    for tech_name, power in node_gens.items():
        if tech_name in tech_mapping:
            tech_info = tech_mapping[tech_name]
            generator = create_generator(tech_name, power, tech_info, bus)
            system.add_component(generator)

def sys2bus() -> System:
    """Return an instance of the Aggregated reduced 2-bus system."""
    system = System(name="AggReduced 2-bus System", auto_add_composed_components=True)

    # Create FRCC bus
    area_frcc = Area(name="frcc")
    load_zone_frcc = LoadZone(name="LoadZoneFRCC")
    bus_frcc = ACBus(
        number=1,
        name="node_frcc",
        base_voltage=230 * ureg.volt,
        area=area_frcc,
        load_zone=load_zone_frcc,
        ext={"node1": "p101", "node2": "p102"},
    )

    # Create SERTP bus
    area_sertp = Area(name="sertp")
    load_zone_sertp = LoadZone(name="LoadZoneSERTP")
    bus_sertp = ACBus(
        number=2,
        name="node_sertp",
        base_voltage=230 * ureg.volt,
        area=area_sertp,
        load_zone=load_zone_sertp,
        ext={"node1": "p88", "node2": "p89", "node3": "p90", "node4": "p91", 
             "node5": "p92", "node6": "p93", "node7": "p94", "node8": "p95", 
             "node9": "p96", "node10": "p97", "node11": "p98"},
    )

    # Add buses to system
    for bus in [bus_frcc, bus_sertp]:
        system.add_component(bus)

    # Add generators to buses
    add_generators_to_bus(system, frcc_node_gens, technology_mapping, bus_frcc)
    add_generators_to_bus(system, sertp_node_gens, technology_mapping, bus_sertp)

    # Add branch between regions
    branch_frcc_sertp = MonitoredLine(
        name="br_frcc-sertp",
        active_power_flow=0.0,
        reactive_power_flow=0.0,
        from_bus=bus_frcc,
        to_bus=bus_sertp,
    )
    system.add_component(branch_frcc_sertp)

    return system