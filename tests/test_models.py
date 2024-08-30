from r2x.enums import PrimeMoversType
from r2x.models import Generator, ACBus, Emission, HydroPumpedStorage, ThermalStandard
from r2x.units import EmissionRate, ureg


def test_generator_model():
    generator = ThermalStandard.example()
    assert isinstance(generator, ThermalStandard)
    assert isinstance(generator.active_power.magnitude, float)


def test_emission_objects():
    emission = Emission.example()

    assert isinstance(emission, Emission)
    assert isinstance(emission.rate, EmissionRate)
    assert isinstance(emission.emission_type, str)
    assert emission.emission_type == "CO2"


def test_bus_model():
    bus = ACBus.example()
    assert isinstance(bus, ACBus)
    assert isinstance(bus.number, int)


def test_generator_objects():
    bus = ACBus.example()
    generator = Generator(name="GEN01", active_power=100 * ureg.MW, bus=bus)
    assert isinstance(generator.bus, ACBus)


def test_pumped_hydro_generator():
    pumped_storage = HydroPumpedStorage.example()
    assert isinstance(pumped_storage, HydroPumpedStorage)
    assert isinstance(pumped_storage.bus, ACBus)
    assert isinstance(pumped_storage.prime_mover_type, PrimeMoversType)
    assert pumped_storage.prime_mover_type == PrimeMoversType.PS
