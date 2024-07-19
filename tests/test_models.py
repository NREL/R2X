from r2x.enums import PrimeMoversType
from r2x.model import Generator, ACBus, Emission, HydroPumpedStorage
from r2x.units import EmissionRate, ureg


def test_generator_model():
    generator = Generator.example()
    assert isinstance(generator, Generator)
    assert isinstance(generator.base_power.magnitude, float)


def test_emission_objects():
    emission = Emission.example()

    assert isinstance(emission, Emission)
    assert isinstance(emission.rate, EmissionRate)
    assert isinstance(emission.emission_type, str)
    assert emission.emission_type == "CO2"


def test_bus_model():
    bus = ACBus.example()
    assert isinstance(bus, ACBus)
    assert isinstance(bus.id, int)


def test_generator_objects():
    bus = ACBus.example()
    generator = Generator(name="GEN01", base_power=100 * ureg.MW, bus=bus)
    assert isinstance(generator.bus, ACBus)


def test_pumped_hydro_generator():
    pumped_storage = HydroPumpedStorage.example()
    assert isinstance(pumped_storage, HydroPumpedStorage)
    assert isinstance(pumped_storage.bus, ACBus)
    assert isinstance(pumped_storage.prime_mover_type, PrimeMoversType)
    assert pumped_storage.prime_mover_type == PrimeMoversType.PS
