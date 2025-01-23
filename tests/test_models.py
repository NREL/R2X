from r2x.enums import PrimeMoversType
from r2x.models import Generator, ACBus, Emission, HydroPumpedStorage, ThermalStandard
from r2x.models import MinMax
from r2x.parser.handler import create_model_instance
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


def test_serialize_active_power_limits():
    active_power_limits = MinMax(min=0, max=100)
    generator = Generator(name="TestGEN", active_power_limits=active_power_limits)

    output = generator.model_dump()
    assert output["active_power_limits"] == {"min": 0, "max": 100}

    output = generator.serialize_active_power_limits(active_power_limits)
    assert output == {"min": 0, "max": 100}


def test_create_model_instance():
    name = "TestGen"
    generator = create_model_instance(Generator, name=name)
    assert isinstance(generator, Generator)
    assert isinstance(generator.name, str)
    assert generator.name == name

    name = ["TestGen"]
    generator = create_model_instance(Generator, name=name, skip_validation=True)
    assert isinstance(generator, Generator)
    assert isinstance(generator.name, list)
    assert generator.name == name
