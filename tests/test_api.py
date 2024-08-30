import pytest
from r2x.__version__ import __data_model_version__
from r2x.api import System
from r2x.models import Area, ACBus, Generator
from r2x.units import ureg


@pytest.fixture(scope="class")
def empty_system():
    return System(name="TestSystem")


def test_system_instance(empty_system):
    assert isinstance(empty_system, System)


def test_system_data_model_version(empty_system):
    assert empty_system.version == __data_model_version__
    assert empty_system.data_format_version == __data_model_version__


def test_add_single_component(empty_system):
    area = Area.example()
    empty_system.add_component(area)
    assert isinstance(empty_system.get_component(Area, area.name), Area)


def test_add_composed_component():
    system = System(name="TestComposed", auto_add_composed_components=True)

    # Simple scenario of Generator with a bus attached
    bus = ACBus.example()
    generator = Generator(name="TestGen", active_power=100 * ureg.MW, bus=bus)
    system.add_component(generator)

    assert system.get_component(Generator, "TestGen") == generator
    assert system.get_component(Generator, "TestGen").bus == bus
