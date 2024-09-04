from r2x.enums import PrimeMoversType
from r2x.models import Generator
from r2x.plugins.break_gens import break_generators
from .models import ieee5bus


def test_break_generators():
    system = ieee5bus()
    capacity_threshold = 10
    reference_generators = {
        "storage": {"avg_capacity_MW": 100},
    }
    system = break_generators(system, reference_generators, capacity_threshold, break_category="category")
    updated_generators = list(system.get_components(Generator))
    assert len(updated_generators) == 9  # 8 original generator + 1 new one

    new_generators = system.get_components(
        Generator, filter_func=lambda x: x.prime_mover_type == PrimeMoversType.BA
    )
    for generator in new_generators:
        assert generator.active_power.magnitude == 100
        assert generator.ext["original_capacity"].magnitude == 200
        assert generator.ext["broken"]


def test_break_generators_break_category():
    system = ieee5bus()
    capacity_threshold = 10
    reference_generators = {
        "Battery1": {"avg_capacity_MW": 100},
    }
    non_break_techs = ["storage", "Solitude", "Park City"]
    system = break_generators(
        system, reference_generators, capacity_threshold, non_break_techs, break_category="name"
    )

    updated_generators = list(system.get_components(Generator))
    assert len(updated_generators) == 9  # 8 original generator + 1 new ones


def test_break_generators_multi_category():
    system = ieee5bus()
    capacity_threshold = 10
    reference_generators = {
        "storage": {"avg_capacity_MW": 100},
        "thermal": {"avg_capacity_MW": 100},
        "solar": {"avg_capacity_MW": 100},
    }
    system = break_generators(system, reference_generators, capacity_threshold)

    updated_generators = list(system.get_components(Generator))
    assert len(updated_generators) == 28  # 8 original generator + 18 new ones


def test_break_generators_capacity_threshold():
    system = ieee5bus()
    capacity_threshold = 50
    reference_generators = {
        "storage": {"avg_capacity_MW": 150},
    }
    system = break_generators(system, reference_generators, capacity_threshold)

    updated_generators = list(system.get_components(Generator))
    assert len(updated_generators) == 8  # 8 original generator - 1 dropped
