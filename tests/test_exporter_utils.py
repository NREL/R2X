import pytest
from pint import Quantity
from r2x.exporter.utils import (
    apply_property_map,
    apply_valid_properties,
    apply_pint_deconstruction,
    get_property_magnitude,
)


@pytest.mark.exporter_utils
def test_apply_property_map():
    """Test the apply_property_map function."""
    component = {"voltage": 230, "current": 10}
    property_map = {"voltage": "v", "current": "i"}

    result = apply_property_map(component, property_map)
    assert result == {"v": 230, "i": 10}

    component = {"voltage": 230, "resistance": 50}
    property_map = {"voltage": "v", "current": "i"}
    result = apply_property_map(component, property_map)
    assert result == {"v": 230, "resistance": 50}

    # Test for empty component
    result = apply_property_map({}, property_map)
    assert result == {}


@pytest.mark.exporter_utils
def test_apply_pint_deconstruction():
    """Test the apply_pint_deconstruction function."""
    component = {"length": Quantity(100, "meters"), "time": Quantity(3600, "seconds")}
    unit_map = {"length": "km", "time": "h"}

    result = apply_pint_deconstruction(component, unit_map)
    assert result["length"] == 0.1  # 100 meters to kilometers
    assert result["time"] == 1  # 3600 seconds to hours

    result = apply_pint_deconstruction(component, {})
    assert result["length"] == 100
    assert result["time"] == 3600


@pytest.mark.exporter_utils
def test_apply_valid_properties():
    """Test the apply_valid_properties function."""
    component = {"voltage": 230, "current": 10, "resistance": 50}
    valid_properties = ["voltage", "current"]

    # Test filtering
    result = apply_valid_properties(component, valid_properties)
    assert result == {"voltage": 230, "current": 10}

    component_with_name = {"voltage": 230, "current": 10, "resistance": 50, "name": "Component A"}
    valid_properties_with_name = ["voltage", "current"]

    result_with_name = apply_valid_properties(component_with_name, valid_properties_with_name, add_name=True)
    assert result_with_name == {"voltage": 230, "current": 10, "name": "Component A"}

    result_empty = apply_valid_properties(component, [])
    assert result_empty == {}


@pytest.mark.exporter_utils
def test_get_property_magnitude():
    """Test the get_property_magnitude function."""
    q1 = Quantity(100, "meters")  # Pint Quantity
    q2 = Quantity(50, "kilograms")  # Pint Quantity
    q3 = 200  # Not a Quantity

    assert get_property_magnitude(q1, "kilometers") == 0.1  # Convert 100 meters to kilometers
    assert get_property_magnitude(q2, "grams") == 50000  # Convert 50 kg to grams

    assert get_property_magnitude(q3) == 200  # No conversion for a non-Quantity
    assert get_property_magnitude(q1) == 100  # Magnitude of Quantity without conversion
