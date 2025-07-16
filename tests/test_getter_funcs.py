import pytest
from pint import Quantity

from r2x.models import Generator
from r2x.models.named_tuples import MinMax, UpDown
from r2x.models.getters import get_value, get_ramp_limits, get_max_active_power, _get_multiplier
from r2x.units import BaseQuantity, get_magnitude


class TestGetValue:
    """Test get_value getter function with different value types."""

    def test_get_value_minmax(self):
        """Test get_value with MinMax value."""
        component = Generator(name="test_generator", base_power=BaseQuantity(100, "MW"))

        value = MinMax(min=0.5, max=1.0)
        result = get_value(value, component)

        assert isinstance(result, MinMax)
        assert result.min == 50.0
        assert result.max == 100.0

    def test_get_value_float(self):
        """Test get_value with float value."""
        component = Generator(name="test_generator", base_power=BaseQuantity(200, "MW"))

        value = 0.8
        result = get_value(value, component)

        assert result == 160.0

    def test_get_value_quantity(self):
        """Test get_value with Quantity value."""
        component = Generator(name="test_generator", base_power=BaseQuantity(50, "MW"))

        value = Quantity(0.6, "MW")
        result = get_value(value, component)

        assert result == 30.0

    def test_get_value_not_implemented(self):
        """Test get_value raises NotImplementedError for unsupported types."""
        component = Generator(name="test_generator", base_power=BaseQuantity(100, "MW"))

        with pytest.raises(NotImplementedError, match="`get_value` not implemented for"):
            get_value("unsupported_string", component)

    def test_get_value_no_base_power(self):
        """Test get_value when component has no base_power."""
        component = Generator(name="test_generator", base_power=None)

        value = 5.0
        result = get_value(value, component)

        assert result == 5.0


class TestGetMaxActivePower:
    """Test get_max_active_power getter function."""

    def test_get_max_active_power_generator(self):
        """Test get_max_active_power with Generator."""
        generator = Generator(
            name="test_generator",
            base_power=BaseQuantity(100, "MW"),
            active_power_limits=MinMax(min=0.3, max=1.0),
        )

        result = get_max_active_power(generator)

        assert result == 100.0

    def test_get_max_active_power_not_implemented(self):
        """Test get_max_active_power raises TypeError due to NotImplementedType usage."""
        unsupported_component = "Generator()"

        with pytest.raises(TypeError, match="NotImplementedType takes no arguments"):
            get_max_active_power(unsupported_component)


class TestGetRampLimits:
    """Test get_ramp_limits getter function."""

    def test_get_ramp_limits_generator(self):
        """Test get_ramp_limits with Generator having ramp_limits."""
        generator = Generator(
            name="test_generator", ramp_limits=UpDown(up=0.1, down=0.08), base_power=BaseQuantity(100, "MW")
        )

        result = get_ramp_limits(generator)

        assert isinstance(result, UpDown)
        assert get_magnitude(result.up) == 10.0
        assert get_magnitude(result.down) == 8.0

    def test_get_ramp_limits_no_ramp(self):
        """Test get_ramp_limits when Generator has no ramp_limits."""
        generator = Generator(
            name="test_generator",
        )
        generator.base_power = BaseQuantity(100, "MW")
        generator.ramp_limits = None

        with pytest.raises(KeyError, match="Ramp not defined for"):
            get_ramp_limits(generator)

    def test_get_ramp_limits_not_implemented(self):
        """Test get_ramp_limits raises NotImplementedType for unsupported components."""
        unsupported_component = "Generator()"

        with pytest.raises(TypeError, match="NotImplementedType takes no arguments"):
            get_ramp_limits(unsupported_component)


class TestGetMultiplier:
    """Test _get_multiplier getter function."""

    def test_get_multiplier_with_base_power(self):
        """Test _get_multiplier when component has base_power."""
        component = Generator(name="test_generator", base_power=BaseQuantity(150, "MW"))

        result = _get_multiplier(component)
        assert result == 150.0

    def test_get_multiplier_no_base_power(self):
        """Test _get_multiplier when component has no base_power."""
        component = Generator(name="test_generator", base_power=None)

        result = _get_multiplier(component)
        assert result == 1.0

    def test_get_multiplier_no_base_power_attribute(self):
        """Test _get_multiplier when component doesn't have base_power attribute."""
        component = Generator(
            name="test_generator",
        )

        result = _get_multiplier(component)
        assert result == 1.0
