"""Helper functions for the exporters."""

from typing import Any
from collections.abc import Callable
import copy
from functools import wraps
from r2x.enums import ReserveType, ReserveDirection
from r2x.exceptions import FieldRemovalError
import pint
from infrasys.base_quantity import BaseQuantity


def get_reserve_type(
    reserve_type: ReserveType, reserve_direction: ReserveDirection, reserve_types: dict[str, dict[str, str]]
) -> str:
    """Return the reserve type from a mapping.

    If not found, return default reserve type
    """
    for key, value in reserve_types.items():
        if value["type"] == reserve_type and value["direction"] == reserve_direction:
            return key
    return get_reserve_type(
        ReserveType[reserve_types["default"]["type"]],
        ReserveDirection[reserve_types["default"]["direction"]],
        reserve_types,
    )


def required_fields(*fields: str | list[str] | set):
    """Specify required fields for the transformation."""

    def decorator(
        func: Callable,
    ) -> Callable:
        @wraps(func)
        def wrapper(component_data, *args, **kwargs):
            original_data = copy.deepcopy(component_data)
            result = func(component_data)
            removed_fields = set(original_data.keys()) - set(result.keys())
            if removed_fields & set(fields):
                removed_required = removed_fields & set(fields)
                raise FieldRemovalError(
                    f"Transformation {func.__name__} removed required fields: {removed_required}"
                )
            return result

        return wrapper

    return decorator


def compose(
    *functions: Callable[[dict[str, Any]], dict[str, Any]],
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """
    Compose multiple functions, applying them sequentially to the input data.

    Parameters
    ----------
    functions : Callable[[Dict[str, Any]], Dict[str, Any]]
        Functions that transform a dictionary.

    Returns
    -------
    Callable[[Dict[str, Any]], Dict[str, Any]]
        A function that sequentially applies the provided functions to a dictionary.
    """

    def apply_all(data: dict[str, Any]) -> dict[str, Any]:
        for func in functions:
            data = func(data)
        return data

    return apply_all


def modify_components(
    *transform_functions: Callable[[dict[str, Any]], dict[str, Any]],
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Apply multiple transformations to components."""
    return compose(*transform_functions)


def apply_property_map(component: dict[str, Any], property_map: dict[str, str]) -> dict[str, Any]:
    """Apply a key mapping to component keys.

    For each key in the `component` dictionary, if the key exists in the `property_map`,
    it will be replaced by the corresponding mapped key. Otherwise, the original key is used.

    Parameters
    ----------
    component : dict[str, Any]
        The original dictionary where keys represent component properties.
    property_map : dict[str, str]
        A dictionary mapping old property names to new property names.

    Returns
    -------
    dict[str, Any]
        A new dictionary where the keys have been remapped according to `property_map`.

    Examples
    --------
    >>> component = {"voltage": 230, "current": 10}
    >>> property_map = {"voltage": "v", "current": "i"}
    >>> apply_property_map(component, property_map)
    {'v': 230, 'i': 10}

    >>> component = {"voltage": 230, "resistance": 50}
    >>> property_map = {"voltage": "v", "current": "i"}
    >>> apply_property_map(component, property_map)
    {'v': 230, 'resistance': 50}
    """
    return {property_map.get(key, key): value for key, value in component.items()}


def apply_pint_deconstruction(component: dict[str, Any], unit_map: dict[str, str]) -> dict[str, Any]:
    """Get property magnitude from a pint Quantity.

    If unit_map is passed, convert to units specified

    Parameters
    ----------
    component: dict[str, Any]
        Dictionary representation of component. Typically created with `.model_dump()`
    unit_map: dict[str, str]
        Map to convert property to a desired units. Optional.
    """
    return {
        property: get_property_magnitude(property_value, to_unit=unit_map.get(property, None))
        for property, property_value in component.items()
    }


def apply_valid_properties(
    component: dict[str, Any], valid_properties: list[str], add_name: bool = False
) -> dict[str, Any]:
    """Filter a component dictionary to only include keys that are in the valid properties list.

    Parameters
    ----------
    component : dict of str to Any
        A dictionary representing the component with properties as keys.
    valid_properties : list of str
        A list of valid property names. Only keys present in this list will be kept in the output.

    Returns
    -------
    dict
        A new dictionary with only the properties from `valid_properties`.

    Examples
    --------
    >>> component = {"voltage": 230, "current": 10, "resistance": 50}
    >>> valid_properties = ["voltage", "current"]
    >>> apply_valid_properties(component, valid_properties)
    {'voltage': 230, 'current': 10}
    """
    if add_name:
        default_properties = ["name"]
        return {
            key: value
            for key, value in component.items()
            if key in valid_properties or key in default_properties
        }
    return {key: value for key, value in component.items() if key in valid_properties}


def apply_unnest_key(component: dict[str, Any], key_map: dict[str, Any]) -> dict[str, Any]:
    """Unnest specific nested dictionary values based on a provided key map.

    This function processes a dictionary, potentially containing nested dictionaries,
    and unnests specific values based on a provided key map. For each key in the input
    dictionary that has a corresponding entry in the key map, if the value is a dictionary,
    it extracts the value using the mapped key.

    Parameters
    ----------
    component : dict[str, Any]
        The input dictionary to process.
    key_map : dict[str, Any]
        A dictionary mapping keys in the input dictionary to keys in nested dictionaries.

    Returns
    -------
    dict[str, Any]
        A new dictionary with unnested values based on the key map.

    Examples
    --------
    >>> component = {
    ...     "name": "Example",
    ...     "config": {"type": "A", "value": 10},
    ...     "data": {"content": "Some data"},
    ... }
    >>> key_map = {"config": "type", "data": "content"}
    >>> apply_unnest_key(component, key_map)
    {'name': 'Example', 'config': 'A', 'data': 'Some data'}

    Notes
    -----
    - If a key in the input dictionary is not in the key map, its value remains unchanged.
    - If a key is in the key map but the corresponding value in the input dictionary
      is not a dictionary, the value remains unchanged.
    - If a key is in the key map and the corresponding value is a dictionary, but the
      mapped key is not in this nested dictionary, the result for this key will be None.
    """
    if not key_map:
        return component
    return {
        key: value if not isinstance(value, dict) else value.get(key_map.get(key), value)
        for key, value in component.items()
    }


def get_property_magnitude(property_value, to_unit: str | None = None) -> Any:
    """Return magnitude with the given units for a pint Quantity.

    Parameters
    ----------
    property_name

    property_value
        pint.Quantity to extract magnitude from
    to_unit
        String that contains the unit conversion desired. Unit must be compatible.

    Returns
    -------
    float
        Magnitude representation of the `pint.Quantity` or original value.
    """
    if not isinstance(property_value, pint.Quantity | BaseQuantity):
        return property_value
    if to_unit:
        unit = to_unit.replace("$", "usd")  # Dollars are named usd on pint
        property_value = property_value.to(unit)
    return property_value.magnitude
