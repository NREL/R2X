"""Helper functions for the exporters."""

from r2x.enums import ReserveType, ReserveDirection


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
