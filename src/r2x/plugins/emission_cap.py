"""Plugin to add annual carbon cap to the model.

This plugin is only applicable for ReEDs, but could work with similarly arrange data
"""

from argparse import ArgumentParser

from loguru import logger

from r2x.api import System
from r2x.config_scenario import Scenario
from r2x.enums import EmissionType
from r2x.models.services import Emission
from r2x.models.utils import Constraint, ConstraintMap
from r2x.parser.handler import BaseParser
from r2x.units import ureg
from r2x.utils import validate_string


def cli_arguments(parser: ArgumentParser):
    """CLI arguments for the plugin."""
    parser.add_argument(
        "--emission-cap",
        type=float,
        help="Emission cap for the solve year in tonnes. Defaults to CO2 or CO2e.",
    )


def update_system(
    config: Scenario,
    system: System,
    parser: BaseParser | None = None,
    emission_cap: float | None = None,
    default_unit: str = "tonne",
) -> System:
    """Apply an emission cap constraint for the system.

    This function adds to the sytem a :class:`~r2x.models.Constraint`object that is used to set the maximum
    emission per year.

    Parameters
    ----------
    config : Scenario
        The scenario configuration.
    parser : BaseParser
        The parser object used for parsing.
    system : System
        The system object to be updated.
    emission_cap : float or None, optional
        The emission cap value. If None, no cap is applied. Default is None.
    default_unit : str, optional
        The default unit for measurement. Default is 'tonne'.

    Notes
    -----
    When summarizing emissions from either fuels or generators, the metric model
    defines one unit in summary (day, week, month, year) as 1000 of the base units,
    whereas the imperial U.S. model uses 2000 units. Thus, if you define a
    constraint on total emissions over a day, week, month, or year, you must
    enter the limit in the appropriate unit. For example, if the production rate
    is in lb/MWh, then an annual constraint would be in short tons, where one
    short ton equals 2000 lbs. For units in kg/MWh and `emission_cap` in metric tons,
    we multiply by 1000 (`Scalar` property in Plexos).
    """
    if not config.output_model == "plexos":
        msg = "Plugin `emission_cap.py` is not compatible with a model that is not Plexos."
        raise NotImplementedError(msg)

    logger.info("Adding emission cap...")

    if emission_cap is not None:
        logger.debug("Using emission cap value from CLI. Setting emission cap to {}", emission_cap)

    emission_object = EmissionType.CO2  #  This is the default emission object.
    if not any(component.emission_type == emission_object for component in system.get_components(Emission)):
        logger.warning("Did not find any emission type to apply emission_cap")
        return system

    if parser is not None and config.input_model == "reeds-US" and emission_cap is None:
        assert parser.data.get("switches") is not None, "Missing switches file from run folder."
        switches = {key: validate_string(value) for key, value in parser.data["switches"].iter_rows()}
        emission_object = EmissionType.CO2E if switches["gsw_annualcapco2e"] else EmissionType.CO2
        assert parser.data.get("co2_cap", None) is not None, "co2_cap not found from ReEDS parser"
        emission_cap = parser.data["co2_cap"]["value"].item()

    if emission_cap is None:
        logger.warning("Could not set emission cap value. Skipping plugin.")
        return system

    emission_cap = ureg.Quantity(emission_cap, default_unit)

    # All of this are Plexos properties that need to be added
    constraint_properties = {
        "Sense": -1,
        "RHS Year": emission_cap,
        "Scalar": 1e3,
        "Penalty Price": 500,
        emission_object: {"Production Coefficient": 1},
    }
    constraint = Constraint(name=f"Annual_{emission_object}_cap", ext=constraint_properties)

    constraint_map = list(system.get_components(ConstraintMap))
    if len(constraint_map) > 1:
        msg = "Multiple constraint maps are not supported yet."
        raise NotImplementedError(msg)

    if not constraint_map:
        constraint_map = ConstraintMap(name="Constraints")
        system.add_component(constraint_map)

    if isinstance(constraint_map, list):
        constraint_map = constraint_map[0]

    constraint_map.mapping[constraint.name].append(emission_object)

    system.add_component(constraint)
    return system
