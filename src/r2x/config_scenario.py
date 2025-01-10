"""Configuration handler.

This script provides the base class to save the metadata for a given ReEDS scenario.
It can either read the information directly or throught a cases file.
"""

import inspect
import os
from collections import ChainMap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import rich
from loguru import logger
from rich.table import Table

from r2x.config_utils import get_input_defaults, get_model_config_class, get_output_defaults
from r2x.utils import get_enum_from_string, update_dict

from .config_models import BaseModelConfig, Models


@dataclass
class Scenario:
    """Scenario class.

    The purpose of the scenario class is to provide a way to track
    the inputs and defaults for a given translation. It serves as an access
    point to get the defaults, configuration and features flags for different
    steps (e.g., parser, exporter or validation) since most of the codebase use
    object composition.

    Each scenario can have its own set of flags that will run different parts
    of the parsers.

    A very simple use case for this class:

        from r2x.config_scenario import Scenario

        scenario = Scenario(name="test")
        print(scenario)

    Attributes
    ----------
    name
        Name for the scenario
    run_folder
        Path for the scenario inputs
    output_folder
        Path for output exports
    input_model
        Model to translate from
    output_model
        model to translate to
    feature_flags
        Dictionary with experimental features
    plugins
        List of plugins enabled

    See Also
    --------
    Configuration
    """

    name: str | None = None
    run_folder: Path | str | None = None
    output_folder: Path | str = Path(".")
    input_model: str | None = None
    output_model: str | None = None
    input_config: BaseModelConfig | None = None
    output_config: BaseModelConfig | None = None
    feature_flags: dict[str, Any] = field(default_factory=dict)
    plugins: list[Any] | None = None
    user_dict: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        self._normalize_path()
        self._set_scenario_name()
        self._load_plugins()
        self._load_model_config()

        # Overload configuration if the key appear on the config file
        if self.user_dict and self.input_config:
            self.input_config.defaults = update_dict(self.input_config.defaults, self.user_dict)
            self.input_config.fmap = update_dict(self.input_config.fmap, self.user_dict.get("fmap", {}))
        if self.user_dict and self.output_config:
            self.output_config.defaults = update_dict(self.output_config.defaults, self.user_dict)
        return None

    def __len__(self) -> int:
        """Return 1 scenario."""
        return 1

    def _normalize_path(self) -> None:
        if isinstance(self.output_folder, str):
            self.output_folder = Path(self.output_folder)

        # Create output folder if it does not exists.
        logger.trace(f"Creating folder {self.output_folder=}.")
        self.output_folder.mkdir(exist_ok=True)
        return

    def _set_scenario_name(self) -> None:
        if not self.name and self.run_folder:
            self.name = os.path.basename(self.run_folder)
        return None

    def _load_plugins(self) -> None:
        logger.debug("Using the following plugins: {}", self.plugins)
        return None

    def _load_model_config(self) -> None:
        if self.input_model:
            self._input_model_enum = get_enum_from_string(self.input_model, Models)
            self.input_config = get_model_config_class(self._input_model_enum)
            self.input_config.defaults = get_input_defaults(self._input_model_enum)
        if self.output_model:
            self._output_model_enum = get_enum_from_string(self.output_model, Models)
            self.output_config = get_model_config_class(self._output_model_enum)
            self.output_config.defaults = get_output_defaults(self._output_model_enum)
        return None

    def info(self) -> None:
        """Return table summary of the configuration."""
        config_table = Table(
            title="R2X Scenario Configuration",
            show_header=True,
            title_justify="left",
            title_style="bold",
        )
        config_table.add_column("Property", style="green", justify="left", min_width=20)
        config_table.add_column("Value", justify="left", min_width=20)

        for _field in self.__dict__:
            value = getattr(self, _field)
            if _field == "fmap":
                value = f"Length: {len(value)}"
            elif _field == "defaults":
                value = f"Length: {len(value)}"
            if value:
                config_table.add_row(_field, str(value))

        return rich.print(config_table)

    @classmethod
    def from_kwargs(
        cls, input_model: str, output_model: str, user_dict: dict | None = None, **kwargs
    ) -> "Scenario":
        """Create Scenario instance from key arguments."""
        cls_fields = {field for field in inspect.signature(cls).parameters}

        input_model_enum = get_enum_from_string(input_model, Models)
        input_config = get_model_config_class(input_model_enum)

        output_model_enum = get_enum_from_string(output_model, Models)
        output_config = get_model_config_class(output_model_enum)

        input_config_fields = {field for field in input_config.model_fields}
        output_config_fields = {field for field in output_config.model_fields}

        native_args, input_config_args, output_config_args, other_args = {}, {}, {}, {}
        for name, val in kwargs.items():
            if name in cls_fields:
                native_args[name] = val
            elif name in input_config_fields:
                input_config_args[name] = val
            elif name in output_config_fields:
                output_config_args[name] = val
            else:
                other_args[name] = val

        instance = cls(input_model=input_model, output_model=output_model, user_dict=user_dict, **native_args)

        # Set configurations if additional args were passed.
        if input_config_args:
            for name, value in input_config_args.items():
                setattr(instance.input_config, name, value)
        if output_config_args:
            for name, value in output_config_args.items():
                setattr(instance.output_config, name, value)

        for new_name, new_val in other_args.items():
            setattr(instance, new_name, new_val)
        return instance


@dataclass
class Configuration:
    """r2x.config_scenariouration manager that wraps multiple Scenario instances.

    This class parses either the cases_*.csv file or reads the inputs from the CLI.

    Attributes
    ----------
    scenarios_list
        Dictionary of scenarios to be translated.

    See Also
    --------
    Scenario
    """

    scenarios: dict[str, Scenario] = field(default_factory=dict)

    def __len__(self) -> int:
        """Get the number of scenarios in the class."""
        return len(self.scenarios)

    def __getitem__(self, scenario_name: str) -> Scenario:
        """Return a single scenario from the configuration.

        Parameters
        ----------
        name
            Name of the scenario

        Returns
        -------
            Scenario

        Raises
        ------
        KeyError
            If scenario does not exists.
        """
        for scenario in self.scenarios:
            if scenario == scenario_name:
                return self.scenarios[scenario]
        raise KeyError(f"No scenario named '{scenario_name}'")

    def __iter__(self):
        return iter(self.scenarios)

    def list_scenarios(self):
        """Return a list of scenarios in the configuration."""
        return self.scenarios.keys()

    @classmethod
    def from_cli(cls, cli_args: dict, user_dict: dict | None = None, **kwargs):
        """Create scenario from the CLI arguments.

        It saves the created scenario in the scenario_list` and scenario_names

        Parameters
        ----------
        kwargs
            Arguments for constructing the scenario.

        See Also
        --------
            Scenario.from_kwargs
        """
        instance = cls()

        mapping = ChainMap(cli_args, user_dict or {}, kwargs)
        scenario = Scenario.from_kwargs(user_dict=user_dict, **mapping)
        assert scenario.name
        instance.scenarios[scenario.name] = scenario
        return instance

    @classmethod
    def from_scenarios(cls, cli_args: dict, user_dict: dict, **kwargs):
        """Create scenario from scenarios on the config file.

        This method takes the `user_dict['scenarios'] key which is a list of dicts to create the different
        scenarios for translation. The order of override is CLI -> Global Keys -> Scenario Keys

        Parameters
        ----------
        cli_args
            Arguments for constructing the scenario.
        user_dict
            Configuration for the translation.

        See Also
        --------
            Scenario.from_kwargs
        """
        instance = cls()

        global_keys = {key: value for key, value in user_dict.items() if key != "scenarios"}

        for scenario_dict in user_dict["scenarios"]:
            scenario_dict = ChainMap(cli_args, global_keys, scenario_dict)
            scenario_class = Scenario.from_kwargs(**scenario_dict)
            assert scenario_class.name
            instance.scenarios[scenario_class.name] = scenario_class
        return instance


def get_scenario_configuration(cli_args: dict, user_dict: dict | None = None) -> Configuration:
    """Create configuration class using CLI arguments."""
    if solve_year := cli_args.get("solve_year"):
        cli_args["solve_year"] = solve_year[0] if len(solve_year) == 1 else solve_year

    if user_dict is not None and "scenarios" in user_dict:
        config = Configuration.from_scenarios(cli_args, user_dict)
    else:
        config = Configuration.from_cli(cli_args, user_dict=user_dict)
    assert config

    # if scenario_name := cli_args.get("scenario_name", None):
    #     config = config[scenario_name]
    return config
