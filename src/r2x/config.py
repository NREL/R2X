"""Configuration handler.

This script provides the base class to save the metadata for a given ReEDS scenario.
It can either read the information directly or throught a cases file.
"""

# System packages
import inspect
import os
import pathlib
from collections import ChainMap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Third-party packages
from loguru import logger
import rich
from rich.table import Table

# Local imports
from .utils import get_defaults, match_input_model, update_dict


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

    A very simple use case for this class::

        from r2x.config import Scenario

        scenario = Scenario(name="test")
        print(scenario)

    Parameters
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
    fmap
        Dictionary with files configuration
    defaults
        Default configuration from the scenario
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
    solve_year: list[int] | int | None = None
    weather_year: int | None = None
    input_model: str | None = None
    output_model: str | None = None
    cases_flags: dict[str, Any] = field(default_factory=dict)
    fmap: dict[str, Any] = field(default_factory=dict)
    defaults: dict[str, Any] = field(default_factory=dict)
    feature_flags: dict[str, Any] = field(default_factory=dict)
    plugins: list[Any] | None = None
    _mkdir: bool = True

    def __str__(self) -> str:
        _str = (
            f"Scenario(name={self.name}, input_model={self.input_model}, output_model={self.output_model},"
            + f"run_folder={self.run_folder})"
        )
        return _str

    def __repr__(self) -> str:
        _str = (
            f"Scenario(name={self.name}, input_model={self.input_model}, output_model={self.output_model},"
            + f"run_folder={self.run_folder})"
        )
        return _str

    def info(self) -> None:
        """Return table summary of the configuration."""
        config_table = Table(
            title="R2X configuration",
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

    def __post_init__(self):
        self._normalize_path()
        self._set_scenario_name()
        self._load_fmap_config()
        self._load_defaults()

    def _normalize_path(self) -> None:
        if isinstance(self.output_folder, str):
            self.output_folder = pathlib.Path(self.output_folder)

        # Create output folder if it does not exists.
        if self._mkdir:
            logger.trace(f"Creating folder {self.output_folder=}.")
            self.output_folder.mkdir(exist_ok=True)
        return

    def _set_scenario_name(self) -> None:
        if not self.name and self.run_folder:
            self.name = os.path.basename(self.run_folder)
        return

    def _load_fmap_config(self) -> None:
        if self.input_model and not self.fmap:
            self.fmap = match_input_model(self.input_model)
            logger.debug(f"Getting fmap configuration for {self.input_model}")

        return

    def _load_defaults(self) -> None:
        logger.info("Getting defaults for scenario {}", self.name)
        self.defaults = get_defaults(self.input_model, self.output_model)
        if not self.plugins:
            logger.debug("Using default plugins.")
            setattr(self, "plugins", self.defaults.get("default_plugins"))
        else:
            logger.debug("Using the following plugins: {}", self.plugins)

    @classmethod
    def from_kwargs(cls, **kwargs) -> "Scenario":
        """Create Scenario instance from key arguments."""
        cls_fields = {field for field in inspect.signature(cls).parameters}

        native_args, new_args = {}, {}
        for name, val in kwargs.items():
            if name in cls_fields:
                native_args[name] = val
            else:
                new_args[name] = val

        ret = cls(**native_args)

        for new_name, new_val in new_args.items():
            setattr(ret, new_name, new_val)
        return ret


@dataclass
class Configuration:
    """R2X configuration manager that wraps multiple Scenario instances.

    This class parses either the cases_*.csv file or reads the inputs from the CLI.

    Attributes
    ----------
    scenario_list
        List of Scenario instances
    scenario_names
        Names of the scenarios in `scenario_list`

    See Also
    --------
    Scenario
    """

    scenarios: dict = field(default_factory=dict)
    scenario_names: list = field(default_factory=list)

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

    def get(self, name: str) -> "Configuration":
        """Return a scenario from the configuration manager.

        Parameters
        ----------
        name
            Name of the scenario to get

        Returns
        -------
            Configuration

        Raises
        ------
        KeyError
            If name is not present on scenario_names
        """
        if name not in self.scenario_names:
            msg = f"{name=} not found in scenarios. List of available scenarios {self.scenario_names}"
            raise KeyError(msg)

        return self.scenarios[name]

    @staticmethod
    def override(params: dict, cli_args: dict, user_dict: dict | None = None) -> Scenario:
        """Override scenario either by user provided dict or cli.

        If the user want to override defaults it can be done by passing the
        `user_dict`. Currently, we only use the `user_dict` to override the defaults and the fmap.

        Parameters
        ----------
        params
            Parameters to create the scenario
        user_dict
            Dictionary that will override the defaults
        cli_args
            Arguments passed from the CLI
        """
        if user_dict is None:
            user_dict = {}
        if not any(key in params for key in cli_args) and not user_dict:
            return Scenario.from_kwargs(**params)

        # Order is CLI -> user_dict -> Default params
        logger.debug("Overriding scenario with {}", user_dict)
        override_dict = user_dict.copy()
        fmap_dict = override_dict.pop("fmap", {})
        mapping = ChainMap(cli_args, override_dict, params)
        config = Scenario.from_kwargs(**mapping)

        config.fmap = update_dict(base_dict=config.fmap, override_dict=ChainMap(cli_args, fmap_dict))
        config.defaults = update_dict(
            base_dict=config.defaults,
            override_dict=ChainMap(cli_args, override_dict),
        )
        return config

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
        scenario = Scenario.from_kwargs(**cli_args)
        if user_dict:
            scenario = instance.override(scenario.__dict__, cli_args=cli_args, user_dict=user_dict)
        instance.scenario_names.append(scenario.name)
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

        for scenario in user_dict["scenarios"]:
            scenario = ChainMap(cli_args, global_keys, scenario)
            scenario_class = Scenario.from_kwargs(**scenario)
            instance.scenario_names.append(scenario_class.name)
            instance.scenarios[scenario_class.name] = scenario_class
        return instance


def get_config(cli_args, user_dict):
    """Create configuration class using CLI arguments."""
    if solve_year := cli_args.get("solve_year"):
        cli_args["solve_year"] = solve_year[0] if len(solve_year) == 1 else solve_year

    if "scenarios" in user_dict:
        config_mgr = Configuration.from_scenarios(cli_args, user_dict)
    else:
        config_mgr = Configuration.from_cli(cli_args, user_dict=user_dict)
    assert config_mgr

    if scenario_name := cli_args.get("scenario_name", None):
        config_mgr = config_mgr.get(scenario_name)
    return config_mgr
