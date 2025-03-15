"""Interface definitions for the plugin manager.

DefaultFile: Represents a default configuration file path to a json file
that is tied to a specific plugin. The plugin manager can load a
DefaultFile whether it is an internal or external plugin.

PluginComponent: Represents the various subcomponents that make up a plugin.
This includes one or more of the following:
    parsers, exporters, configuration models, filter functions and default files.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING
import json
from pathlib import Path
from importlib import resources
from collections.abc import Callable

if TYPE_CHECKING:
    from polars import DataFrame
    from r2x.parser.handler import BaseParser
    from r2x.exporter.handler import BaseExporter
    from r2x.config_models import BaseModelConfig



@dataclass
class DefaultFile:
    """Represents a default configuration file."""

    name: str
    path: Path

    def __post_init__(self):
        if not self.path.is_absolute():
            raise ValueError("Path must be absolute after initialization.")

    @classmethod
    def from_path(cls, path: str, module=None) -> DefaultFile:
        """
        Create a DefaultFile from a path string, resolving it to an absolute path.

        Parameters
        ----------
            path (str): The path to the file.
            module (str or module): The module to resolve the path relative to.

        Returns
        -------
            DefaultFile: The created DefaultFile instance.
        """
        path_obj = Path(path)
        if path_obj.is_absolute():
            return cls(name=path_obj.name, path=path_obj)

        # Default to the r2x module if no module is provided
        if module is None:
            module = "r2x"

        package_name = module if isinstance(module, str) else module.__name__.split(".")[0]
        try:
            base_path = resources.files(package_name)
            absolute_path = base_path.joinpath(path_obj).resolve()
            if not absolute_path.is_file():

                raise FileNotFoundError(f"Resolved path {absolute_path} does not exist.")
        except (ModuleNotFoundError, TypeError, FileNotFoundError) as e:
            if hasattr(module, "__file__"):
                base_dir = Path(module.__file__).parent
                absolute_path = (base_dir / path_obj).resolve()
                if not absolute_path.is_file():
                    raise FileNotFoundError(f"Resolved path {absolute_path} does not exist.")
            else:
                raise ValueError(f"Could not resolve path {path} for module {package_name}: {e!s}")

        return cls(name=path_obj.name, path=absolute_path)

    def read(self) -> dict[str, Any]:
        """
        Read the underlying JSON file of the Type DefaultFile.

        Returns
        -------
            dict[str, Any]: The parsed JSON data.
        """
        with open(self.path) as f:
            return json.load(f)

@dataclass
class PluginComponent:
    """Components associated with a model."""

    config: type[BaseModelConfig]  # Required
    parser: type[BaseParser] | None = None
    parser_defaults: list[DefaultFile] = field(default_factory=list)
    parser_filters: list[str] | list[Callable[[DataFrame, dict], DataFrame]] | None = None
    exporter: type[BaseExporter] | None = None
    export_defaults: list[DefaultFile] = field(default_factory=list)
    fmap: DefaultFile | None = None
