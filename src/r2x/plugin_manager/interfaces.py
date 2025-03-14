from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Type, Callable, Any, Union, TYPE_CHECKING
import json
from pathlib import Path
from importlib import resources
#from polars import DataFrame

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
    def from_path(cls, path: str, module=None) -> "DefaultFile":
        """
        Create a DefaultFile from a path string, resolving it to an absolute path.
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
                raise ValueError(f"Could not resolve path {path} for module {package_name}: {str(e)}")

        return cls(name=path_obj.name, path=absolute_path)

    def read(cls) -> Dict[str, Any]:
        with open(cls.path, "r") as f:
            return json.load(f)

@dataclass
class PluginComponent:

    """Components associated with a model."""
    config: Type[BaseModelConfig]  # Required
    parser: Optional[Type[BaseParser]] = None
    parser_defaults: List[DefaultFile] = field(default_factory=list)

    # TODO this could also be a list of string. If it is a string, we should check if that function
    # is avaialable first.
    parser_filters: Optional[
        Union[
            List[str],
            List[Callable[[DataFrame, dict], DataFrame]]
        ]
    ] = None
    exporter: Optional[Type[BaseExporter]] = None
    export_defaults: List[DefaultFile] = field(default_factory=list)
    fmap: Optional[DefaultFile] = None
