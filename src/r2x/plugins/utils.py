"""Helper function to enable plugins/extension on R2X."""

from importlib.util import find_spec, module_from_spec

from loguru import logger

# Module level imports
from ..utils import DEFAULT_PLUGIN_PATH


def valid_plugin_list(plugin_list: list[str], folder: str = DEFAULT_PLUGIN_PATH, logger=logger) -> list[str]:
    """Return valid plugins for the R2X process.

    Args:
        plugin_list: list of plugins to include.
        folder: Folder that contains the plugin.
        logger: Logger object.

    Returns
    -------
        List[str] of valid plugins.

    Note(s):
        - Currently only works if the folder is located inside of the R2X
          project folder.
    """
    # Initialize list of valid plugins.
    valid_plugins = []
    for plugin in plugin_list:
        if validate_plugin(plugin, folder, logger):
            valid_plugins.append(plugin)
    if len(valid_plugins) != plugin_list:
        logger.debug(f"Updated list {valid_plugins=}")
    return valid_plugins


def validate_plugin(
    plugin: str,
    folder: str,
    logger=logger,
    valid_fn_names: list[str] = ["cli_arguments", "translator", "exporter"],
) -> bool:
    """Check if the plugin is callable and has the default function names.

    Args:
        plugin: Name of the plugin.
        folder: Folder that contains the plugin.
        logger: Logger object.
        valid_fn_names: Function names to look for function

    Returns
    -------
        True or False of the plugin is found and has any callable function names.
    """
    # Return if the plugin is not found.
    if not (spec := find_spec(f"{folder}.{plugin}")):
        logger.warning(f"Plugin {plugin} specified does not exists. Skipping it.")
        return False

    # Load plugin.
    module = module_from_spec(spec)
    if spec.loader is None:
        return False
    spec.loader.exec_module(module)

    valid_functions = [hasattr(module, fn) for fn in valid_fn_names]
    if any(valid_functions):
        return True
    else:
        logger.warning(f"{plugin} does not have any valid function names. Skipping it")
        return False
