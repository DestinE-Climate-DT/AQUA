# aqua/core/components.py
"""Discovery of installed aqua.* components (core + plugins)."""

import importlib.resources as pypath
import logging
import os
import warnings
from functools import cache
from importlib import import_module
from importlib.metadata import entry_points

# directories to be installed in the AQUA config folder
CORE_CONFIG_DIRECTORIES = ["catgen", "data_model", "fixes", "grids", "styles"]
CORE_TEMPLATE_DIRECTORIES = ["catgen", "drop", "gridbuilder"]

# module-level logger, independent of any CLI's configured loglevel.
# Callers that care about their own loglevel should re-log the (cached,
# essentially free) result themselves - see AquaConsole.__init__ below.
_logger = logging.getLogger("aqua.components")


def _resolve_component_info(name, data=None):
    """
    Build a uniform info dict for an aqua component (core or plugin).

    Args:
        name (str): component name, e.g. "core", "diagnostics", "emulators"
        data (dict, optional): pre-loaded {"config": [...], "templates": [...]}
            already obtained from an entry point call. Used for plugins.
            Not used for "core", which is resolved directly below.

    Returns:
        dict: {
            "installed": bool,
            "config_dirs": list,
            "template_dirs": list,
            "path": str or None,
        }
    """
    module_name = f"aqua.{name}"
    empty = {"installed": False, "config_dirs": [], "template_dirs": [], "path": None}

    if name == "core":
        # core is not a plugin: no get_install_dirs() contract, just read
        # its known constants directly.
        config_dirs = CORE_CONFIG_DIRECTORIES
        template_dirs = CORE_TEMPLATE_DIRECTORIES
    elif data is not None:
        config_dirs = data.get("config", [])
        template_dirs = data.get("templates", [])
    else:
        try:
            module = import_module(module_name)
            data = module.get_install_dirs()
            config_dirs = data.get("config", [])
            template_dirs = data.get("templates", [])
        except (ImportError, AttributeError) as e:
            warnings.warn(f"Could not resolve aqua component '{name}': {e}")
            return empty

    path = None
    if config_dirs and template_dirs:
        try:
            path = os.path.join(pypath.files(module_name), "config")
        except ModuleNotFoundError:
            return empty

    return {
        "installed": path is not None,
        "config_dirs": config_dirs,
        "template_dirs": template_dirs,
        "path": path,
    }


@cache
def discover_aqua_components():
    """
    All installed aqua components: core (resolved directly, not a plugin)
    plus any aqua.plugins entry points.

    Cached: the entry-point scan and imports only run once per process,
    regardless of how many callers (parser construction, CLI __init__,
    tests, ...) invoke this.
    """
    components = {"core": _resolve_component_info("core")}

    for ep in entry_points(group="aqua.plugins"):
        if ep.name == "core":
            warnings.warn("Ignoring unexpected 'core' entry point in aqua.plugins group")
            continue
        try:
            data = ep.load()()
        except Exception as e:
            warnings.warn(f"Could not load aqua plugin '{ep.name}': {e}")
            components[ep.name] = {"installed": False, "config_dirs": [], "template_dirs": [], "path": None}
            continue
        components[ep.name] = _resolve_component_info(ep.name, data)

    _logger.debug(
        "Discovered aqua components: %s",
        [c for c in components if components[c]["installed"]],
    )
    return components
