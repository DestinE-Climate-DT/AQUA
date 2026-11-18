# aqua/core/components.py
"""Discovery of installed aqua.* components: core plus the plugins of the ``aqua.plugins`` entry-point group.

A plugin is a project (the name "project" is provisional): its entry-point name is the project name, and
its ``get_install_dirs()`` returns a dictionary following the plugin contract version 2::

    {
        "contract": 2,
        "config": ["analysis", "collections", "tools"],  # project-scoped: ~/.aqua/<dir>/<project>
        "flat": ["definitions"],  # whole folders: ~/.aqua/<dir>
        "templates": ["collections"],  # whole folders: ~/.aqua/templates/<dir>
        "files": ["grids"],  # single files merged into the flat ~/.aqua/<dir>
    }

Folder names are relative to ``aqua/<project>/config`` (``aqua/<project>/templates`` for ``templates``).
Missing list keys default to empty lists. Core is not a plugin: its folders are the constants below.
"""

import importlib.resources as pypath
import logging
import os
import warnings
from functools import cache
from importlib import import_module
from importlib.metadata import entry_points

# directories of aqua-core installed in the AQUA config folder: core is not a project, so all are flat
CORE_FLAT_DIRECTORIES = ["catgen", "data_model", "fixes", "grids", "styles"]
CORE_TEMPLATE_DIRECTORIES = ["catgen", "drop", "gridbuilder"]

# plugin contract implemented by this aqua-core
PLUGIN_CONTRACT = 2
# keys of the dictionary returned by get_install_dirs() and the info keys they are stored in
PLUGIN_DIR_KEYS = {"config": "config_dirs", "flat": "flat_dirs", "templates": "template_dirs", "files": "file_dirs"}

# module-level logger, independent of any CLI's configured loglevel.
# Callers that care about their own loglevel should re-log the (cached,
# essentially free) result themselves - see AquaConsole.__init__ below.
_logger = logging.getLogger("aqua.components")


class PluginContractError(ValueError):
    """The dictionary returned by a plugin's get_install_dirs() does not follow the plugin contract."""


def _empty_info(name, error=None):
    """Info dictionary of a component that cannot be installed."""
    info = {"installed": False, "project": name != "core", "path": None, "error": error}
    info.update(dict.fromkeys(PLUGIN_DIR_KEYS.values(), []))
    return info


def validate_install_dirs(name, data):
    """Check the dictionary returned by the ``get_install_dirs()`` of plugin ``aqua-<name>``.

    Args:
        name (str): plugin (project) name.
        data (dict): the dictionary returned by ``get_install_dirs()``.

    Returns:
        dict: the four lists of folders, under the info keys ``config_dirs``, ``flat_dirs``,
        ``template_dirs`` and ``file_dirs``; missing keys are empty lists.

    Raises:
        PluginContractError: if the plugin uses an old contract, an unknown key or a value that is not a list.
    """
    if not isinstance(data, dict) or data.get("contract") != PLUGIN_CONTRACT:
        raise PluginContractError(
            f"plugin aqua-{name} uses an old plugin contract, please update it (e.g. aqua-diagnostics>=0.27.0)"
        )
    unknown = sorted(set(data) - set(PLUGIN_DIR_KEYS) - {"contract"})
    if unknown:
        raise PluginContractError(
            f"plugin aqua-{name}: unknown key(s) {', '.join(unknown)} in get_install_dirs(), "
            f"allowed keys are contract, {', '.join(PLUGIN_DIR_KEYS)}"
        )
    dirs = {}
    for key, info_key in PLUGIN_DIR_KEYS.items():
        value = data.get(key, [])
        if not isinstance(value, list):
            raise PluginContractError(f"plugin aqua-{name}: '{key}' in get_install_dirs() must be a list, not {value!r}")
        dirs[info_key] = value
    return dirs


def _resolve_component_info(name, data=None):
    """
    Build a uniform info dict for an aqua component (core or plugin).

    Args:
        name (str): component name, e.g. "core", "diagnostics", "claima"
        data (dict, optional): the dictionary returned by the plugin's get_install_dirs(),
            already obtained from an entry point call. Not used for "core", which is resolved
            from its constants.

    Returns:
        dict: {
            "installed": bool,       # the package and its config folder are available
            "project": bool,         # False for core only
            "config_dirs": list,     # project-scoped folders
            "flat_dirs": list,
            "template_dirs": list,
            "file_dirs": list,
            "path": str or None,     # the package config folder
            "error": str or None,    # why the plugin cannot be installed (e.g. old contract)
        }
    """
    module_name = f"aqua.{name}"

    if name == "core":
        dirs = {
            "config_dirs": [],
            "flat_dirs": CORE_FLAT_DIRECTORIES,
            "template_dirs": CORE_TEMPLATE_DIRECTORIES,
            "file_dirs": [],
        }
    else:
        if data is None:
            try:
                data = import_module(module_name).get_install_dirs()
            except (ImportError, AttributeError) as e:
                warnings.warn(f"Could not resolve aqua component '{name}': {e}")
                return _empty_info(name)
        try:
            dirs = validate_install_dirs(name, data)
        except PluginContractError as e:
            return _empty_info(name, error=str(e))

    path = None
    if any(dirs.values()):
        try:
            path = os.path.join(pypath.files(module_name), "config")
        except ModuleNotFoundError:
            return _empty_info(name)

    return {"installed": path is not None, "project": name != "core", "path": path, "error": None, **dirs}


@cache
def discover_aqua_components():
    """
    All installed aqua components: core (resolved directly, not a plugin)
    plus any aqua.plugins entry points.

    A plugin whose module cannot be imported is reported as not installed, with a warning.
    A plugin whose get_install_dirs() fails or breaks the contract is reported as not installed
    with its ``error``: installing it stops with that error.

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
            get_install_dirs = ep.load()
        except Exception as e:
            warnings.warn(f"Could not load aqua plugin '{ep.name}': {e}")
            components[ep.name] = _empty_info(ep.name)
            continue
        try:
            data = get_install_dirs()
        except Exception as e:
            components[ep.name] = _empty_info(ep.name, error=f"plugin aqua-{ep.name}: {e}")
            continue
        components[ep.name] = _resolve_component_info(ep.name, data)

    _logger.debug(
        "Discovered aqua components: %s",
        [c for c in components if components[c]["installed"]],
    )
    return components


def component_version(name):
    """Version of the package ``aqua.<name>``, or None if it cannot be imported or has no ``__version__``."""
    try:
        return getattr(import_module(f"aqua.{name}"), "__version__", None)
    except ImportError:
        return None
