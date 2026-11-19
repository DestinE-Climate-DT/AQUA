# aqua/core/components.py
"""Discovery of installed aqua.* components: core plus the plugins of the ``aqua.plugins`` entry-point group.

A plugin is a project: its entry-point name is the project name, and
its ``get_install_dirs()`` returns a dictionary following the plugin contract version 2::

    {
        "contract": 2,
        "config": ["analysis", "collections", "tools"],  # project-scoped: ~/.aqua/<dir>/<project>
        "flat": ["definitions"],  # whole folders: ~/.aqua/<dir>
        "templates": ["collections"],  # whole folders: ~/.aqua/templates/<dir>
        "files": ["grids"],  # single files merged into the flat ~/.aqua/<dir>
    }

Folder names are relative to ``aqua/<project>/config`` (``aqua/<project>/templates`` for ``templates``).
Missing keys default to empty lists. Core is not a plugin, but its folders follow the same contract.
"""

import importlib.resources as pypath
import logging
import os
import warnings
from functools import cache
from importlib import import_module
from importlib.metadata import entry_points

from aqua.core.util import to_list

# plugin contract implemented by this aqua-core, and the folder lists it defines
PLUGIN_CONTRACT = 2
PLUGIN_KEYS = ("config", "flat", "templates", "files")

# folders of aqua-core installed in the AQUA config folder: core is not a project, so all are flat
CORE_INSTALL_DIRS = {
    "contract": PLUGIN_CONTRACT,
    "flat": ["catgen", "data_model", "fixes", "grids", "styles"],
    "templates": ["catgen", "drop", "gridbuilder"],
}

# module-level logger, independent of any CLI's configured loglevel.
# Callers that care about their own loglevel should re-log the (cached,
# essentially free) result themselves - see AquaConsole.__init__ below.
_logger = logging.getLogger("aqua.components")


def validate_install_dirs(name, data):
    """Check the dictionary returned by ``get_install_dirs()`` of ``aqua-<name>`` and return its folder lists.

    Args:
        name (str): component name.
        data (dict): the dictionary returned by ``get_install_dirs()``.

    Returns:
        dict: one list per key of ``PLUGIN_KEYS``, empty if missing.

    Raises:
        ValueError: if the plugin uses an old contract or an unknown key.
    """
    if not isinstance(data, dict) or data.get("contract") != PLUGIN_CONTRACT:
        raise ValueError(f"plugin aqua-{name} uses an old plugin contract, please update it (e.g. aqua-diagnostics>=0.27.0)")
    unknown = set(data) - set(PLUGIN_KEYS) - {"contract"}
    if unknown:
        raise ValueError(f"plugin aqua-{name}: unknown key(s) {', '.join(sorted(unknown))} in get_install_dirs()")
    return {key: to_list(data.get(key)) for key in PLUGIN_KEYS}


def _resolve_component_info(name, data):
    """
    Build a uniform info dict for an aqua component (core or plugin).

    Args:
        name (str): component name, e.g. "core", "diagnostics", "claima"
        data (dict or None): the dictionary returned by get_install_dirs(); None if it could not be obtained

    Returns:
        dict: {"installed": bool, "path": str or None, "error": str or None, plus one list per key of PLUGIN_KEYS},
        where ``path`` is the package config folder and ``error`` why the plugin cannot be installed.
    """
    info = {"installed": False, "path": None, "error": None, **dict.fromkeys(PLUGIN_KEYS, [])}
    if data is None:
        return info
    try:
        info.update(validate_install_dirs(name, data))
        info["path"] = os.path.join(pypath.files(f"aqua.{name}"), "config")
    except ValueError as e:
        info["error"] = str(e)
    except ModuleNotFoundError:
        pass
    info["installed"] = info["path"] is not None
    return info


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
    components = {"core": _resolve_component_info("core", CORE_INSTALL_DIRS)}

    for ep in entry_points(group="aqua.plugins"):
        try:
            get_install_dirs = ep.load()
        except Exception as e:
            warnings.warn(f"Could not load aqua plugin '{ep.name}': {e}")
            components[ep.name] = _resolve_component_info(ep.name, None)
            continue
        try:
            components[ep.name] = _resolve_component_info(ep.name, get_install_dirs())
        except Exception as e:
            components[ep.name] = _resolve_component_info(ep.name, None)
            components[ep.name]["error"] = f"plugin aqua-{ep.name}: {e}"

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
