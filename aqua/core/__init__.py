"""AQUA module"""
from importlib import import_module

from .accessor import AquaAccessor  # noqa: F401  eager: registers the xarray .aqua accessor
from .version import __version__  # noqa: F401

# Public objects are imported on first access (PEP 562), to keep `import aqua` and the CLI fast
_LAZY_IMPORTS = {
    "plot_single_map": ".graphics", "plot_maps": ".graphics", "plot_single_map_diff": ".graphics",
    "plot_timeseries": ".graphics", "plot_hovmoller": ".graphics",
    "plot_lat_lon_profiles": ".graphics", "plot_seasonal_lat_lon_profiles": ".graphics",
    "AquaFDBGenerator": ".catgen",
    "Drop": ".drop",
    "Reader": ".reader", "show_catalog_content": ".reader",
    "Regridder": ".regridder",
    "GridBuilder": ".gridbuilder",
    "FldStat": ".fldstat",
    "Fixer": ".fixer",
    "histogram": ".histogram",
}


def resolve_lazy(name):
    """Import and return the public object `name` from its submodule."""
    return getattr(import_module(_LAZY_IMPORTS[name], __name__), name)


def __getattr__(name):
    if name in _LAZY_IMPORTS:
        value = resolve_lazy(name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(_LAZY_IMPORTS))


__all__ = ["plot_single_map", "plot_maps", "plot_single_map_diff", "plot_timeseries",
           "plot_hovmoller", "histogram",
           "plot_lat_lon_profiles", "plot_seasonal_lat_lon_profiles",
           "AquaFDBGenerator",
           "Drop",
           "Reader", "show_catalog_content",
           "Regridder", "GridBuilder",
           "Fixer", "FldStat"]
