"""AQUA module"""
from .version import __version__
# Finish util/configurer initialization before loading defaults to avoid circular imports.
from . import util as _util
from .default import _load_defaults

_load_defaults()

from .graphics import plot_single_map, plot_maps, plot_single_map_diff, plot_timeseries
from .graphics import plot_hovmoller
from .graphics import plot_lat_lon_profiles, plot_seasonal_lat_lon_profiles
from .catgen import AquaFDBGenerator
from .drop import Drop
from .reader import Reader, show_catalog_content
from .regridder import Regridder
from .gridbuilder import GridBuilder
from .fldstat import FldStat
from .fixer import Fixer
from .accessor import AquaAccessor
from .histogram import histogram


__all__ = ["plot_single_map", "plot_maps", "plot_single_map_diff", "plot_timeseries",
           "plot_hovmoller", "histogram",
           "plot_lat_lon_profiles", "plot_seasonal_lat_lon_profiles",
           "AquaFDBGenerator",
           "Drop",
           "Reader", "show_catalog_content",
           "Regridder", "GridBuilder",
           "Fixer", "FldStat"]
