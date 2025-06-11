"""AQUA module"""
from .version import __version__
from .graphics import plot_single_map, plot_maps, plot_single_map_diff, plot_timeseries
from .graphics import plot_hovmoller
from .graphics import plot_lat_lon_profiles, plot_lines
from .lra_generator import LRAgenerator
from .reader import Reader, catalog, Streaming, inspect_catalog
from. regridder import Regridder
from .fldstat import FldStat
from .fixer import Fixer
from .accessor import AquaAccessor

__all__ = ["plot_single_map", "plot_maps", "plot_single_map_diff", "plot_timeseries",
           "plot_hovmoller",
           "plot_lat_lon_profiles", "plot_lines",
           "LRAgenerator",
           "Reader", "catalog", "Streaming", "inspect_catalog",
           "Regridder", "FldStat",
           "Fixer",
           "Regridder"]
