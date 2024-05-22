"""
Graphics module for Aqua.

This module contains the following files:
- single_map: For plotting a single map.
- multiple_maps: For plotting multiple maps.
- timeseres: For timeseries ans seasonal cycle plots.

The following functions are available:
- plot_single_map: Plot a single map.
- plot_single_map_diff: Plot a map as contour and
                        the difference with a reference as contourf
- plot_timeseries: Plot monthly and annual timeseries
- plot_seasonalcycle: Plot a seasonal cycle
- plot_maps: Plot multiple maps using plot_single_map
"""
from .hovmoller import plot_hovmoller
from .single_map import plot_single_map, plot_single_map_diff
from .timeseries import plot_timeseries, plot_seasonalcycle
from .multiple_maps import plot_maps

__all__ = ["plot_hovmoller",
           "plot_single_map", "plot_single_map_diff",
           "plot_timeseries", "plot_seasonalcycle",
           "plot_maps"]
