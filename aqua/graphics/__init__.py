"""
Graphics module for Aqua.

This module contains the following files:
- single_map: For plotting a single map.
- timeseres: For timeseries ans seasonal cycle plots.
"""
from .single_map import plot_single_map, plot_single_map_diff
from .timeseries import plot_timeseries, plot_seasonalcycle

__all__ = ["plot_single_map", "plot_single_map_diff",
           "plot_timeseries", "plot_seasonalcycle"]
