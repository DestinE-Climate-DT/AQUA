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
from .gregory import plot_gregory_monthly, plot_gregory_annual
from .histogram import plot_histogram
from .hovmoller import plot_hovmoller
from .boxplot import boxplot
from .index_plot import index_plot, indexes_plot
from .single_map import plot_single_map, plot_single_map_diff
from .styles import ConfigStyle
from .timeseries import plot_timeseries, plot_seasonalcycle
from .multiple_maps import plot_maps, plot_maps_diff
from .vertical_profiles import plot_vertical_profile, plot_vertical_profile_diff
from .lat_lon_profiles import plot_lat_lon_profiles
from .seasonal_means import plot_seasonal_lat_lon_profiles
from .vertical_line import plot_vertical_lines


__all__ = ["plot_gregory_monthly", "plot_gregory_annual",
           "plot_histogram",
           "plot_hovmoller", "boxplot",
           "index_plot", "indexes_plot",
           "plot_single_map", "plot_single_map_diff",
           "ConfigStyle",
           "plot_timeseries", "plot_seasonalcycle",
           "plot_maps", "plot_maps_diff",
           "plot_vertical_profile", "plot_vertical_profile_diff",
           "plot_lat_lon_profiles",
           "plot_seasonal_lat_lon_profiles",
           "plot_vertical_lines"]


def activate_style(style: str = None,
                   loglevel: str = 'WARNING'):
    """Activate the style for graphical utilities
    Args:
        style (str): name of the style to load.
                     If not provided, it will be read from the configuration file.
        loglevel (str): logging level. Default is 'WARNING'.
    """
    ConfigStyle(style=style, loglevel=loglevel)
