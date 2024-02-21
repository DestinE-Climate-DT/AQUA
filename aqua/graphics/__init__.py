"""Graphics package for Aqua."""
from .single_map import plot_single_map
from .timeseries import plot_timeseries, plot_seasonalcycle

__all__ = ["plot_single_map",
           "plot_timeseries", "plot_seasonalcycle"]
