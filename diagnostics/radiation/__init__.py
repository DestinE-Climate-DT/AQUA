"""radiation module"""

from .radiation_functions import process_ceres_data, process_model_data
from .radiation_functions import gregory_plot, barplot_model_data, plot_model_comparison_timeseries, plot_bias, plot_maps, plot_mean_bias

# Optional but recommended
__version__ = '0.0.1'

# This specifies which methods are exported publicly, used by "from radiation import *"
__all__ = ["process_ceres_data", "process_model_data", "gregory_plot", 
           "barplot_model_data", "plot_model_comparison_timeseries", "plot_bias", "plot_maps", "plot_mean_bias"]