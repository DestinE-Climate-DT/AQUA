"""radiation module"""

from .radiation_functions import process_ceres_data, process_model_data
from .radiation_functions import boxplot_model_data
from .radiation_functions import plot_model_comparison_timeseries, plot_mean_bias

# Optional but recommended
__version__ = '0.0.3'

# This specifies which methods are exported publicly, used by "from radiation import *"
__all__ = ["process_ceres_data", "process_model_data", "boxplot_model_data",
           "plot_model_comparison_timeseries", "plot_mean_bias"]

# Changelog
# 0.0.3: Improved functions, bugfix in timeseries, removed Gregory Plot
# 0.0.2: Adapted to the new data governance
# 0.0.1: Initial version
