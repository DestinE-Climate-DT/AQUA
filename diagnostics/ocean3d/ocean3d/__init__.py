"""Ocean3D module"""

from .ocean_util import * #check_variable_name, time_slicing
from .ocean_drifts import hovmoller_plot , multilevel_t_s_trend_plot, zonal_mean_trend_plot
from .ocean_drifts import time_series
from .ocean_circulation.ocean_circulation import plot_stratification, plot_spatial_mld_clim
# Optional but recommended
__version__ = '0.0.3'

# This specifies which methods are exported publicly"
__all__ = ["hovmoller_plot",
           "multilevel_t_s_trend_plot",
           "zonal_mean_trend_plot",
           "plot_spatial_mld_clim",
           "plot_stratification"]

# CHANGELOG
# 0.0.2: Added CLI and refinement
# 0.0.1: Initial commit
