"""Ocean3D module"""

from .ocean_util import * #check_variable_name, time_slicing
from .ocean_drifts import hovmoller_plot
from .ocean_drifts import multilevel_trend
from .ocean_drifts import zonal_mean_trend
from .ocean_drifts import time_series
# from .ocean_circulation. import 
from .ocean_circulation import stratification
from .ocean_circulation import plot_spatial_mld_clim
# Optional but recommended
__version__ = '0.0.5'

# This specifies which methods are exported publicly"
__all__ = ["hovmoller_plot",
            "time_series",
           "multilevel_trend",
           "zonal_mean_trend",
           "plot_spatial_mld_clim",
           "plot_stratification"]

# CHANGELOG
# 0.0.2: Added CLI and refinement
# 0.0.1: Initial commit
