"""Ocean3D module"""

from .ocean_util import *
from .ocean_circulation import *
from .ocean_drifts import *

# Optional but recommended
__version__ = '0.0.1'

# This specifies which methods are exported publicly, used by "from dummy import *"
__all__ = ["hovmoller_lev_time_plot",
           "time_series_multilevs",
           "multilevel_t_s_trend_plot",
           "zonal_mean_trend_plot",
           "plot_spatial_mld_clim",
           "plot_stratification"]
