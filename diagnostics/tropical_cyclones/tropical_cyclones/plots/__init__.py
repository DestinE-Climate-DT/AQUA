"""tropical_cyclones plotting module"""

from .plotting_hist import plot_hist_cat, plot_press_wind
from .plotting_TCs import multi_plot, plot_trajectories

# This specifies which methods are exported publicly, used by "from tropical cyclones import *"
__all__ = ["multi_plot", "plot_trajectories", "plot_hist_cat", "plot_pres_wind"]
