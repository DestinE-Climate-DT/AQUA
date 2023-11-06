"""tropical_cyclones plotting module"""

from .plotting_TCs import multi_plot, plot_trajectories

# This specifies which methods are exported publicly, used by "from tropical cyclones import *"
__all__ = ["multi_plot", "plot_trajectories"]
