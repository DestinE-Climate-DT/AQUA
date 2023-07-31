"""tropical_cyclones module"""

# The following lines are needed so that the tropical cyclones class constructor
# and associated functions are available directly from the module "tropical_cyclones"


from .tropical_cyclones import TCs
from .detect_nodes import DetectNodes
from .stitch_nodes import StitchNodes
from .tempest_utils import getTrajectories, getNodes
from .tcs_utils import clean_files, lonlatbox, write_fullres_field
from .plotting_TCs import multi_plot, plot_trajectories

# Optional but recommended
__version__ = '0.0.1'

# This specifies which methods are exported publicly, used by "from dummy import *"
__all__ = ["TCs", "DetectNodes", "StitchNodes", "getTrajectories", "getNodes", 
           "lonlatbox", "write_fullres_field", "multi_plot", "plot_trajectories"]

