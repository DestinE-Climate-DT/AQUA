"""tropical_cyclones module"""

# The following lines are needed so that the tropical cyclones class constructor
# and associated functions are available directly from the module "tropical_cyclones"

from .tropical_cyclones import TCs
from .detect_nodes import DetectNodes
from .stitch_nodes import StitchNodes


# Optional but recommended
__version__ = '0.2.0'

# This specifies which methods are exported publicly, used by "from tropical cyclones import *"
__all__ = ["TCs", "DetectNodes", "StitchNodes"]