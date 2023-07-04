"""Ocean Circulation module""" 


from .ocean_circulation_class import Global_OceanDiagnostic
from .ocean_circulation_func import *

# Optional but recommended
__version__ = '0.0.1'

# This specifies which methods are exported publicly, used by "from dummy import *"
__all__ = ["ocean_circulation_class", "ocean_circulation_func"]