from .seaice_class import SeaIceExtent, SeaIceVolume, SeaIceConcentration, SeaIceThickness

# Optional but recommended
__version__ = '0.0.2'

# This specifies which methods are exported publicly, used by "from dummy import *"
__all__ = ["SeaIceExtent", "SeaIceVolume", "SeaIceConcentration", "SeaIceThickness"]

# Changelog
# 0.0.2: SeaIceExtent class now has seasonal cycle
# 0.0.1: Initial release
