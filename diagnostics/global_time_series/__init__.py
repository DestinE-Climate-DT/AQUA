from .gregory import GregoryPlot
from .timeseries import Timeseries

__version__ = "0.1.1"

__all__ = ["GregoryPlot",
           "Timeseries"]

# Changelog:

# 0.1.1: Gregory plot as a class
# 0.1.0: complete refactory of the timeseries as a class
# 0.0.5: support for reference data in Gregory plot
# 0.0.4: gregory plot in a separate file
# 0.0.3: added improvement gregory plot
#        obs dataset errorbar
# 0.0.2: added CLI for workflow
# 0.0.1: initial release
