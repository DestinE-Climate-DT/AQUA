""" The tropical rainfall module"""

from .src.tropical_rainfall_tools import ToolsClass
from .src.tropical_rainfall_plots import PlottingClass
from .src.tropical_rainfall_main import MainClass
from .tropical_rainfall_class import Tropical_Rainfall

__version__ = '0.0.1'

__all__ = ['Tropical_Rainfall', 'PlottingClass', 'ToolsClass', 'MainClass']

# Change log
# 0.0.1: Initial version