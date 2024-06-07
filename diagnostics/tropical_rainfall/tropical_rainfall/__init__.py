""" The tropical rainfall module"""

from .src.tropical_rainfall_tools import ToolsClass
from .src.tropical_rainfall_plots import PlottingClass
from .src.tropical_rainfall_data_manager import TropicalPrecipitationDataManager
from .src.tropical_rainfall_daily_variability import DailyVariabilityClass
from .src.tropical_rainfall_extra import ExtraFunctionalityClass
from .src.tropical_rainfall_histograms import HistogramClass
from .src.tropical_rainfall_meta import MetaClass
from .tropical_rainfall_class import Tropical_Rainfall

__version__ = '0.0.1'

__all__ = ['Tropical_Rainfall', 'PlottingClass', 'ToolsClass', 'TropicalPrecipitationDataManager',
           'DailyVariabilityClass', 'ExtraFunctionalityClass', 'HistogramClass', 'MetaClass']

# Change log
# 0.0.1: Initial version