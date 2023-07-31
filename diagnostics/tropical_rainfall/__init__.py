""" The tropical rainfall module"""

from .tropical_rainfall_class import Tropical_Rainfall
from .tropical_rainfall_func import time_interpreter, convert_24hour_to_12hour_clock, convert_monthnumber_to_str
from .tropical_rainfall_func import convert_length, convert_time, unit_splitter, extract_directory_path
from .tropical_rainfall_func import mirror_dummy_grid, data_size

__version__ = '0.0.1'

__all__ = ['Tropical_Rainfall', 'time_interpreter', 'convert_24hour_to_12hour_clock',   'convert_monthnumber_to_str',
           'convert_length',    'convert_time',     'unit_splitter',                    'extract_directory_path',
           'mirror_dummy_grid', 'data_size']

# Change log
# 0.0.1: Initial version