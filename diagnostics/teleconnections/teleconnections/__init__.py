"""teleconnections module"""
from .cdo_testing import station_based_cdo, regional_mean_cdo, regional_anomalies_cdo
from .index import station_based_index, regional_mean_index, regional_mean_anomalies
from .plots import simple_plot, index_plot
from .statistics import reg_evaluation, cor_evaluation
from .tc_class import Teleconnection
from .tools import load_namelist, lon_180_to_360

__version__ = '0.0.3'

__all__ = ['station_based_cdo', 'regional_mean_cdo', 'regional_anomalies_cdo',
           'station_based_index', 'regional_mean_index', 'regional_mean_anomalies',
           'simple_plot', 'index_plot',
           'reg_evaluation', 'cor_evaluation',
           'Teleconnection', 'load_namelist', 'lon_180_to_360']

# Change log
# 0.0.4: Added cor_evaluation and reg_evaluation based on sacpy, removed deprecated functions
# 0.0.3: Class Teleconnection added
# 0.0.2: Added package version
# 0.0.1: Initial version
