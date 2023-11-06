"""teleconnections module"""
from .index import station_based_index, regional_mean_index
from .index import regional_mean_anomalies
from .mjo import mjo_hovmoller
from .plots import index_plot, maps_plot, hovmoller_plot
from .plots import maps_diffs_plot, plot_single_map
from .statistics import reg_evaluation, cor_evaluation
from .tc_class import Teleconnection
from .tools import TeleconnectionsConfig
from .tools import area_selection, wgt_area_mean

__version__ = '0.2.2'

__all__ = ['station_based_index', 'regional_mean_index',
           'mjo_hovmoller',
           'regional_mean_anomalies',
           'index_plot', 'maps_plot', 'hovmoller_plot',
           'maps_diffs_plot', 'plot_single_map',
           'reg_evaluation', 'cor_evaluation',
           'Teleconnection', 'TeleconnectionsConfig',
           'area_selection', 'wgt_area_mean']


# Changelog
# 0.2.2: Added season selection to index_plot
# 0.2.1: Performance improvements
#        Refactory of single map plot function
#        CLI refinement and code linting
# 0.2.0: CLI working with the AQUA wrapper
# 0.1.2: symmetric colorbar on/off for maps_plot
# 0.1.1: bugfix of single_maps, ENSO available with 2t and skt
# 0.1.0: release for version v0.2 of aqua
# 0.0.9: maps_diff_plot and add_cyclic_lon added to plots module
# 0.0.8: regression and correlation refactored
#        Added the possibility to evaluate them with a different variable
#        from the variable used to evaluate the index
# 0.0.7: CLI refined, get_dataset_config moved to tools, tools refactor
# 0.0.6: plots submodules added
# 0.0.5: comparison_index_plot, cli for single and multiple datasets added
# 0.0.4: Added cor_evaluation and reg_evaluation based on sacpy,
#        removed deprecated functions
# 0.0.3: Class Teleconnection added
# 0.0.2: Added package version
# 0.0.1: Initial version
