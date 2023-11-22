"""Atmospheric global mean diagnostic module"""
from .atm_global_mean import seasonal_bias, compare_datasets_plev, plot_map_with_stats

__version__ = '0.0.1'

__all__ = ['seasonal_bias', 'compare_datasets_plev', 'plot_map_with_stats']
