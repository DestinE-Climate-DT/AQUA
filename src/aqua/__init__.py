"""AQUA module"""
from .graphics import plot_single_map, plot_maps, plot_single_map_diff, plot_timeseries
from .graphics import plot_hovmoller
from .lra_generator import LRAgenerator
from .reader import Reader, Streaming, inspect_catalog, aqua_catalog
from .slurm import squeue, job, output_dir, scancel, max_resources_per_node
from .accessor import AquaAccessor

__version__ = '0.12.2'

__all__ = ["plot_single_map", "plot_maps", "plot_single_map_diff", "plot_timeseries",
           "plot_hovmoller",
           "LRAgenerator",
           "Reader", "Streaming", "inspect_catalog", "aqua_catalog",
           "squeue", "job", "output_dir", "scancel", "max_resources_per_node"]
