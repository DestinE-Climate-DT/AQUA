"""AQUA module"""
from .docker import rundiag
from .data_models import translate_coords
from .graphics import plot_single_map, plot_single_map_diff, plot_timeseries
from .gribber import Gribber
from .lra_generator import LRAgenerator
from .reader import Reader, catalogue, Streaming, inspect_catalogue
from .slurm import squeue, job, output_dir, scancel, max_resources_per_node
from .accessor import AquaAccessor

__version__ = '0.7'

__all__ = ["rundiag",
           "translate_coords",
           "plot_single_map", "plot_single_map_diff", "plot_timeseries"
           "Gribber",
           "LRAgenerator",
           "Reader", "catalogue", "Streaming", "inspect_catalogue",
           "squeue", "job", "output_dir", "scancel", "max_resources_per_node"]
