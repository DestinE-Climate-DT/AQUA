"""AQUA module"""
from .docker import rundiag
from .graphics import plot_single_map, plot_single_map_diff
from .gribber import Gribber
from .lra_generator import LRAgenerator
from .reader import Reader, catalogue, Streaming, inspect_catalogue
from .slurm import squeue, job, output_dir, scancel, max_resources_per_node
from .accessor import AquaAccessor

__version__ = '0.6.3'

__all__ = ["rundiag",
           "plot_single_map", "plot_single_map_diff",
           "Gribber",
           "LRAgenerator",
           "Reader", "catalogue", "Streaming", "inspect_catalogue",
           "squeue", "job", "output_dir", "scancel", "max_resources_per_node"]
