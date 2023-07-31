"""AQUA module"""
from .docker import rundiag
from .gribber import Gribber
from .lra_generator import LRAgenerator, OPAgenerator
from .reader import Reader, catalogue, Streaming, inspect_catalogue
from .slurm import squeue, job, output_dir, scancel, max_resources_per_node

__version__ = '0.2'

__all__ = ["rundiag", "Reader", "catalogue", "Streaming", "inspect_catalogue",
           "LRAgenerator", "Gribber", "OPAgenerator", "squeue", "job",
           "output_dir", "scancel", "max_resources_per_node"]
