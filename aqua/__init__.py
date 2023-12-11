"""AQUA module"""
from .docker import rundiag
from .gribber import Gribber
from .lra_generator import LRAgenerator
from .reader import Reader, catalogue, Streaming, inspect_catalogue
from .slurm import squeue, job, output_dir, scancel, max_resources_per_node

__version__ = '0.5'

__all__ = ["rundiag", "Reader", "catalogue", "Streaming", "inspect_catalogue",
           "LRAgenerator", "Gribber", "squeue", "job",
           "output_dir", "scancel", "max_resources_per_node"]
