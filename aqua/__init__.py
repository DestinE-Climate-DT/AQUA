"""AQUA module"""
from .docker import rundiag
from .reader import Reader, catalogue, Streaming, inspect_catalogue

__version__ = '0.0.1'

__all__ = ["rundiag", "Reader", "catalogue", "Streaming", "inspect_catalogue"]
