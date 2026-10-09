""" Test utilities for AQUA test suite """

from .cleanup import TestCleanupRegistry

DPI = 50
APPROX_REL = 1e-4
LOGLEVEL = "DEBUG"

__all__ = ["APPROX_REL", "DPI", "LOGLEVEL", "TestCleanupRegistry"]
