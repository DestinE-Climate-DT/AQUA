""" Test utilities for AQUA test suite """

from .config import AQUA_TESTS, make_private_config

DPI = 50
APPROX_REL = 1e-4
LOGLEVEL = "DEBUG"

__all__ = ["APPROX_REL", "AQUA_TESTS", "DPI", "LOGLEVEL", "make_private_config"]
