""" Test utilities for AQUA test suite """

from .config import AQUA_TESTS, make_private_config

DPI = 50
# Tolerances: by default compare with plain ``pytest.approx(value)``, i.e. pytest's own rel=1e-6.
# APPROX_REL is the exception, wider than the pytest default: use it (``rel=APPROX_REL``) only where
# 1e-6 is not achievable, e.g. results that depend on CDO/ESMF weights, dask reductions or platform
# floating point; never use it where nearby values must be told apart (see test_accessor.py).
APPROX_REL = 1e-4
LOGLEVEL = "DEBUG"

__all__ = ["APPROX_REL", "AQUA_TESTS", "DPI", "LOGLEVEL", "make_private_config"]
