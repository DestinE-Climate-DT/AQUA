"""Expose Destination Earth Climate-DT FDB data as xarray.Datasets using z3fdb."""

from .core import open_z3fdb

__all__ =  ["open_z3fdb"]
__version__ = "0.1.0"
