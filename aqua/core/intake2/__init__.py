"""AQUA intake 2 integration layer.

Provides the v1-style source adapters and the xarray-based intake sources.
Importing this package registers the ``netcdf`` and ``zarr`` drivers in the
intake registry (replacing the unmaintained intake-xarray package) and, when
intake-xarray is not installed, a compatibility stub module for legacy
catalogs still importing it through a ``plugins`` block.
"""

from .adapter import IntakeSourceAdapter, IntakeXarraySourceAdapter
from .xarray_sources import NetCDFSource, ZarrSource, install_intake_xarray_stub, register_intake_drivers

__all__ = [
    "IntakeSourceAdapter",
    "IntakeXarraySourceAdapter",
    "NetCDFSource",
    "ZarrSource",
    "install_intake_xarray_stub",
    "register_intake_drivers",
]
