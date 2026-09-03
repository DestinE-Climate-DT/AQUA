"""Compatibility stub for catalogs still referencing the intake_xarray package."""

import importlib.util
import sys
import types

from .netcdf import IntakeNetCDFSource
from .xzarr import IntakeZarrSource


# TODO: remove once the catalogs drops the plugin block
def install_intake_xarray_stub():
    """
    Install a stub ``intake_xarray`` module when the real package is absent.

    Legacy catalog files may still contain a ``plugins: source: - module: intake_xarray``
    block, which makes intake import that module at catalog-open time. The stub keeps
    those catalogs working, mapping the old source classes to the AQUA ones.

    Returns:
        bool: True if the stub was installed, False if the module is already available.
    """
    if "intake_xarray" in sys.modules:
        return False
    try:
        if importlib.util.find_spec("intake_xarray") is not None:
            return False
    except (ImportError, ValueError):
        pass

    stub = types.ModuleType("intake_xarray")
    netcdf_mod = types.ModuleType("intake_xarray.netcdf")
    netcdf_mod.NetCDFSource = IntakeNetCDFSource
    xzarr_mod = types.ModuleType("intake_xarray.xzarr")
    xzarr_mod.ZarrSource = IntakeZarrSource
    stub.netcdf = netcdf_mod
    stub.xzarr = xzarr_mod
    stub.__version__ = "0.0.0+aqua_stub"
    sys.modules["intake_xarray"] = stub
    sys.modules["intake_xarray.netcdf"] = netcdf_mod
    sys.modules["intake_xarray.xzarr"] = xzarr_mod
    return True
