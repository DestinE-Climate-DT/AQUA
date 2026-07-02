"""Intake sources for NetCDF and Zarr data built on the intake 2 readers.

These classes replace the external (unmaintained) intake-xarray package:
they only wire together ``intake.readers`` datatypes and readers from intake
base, and are registered in the intake driver registry under the same driver
names (``netcdf`` and ``zarr``) used by the catalog entries.

Example usage::

    from aqua.core.intake2 import NetCDFSource

    source = NetCDFSource("/path/to/data_*.nc", xarray_kwargs={"engine": "netcdf4"})
    data = source.to_dask()
"""

import importlib.util
import sys
import types

from intake import readers
from intake.source import register_driver

from .adapter import IntakeXarraySourceAdapter

DEFAULT_NETCDF_ENGINE = "netcdf4"


class NetCDFSource(IntakeXarraySourceAdapter):
    """
    Intake source opening NetCDF files with xarray, registered as the ``netcdf`` driver.

    Exposes ``self.data`` (the intake datatype, holding the url list) and
    ``self.xarray_kwargs`` for direct access by the AQUA backends.
    """

    name = "netcdf"

    def __init__(self, urlpath, xarray_kwargs=None, metadata=None,
                 storage_options=None, path_as_pattern=None, **kwargs):
        """
        Initialize the NetCDF source.

        Args:
            urlpath (str | list): Path(s) to the source file(s). May include globs.
            xarray_kwargs (dict, optional): Additional kwargs for xarray open_dataset/open_mfdataset.
            metadata (dict, optional): Catalog metadata for this source.
            storage_options (dict, optional): Parameters passed to the backend file-system (e.g. s3).
            path_as_pattern (bool | str, optional): Accepted for backward compatibility with
                                                    old intake-xarray catalogs; ignored.
            kwargs: Further parameters forwarded to the xarray reader (e.g. chunks, combine).
        """
        self.xarray_kwargs = dict(xarray_kwargs or {})
        read_kwargs = {**self.xarray_kwargs, **kwargs}
        # intake would default NetCDF3 data to the scipy/h5netcdf engines:
        # AQUA netcdf data may be in any netCDF flavour, so we default to the netcdf4 engine
        read_kwargs.setdefault("engine", DEFAULT_NETCDF_ENGINE)
        self.data = readers.datatypes.NetCDF3(urlpath, storage_options=storage_options,
                                              metadata=metadata)
        self.reader = readers.XArrayDatasetReader(self.data, metadata=metadata, **read_kwargs)
        super().__init__(metadata=metadata)


class ZarrSource(IntakeXarraySourceAdapter):
    """
    Intake source opening Zarr stores with xarray, registered as the ``zarr`` driver.

    Exposes ``self.data`` (the intake datatype, holding the url) and
    ``self.xarray_kwargs`` for direct access by the AQUA backends.
    """

    name = "zarr"

    def __init__(self, urlpath, storage_options=None, metadata=None,
                 xarray_kwargs=None, **kwargs):
        """
        Initialize the Zarr source.

        Args:
            urlpath (str): Path or url to the zarr store (local, s3, reference::, ...).
            storage_options (dict, optional): Parameters passed to the backend file-system.
            metadata (dict, optional): Catalog metadata for this source.
            xarray_kwargs (dict, optional): Additional kwargs for xarray open_dataset.
            kwargs: Further parameters forwarded to the xarray reader (e.g. chunks, consolidated).
        """
        self.xarray_kwargs = dict(xarray_kwargs or {})
        self.data = readers.datatypes.Zarr(urlpath, storage_options=storage_options,
                                           metadata=metadata)
        self.reader = readers.XArrayDatasetReader(self.data, metadata=metadata,
                                                  **self.xarray_kwargs, **kwargs)
        super().__init__(metadata=metadata)


def register_intake_drivers():
    """
    Register the AQUA ``netcdf`` and ``zarr`` drivers in the intake registry.

    Runtime registration with ``clobber=True`` takes precedence over the
    intake-xarray entry points when that package is still installed.
    """
    register_driver("netcdf", NetCDFSource, clobber=True)
    register_driver("zarr", ZarrSource, clobber=True)


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
    netcdf_mod.NetCDFSource = NetCDFSource
    xzarr_mod = types.ModuleType("intake_xarray.xzarr")
    xzarr_mod.ZarrSource = ZarrSource
    stub.netcdf = netcdf_mod
    stub.xzarr = xzarr_mod
    stub.__version__ = "0.0.0+aqua_stub"
    sys.modules["intake_xarray"] = stub
    sys.modules["intake_xarray.netcdf"] = netcdf_mod
    sys.modules["intake_xarray.xzarr"] = xzarr_mod
    return True


register_intake_drivers()
install_intake_xarray_stub()
