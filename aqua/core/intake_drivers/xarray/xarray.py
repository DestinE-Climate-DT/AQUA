"""Intake sources for NetCDF and Zarr data built on the intake 2 readers.

These classes replace the external (unmaintained) intake-xarray package.
Unlike the ``fdb`` and ``icechunk`` drivers, this driver has no local
``datatypes``/``readers``/``openers`` modules: the datatypes
(``intake.readers.datatypes.NetCDF3`` / ``Zarr``) and the reader
(``intake.readers.XArrayDatasetReader``) are provided by intake core, so the
sources here only wire them together. They are registered in the intake
driver registry (see :mod:`aqua.core.intake_drivers`) under the same driver
names (``netcdf`` and ``zarr``) used by the catalog entries.

Example usage::

    from aqua.core.intake_drivers.xarray import IntakeNetCDFSource

    source = IntakeNetCDFSource("/path/to/data_*.nc", xarray_kwargs={"engine": "netcdf4"})
    data = source.to_dask()
"""

from intake import readers

from .base import IntakeXarraySourceAdapter

DEFAULT_NETCDF_ENGINE = "netcdf4"


class IntakeNetCDFSource(IntakeXarraySourceAdapter):
    """
    Intake source opening NetCDF files with xarray, registered as the ``netcdf`` driver.

    Exposes ``self.data`` (the intake datatype, holding the url list) and
    ``self.xarray_kwargs`` for direct access by the AQUA backends.
    """

    name = "netcdf"

    def __init__(self, urlpath, xarray_kwargs=None, metadata=None, storage_options=None, path_as_pattern=None, **kwargs):
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
        self.data = readers.datatypes.NetCDF3(urlpath, storage_options=storage_options, metadata=metadata)
        self.reader = readers.XArrayDatasetReader(self.data, metadata=metadata, **read_kwargs)
        super().__init__(metadata=metadata)


class IntakeZarrSource(IntakeXarraySourceAdapter):
    """
    Intake source opening Zarr stores with xarray, registered as the ``zarr`` driver.

    Exposes ``self.data`` (the intake datatype, holding the url) and
    ``self.xarray_kwargs`` for direct access by the AQUA backends.
    """

    name = "zarr"

    def __init__(self, urlpath, storage_options=None, metadata=None, xarray_kwargs=None, **kwargs):
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
        self.data = readers.datatypes.Zarr(urlpath, storage_options=storage_options, metadata=metadata)
        self.reader = readers.XArrayDatasetReader(self.data, metadata=metadata, **self.xarray_kwargs, **kwargs)
        super().__init__(metadata=metadata)
