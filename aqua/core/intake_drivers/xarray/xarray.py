"""Intake sources for NetCDF and Zarr data, structured like the other AQUA drivers.

The actual opening logic lives in :mod:`aqua.core.intake_drivers.xarray.openers` (``open_netcdf``,
``open_zarr``), the intake 2 readers in :mod:`aqua.core.intake_drivers.xarray.readers`, while the
 datatypes are the ``intake.readers.datatypes`` ones (``NetCDF3``/``Zarr``), which already fit the
pattern.
names (``netcdf`` and ``zarr``) used by the catalog entries (see :mod:`aqua.core.intake_drivers`).

Example usage::

    from aqua.core.intake_drivers.xarray import IntakeNetCDFSource

    source = IntakeNetCDFSource("/path/to/data_*.nc", xarray_kwargs={"engine": "netcdf4"})
    data = source.to_dask()
"""

from intake.readers import datatypes

from aqua.core.util import setup_time_decoding

from .base import IntakeXarraySourceAdapter
from .openers import DEFAULT_NETCDF_ENGINE
from .readers import NetCDFDatasetReader, ZarrDatasetReader


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
        # self.xarray_kwargs keeps the raw catalog kwargs (use_cftime included) for
        # backend introspection; the reader kwargs get the legacy entry folded into
        # a CFDatetimeCoder, since intake merges the stored kwargs into each read call.
        self.xarray_kwargs = dict(xarray_kwargs or {})
        read_kwargs = setup_time_decoding({**self.xarray_kwargs, **kwargs})
        # make the engine explicit in the reader kwargs: AQUA netcdf data may be
        # in any netCDF flavour, so we default to the netcdf4 engine
        read_kwargs.setdefault("engine", DEFAULT_NETCDF_ENGINE)
        self.data = datatypes.NetCDF3(urlpath, storage_options=storage_options, metadata=metadata)
        self.reader = NetCDFDatasetReader(self.data, metadata=metadata, **read_kwargs)
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
        # same as the comment in IntakeNetCDFSource
        self.xarray_kwargs = dict(xarray_kwargs or {})
        read_kwargs = setup_time_decoding({**self.xarray_kwargs, **kwargs})
        self.data = datatypes.Zarr(urlpath, storage_options=storage_options, metadata=metadata)
        self.reader = ZarrDatasetReader(self.data, metadata=metadata, **read_kwargs)
        super().__init__(metadata=metadata)
