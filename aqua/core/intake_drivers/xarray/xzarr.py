"""AQUA-provided intake source for Zarr data, port of ``intake_xarray.xzarr``."""

from intake import readers

from .base import IntakeXarraySourceAdapter
from .readers import NetCDFZarrDatasetReader, fold_use_cftime


class IntakeZarrSource(IntakeXarraySourceAdapter):
    """Open one or more Zarr stores with xarray, registered as the ``zarr`` driver.

    Port of ``intake_xarray.xzarr.ZarrSource``, with the same AQUA deltas
    as :class:`~.netcdf.IntakeNetCDFSource` (exposed ``data``, ``metadata``
    and ``xarray_kwargs``, reads through :class:`NetCDFZarrDatasetReader`,
    ``use_cftime`` folding); an ``xarray_kwargs`` argument is accepted so
    that netcdf and zarr catalog entries share the same signature.

    Example usage::

        source = IntakeZarrSource("/path/to/store.zarr")
        data = source.to_dask()

    Args:
        urlpath (str | list): Path or url to the zarr store(s) (local, s3, ...).
        storage_options (dict, optional): Parameters passed to the backend file-system.
        metadata (dict, optional): Catalog metadata for this source.
        xarray_kwargs (dict, optional): Additional kwargs for the xarray open call.
        kwargs: Further parameters forwarded to the reader (e.g. chunks, consolidated).
    """

    name = "zarr"

    def __init__(self, urlpath, storage_options=None, metadata=None, xarray_kwargs=None, **kwargs):
        self.xarray_kwargs = xarray_kwargs = fold_use_cftime(dict(xarray_kwargs or {}))
        data = readers.datatypes.Zarr(urlpath, storage_options=storage_options, metadata=metadata)
        self.data = data
        self.reader = NetCDFZarrDatasetReader(data, **xarray_kwargs, metadata=metadata, **kwargs)
        super().__init__(metadata=metadata)
