"""AQUA-provided intake source for Zarr data, port of ``intake_xarray.xzarr``."""

from intake import readers

from .base import IntakeXarraySourceAdapter


class IntakeZarrSource(IntakeXarraySourceAdapter):
    """Open one or more Zarr stores with xarray, registered as the ``zarr`` driver.

    Port of ``intake_xarray.xzarr.ZarrSource``, with the AQUA deltas shared with
    the netcdf source in :meth:`~.base.IntakeXarraySourceAdapter._setup` (attributes
    exposed for the backend, reads through :class:`~.readers.NetCDFZarrDatasetReader`,
    ``use_cftime`` folding); an ``xarray_kwargs`` argument is accepted so that netcdf
    and zarr catalog entries share the same signature.

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
        data = readers.datatypes.Zarr(urlpath, storage_options=storage_options, metadata=metadata)
        super().__init__(data, xarray_kwargs, metadata, **kwargs)
