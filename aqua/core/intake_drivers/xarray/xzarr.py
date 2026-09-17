"""AQUA-provided intake source for Zarr data, port of ``intake_xarray.xzarr``."""

from intake import readers

from .base import IntakeXarraySourceAdapter
from .readers import TolerantXArrayDatasetReader


class IntakeZarrSource(IntakeXarraySourceAdapter):
    """Open one or more Zarr stores with xarray, registered as the ``zarr`` driver.

    Port of ``intake_xarray.xzarr.ZarrSource``. AQUA deltas, as in the netcdf source:
    ``chunks`` defaults to ``{}``, reads go through
    :class:`~.readers.TolerantXArrayDatasetReader` (which also receives the catalog
    metadata), and the attributes the backend reads through are exposed by
    :class:`~.base.IntakeXarraySourceAdapter`; an ``xarray_kwargs`` argument is
    accepted so that netcdf and zarr catalog entries share the same signature.

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
        xarray_kwargs = dict(xarray_kwargs or {})
        # a single store takes the same eager ``xr.open_dataset`` route as a single netcdf file
        if "chunks" not in xarray_kwargs:
            kwargs.setdefault("chunks", {})

        data = readers.datatypes.Zarr(urlpath, storage_options=storage_options, metadata=metadata)
        self.reader = TolerantXArrayDatasetReader(data, **xarray_kwargs, metadata=metadata, **kwargs)
        super().__init__(data, xarray_kwargs, metadata)
