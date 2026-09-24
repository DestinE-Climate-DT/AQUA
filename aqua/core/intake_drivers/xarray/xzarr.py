"""AQUA-provided intake source for Zarr data, port of ``intake_xarray.xzarr``."""

from intake import readers

from .base import IntakeXarraySourceAdapter
from .readers import TolerantXArrayDatasetReader


def _relax_s3_bucket_name_validation():
    """Drop botocore's AWS-only bucket-name validator from its builtin handlers.

    Some S3-compatible object stores used for AQUA zarr sources (e.g. LUMI-O) use bucket
    names containing a colon, which are invalid on real AWS: botocore's built-in
    ``validate_bucket_name`` handler rejects the request before it is even sent. Removing
    the entry from ``BUILTIN_HANDLERS`` (read by every new (aio)botocore ``Session``, as
    created internally by s3fs on every filesystem instantiation) disables that check
    process-wide; the actual S3-compatible endpoint still validates the bucket itself.
    """
    try:
        from botocore import handlers as botocore_handlers
    except ImportError:
        return
    botocore_handlers.BUILTIN_HANDLERS = [
        entry for entry in botocore_handlers.BUILTIN_HANDLERS if entry[1] is not botocore_handlers.validate_bucket_name
    ]


_relax_s3_bucket_name_validation()


def _is_fsspec_urlpath(urlpath):
    """Return True if urlpath (str or list of str) carries an fsspec protocol (e.g. ``s3://``)."""
    paths = urlpath if isinstance(urlpath, (list, tuple, set)) else [urlpath]
    return any(isinstance(p, str) and "://" in p for p in paths)


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

        # zarr rejects storage_options on a plain local-path store: it is only honoured
        # when the store is opened as an fsspec URI, so drop it for local urlpaths
        if storage_options and not _is_fsspec_urlpath(urlpath):
            storage_options = None

        data = readers.datatypes.Zarr(urlpath, storage_options=storage_options, metadata=metadata)
        self.reader = TolerantXArrayDatasetReader(data, **xarray_kwargs, metadata=metadata, **kwargs)
        super().__init__(data, xarray_kwargs, metadata)
