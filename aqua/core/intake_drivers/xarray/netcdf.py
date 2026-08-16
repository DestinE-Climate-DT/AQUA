"""AQUA-provided intake source for NetCDF data, port of ``intake_xarray.netcdf``."""

from intake import readers

from .base import IntakeXarraySourceAdapter


class IntakeNetCDFSource(IntakeXarraySourceAdapter):
    """Open one or more NetCDF files with xarray, registered as the ``netcdf`` driver.

    Port of ``intake_xarray.netcdf.NetCDFSource`` (intake-xarray 2.0.0), with the
    xarray engine defaulting to netcdf4. The rest of the AQUA deltas (attributes
    exposed for the backend, reads through
    :class:`~.readers.NetCDFZarrDatasetReader`, ``use_cftime`` folding) is shared
    with the zarr source in :meth:`~.base.IntakeXarraySourceAdapter._setup`.

    Example usage::

        source = IntakeNetCDFSource("/path/to/data_*.nc", xarray_kwargs={"engine": "h5netcdf"})
        data = source.to_dask()

    Args:
        urlpath (str | list): Path(s) to the source file(s); may include globs
                              or ``{field}`` patterns (see ``path_as_pattern``).
        xarray_kwargs (dict, optional): Additional kwargs for the xarray open call.
        metadata (dict, optional): Catalog metadata for this source.
        path_as_pattern (bool | str, optional): Treat the path as a ``{field}`` pattern. Defaults to True.
        storage_options (dict, optional): Parameters passed to the backend file-system (e.g. s3).
        kwargs: Further parameters forwarded to the reader (e.g. chunks, combine).
    """

    name = "netcdf"

    def __init__(self, urlpath, xarray_kwargs=None, metadata=None, path_as_pattern=True, storage_options=None, **kwargs):
        xarray_kwargs = dict(xarray_kwargs or {})
        # intake infers engine="scipy" from the NetCDF3 datatype, and scipy fails on any
        # other flavour: netcdf4 reads them all (the backend used to force it downstream)
        xarray_kwargs.setdefault("engine", "netcdf4")
        data = readers.datatypes.NetCDF3(urlpath, storage_options=storage_options, metadata=metadata)

        if (path_as_pattern is True and "{" in urlpath) or isinstance(path_as_pattern, str):
            # ``{field}`` patterns become output coordinates: intake has its own reader for that
            super().__init__(
                data, xarray_kwargs, metadata, reader_class=readers.XArrayPatternReader, pattern=path_as_pattern, **kwargs
            )
        else:
            super().__init__(data, xarray_kwargs, metadata, **kwargs)
