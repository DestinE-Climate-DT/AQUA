"""AQUA-provided intake source for NetCDF data, port of ``intake_xarray.netcdf``."""

from intake import readers

from .base import IntakeXarraySourceAdapter
from .readers import NetCDFZarrDatasetReader, fold_use_cftime


class IntakeNetCDFSource(IntakeXarraySourceAdapter):
    """Open one or more NetCDF files with xarray, registered as the ``netcdf`` driver.

    Port of ``intake_xarray.netcdf.NetCDFSource`` (intake-xarray 2.0.0). AQUA deltas:
    ``self.data``, ``self.metadata`` and ``self.xarray_kwargs`` are exposed for the
    backend (``data`` is the same object the reader reads from, so its ``url`` can be
    filtered between reads); the engine defaults to netcdf4; reads go through
    :class:`NetCDFZarrDatasetReader`; a legacy ``use_cftime`` is folded into a coder.

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
        self.xarray_kwargs = xarray_kwargs = fold_use_cftime(dict(xarray_kwargs or {}))
        xarray_kwargs.setdefault("engine", "netcdf4")
        data = readers.datatypes.NetCDF3(urlpath, storage_options=storage_options, metadata=metadata)
        self.data = data
        if (path_as_pattern is True and "{" in urlpath) or isinstance(path_as_pattern, str):
            self.reader = readers.XArrayPatternReader(
                data, **xarray_kwargs, metadata=metadata, pattern=path_as_pattern, **kwargs
            )
        else:
            self.reader = NetCDFZarrDatasetReader(data, **xarray_kwargs, metadata=metadata, **kwargs)
        super().__init__(metadata=metadata)
