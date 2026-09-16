"""AQUA-provided intake source for NetCDF data, port of ``intake_xarray.netcdf``."""

from intake import readers

from .base import IntakeXarraySourceAdapter
from .readers import NetCDFZarrDatasetReader


class IntakeNetCDFSource(IntakeXarraySourceAdapter):
    """Open one or more NetCDF files with xarray, registered as the ``netcdf`` driver.

    Port of ``intake_xarray.netcdf.NetCDFSource`` (intake-xarray 2.0.0). AQUA deltas:
    the xarray engine defaults to netcdf4, ``chunks`` defaults to ``{}``, plain
    (non-pattern) urls are read through :class:`~.readers.NetCDFZarrDatasetReader`,
    and the attributes the backend reads through are exposed by
    :class:`~.base.IntakeXarraySourceAdapter`.

    Example usage::

        source = IntakeNetCDFSource("/path/to/data_*.nc", xarray_kwargs={"engine": "h5netcdf"})
        data = source.to_dask()

    Args:
        urlpath (str | list): Path(s) to the source file(s). May be a list, include glob ``*``
            characters or ``{field}`` format patterns, e.g. ``{{ CATALOG_DIR }}/data/air.nc``,
            ``{{ CATALOG_DIR }}/data/*.nc``, ``{{ CATALOG_DIR }}/data/air_{year}.nc``.
        xarray_kwargs (dict, optional): Additional kwargs for ``xr.open_dataset()`` /
            ``xr.open_mfdataset()``. Defaults to None.
        metadata (dict, optional): Catalog metadata for this source (``fixer_name``,
            ``source_grid_name``, ``filter_key``, ...). Defaults to None.
        path_as_pattern (bool | str, optional): Whether to treat the path as a pattern (ie.
            ``data_{field}.nc``) and create new coordinates in the output corresponding to the
            pattern fields. If str, it is treated as the pattern to match on. Defaults to True.
        storage_options (dict, optional): If using a remote fs, the kwargs to pass to that FS.
            Defaults to None.
        chunks (int | dict, optional): Used to load the dataset into dask arrays; ``chunks={}``
            uses a single chunk per array, ``chunks=None`` bypasses dask. Defaults to ``{}``
            (AQUA delta: intake-xarray only applies it inside ``to_dask()``).
        combine ({'by_coords', 'nested'}, optional): Which function concatenates the files when
            urlpath resolves to more than one; passed to ``xr.open_mfdataset`` (default
            ``by_coords``) and dropped on single-file reads, see
            :class:`~.readers.NetCDFZarrDatasetReader`.
        concat_dim (str, optional): Dimension to concatenate the files along. Can be new or
            pre-existing if ``combine="nested"``; must be None or new if ``combine="by_coords"``.
        kwargs: Further parameters forwarded to the reader.
    """

    name = "netcdf"

    def __init__(self, urlpath, xarray_kwargs=None, metadata=None, path_as_pattern=True, storage_options=None, **kwargs):
        xarray_kwargs = dict(xarray_kwargs or {})
        # intake infers engine="scipy" from the NetCDF3 datatype, and scipy fails on any
        # other flavour: netcdf4 reads them all (the backend used to force it downstream)
        xarray_kwargs.setdefault("engine", "netcdf4")
        # AQUA always wants lazy data, but intake routes a single file to ``xr.open_dataset``,
        # which is eager unless ``chunks`` is given
        if "chunks" not in xarray_kwargs:
            kwargs.setdefault("chunks", {})

        data = readers.datatypes.NetCDF3(urlpath, storage_options=storage_options, metadata=metadata)
        if (path_as_pattern is True and "{" in urlpath) or isinstance(path_as_pattern, str):
            # ``{field}`` patterns become output coordinates: intake has its own reader for that
            reader = readers.XArrayPatternReader(data, **xarray_kwargs, metadata=metadata, pattern=path_as_pattern, **kwargs)
        else:
            reader = NetCDFZarrDatasetReader(data, **xarray_kwargs, metadata=metadata, **kwargs)
        self.reader = reader
        super().__init__(data, xarray_kwargs, metadata)
