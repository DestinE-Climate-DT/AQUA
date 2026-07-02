"""Minimal v1-style adapters exposing intake 2 readers as intake sources.

These adapters bridge the intake 2 reader interface (``intake.readers``)
with the v1 ``DataSource`` interface still used by the AQUA catalogs
(``to_dask``, ``read``, ``read_chunked``, ``get``). Concrete sources set
``self.reader`` in their constructor and inherit the whole contract.

Example usage::

    from aqua.core.intake2 import IntakeXarraySourceAdapter

    class MySource(IntakeXarraySourceAdapter):
        name = "mysource"

        def __init__(self, urlpath, metadata=None, **kwargs):
            self.data = readers.datatypes.NetCDF3(urlpath, metadata=metadata)
            self.reader = readers.XArrayDatasetReader(self.data, metadata=metadata, **kwargs)
            super().__init__(metadata=metadata)
"""

from intake.source import base


class IntakeSourceAdapter(base.DataSource):
    """
    Adapter exposing an intake 2 reader through the intake v1 DataSource interface.

    Concrete subclasses must set ``self.reader`` (an ``intake.readers.BaseReader``)
    in their ``__init__`` and then call ``super().__init__(metadata=metadata)``.

    Note:
        Subclassing ``intake.source.base.DataSource`` is mandatory: the intake
        runtime driver registry only accepts ``DataSource`` subclasses
        (drivers loaded from entry points skip that check).
    """

    container = "xarray"
    name = "xarray"
    version = ""

    def to_dask(self):
        """Read the data artefact lazily through the intake 2 reader."""
        return self.reader.read()

    def __call__(self, *args, **kwargs):
        """Sources are fully configured at construction time: return self."""
        return self

    get = __call__

    def read(self):
        """Read the data artefact eagerly (no dask chunking)."""
        return self.reader(chunks=None).read()

    discover = read

    read_chunked = to_dask


class IntakeXarraySourceAdapter(IntakeSourceAdapter):
    """
    Adapter for xarray-based sources (netcdf, zarr).

    ``to_dask`` guarantees a dask-backed dataset by defaulting to ``chunks={}``
    when no chunking is configured, preserving the contract of the former
    ``intake_xarray.base.IntakeXarraySourceAdapter``.
    """

    def to_dask(self):
        """Read the data as a dask-backed dataset, defaulting to ``chunks={}``."""
        if "chunks" not in self.reader.kwargs:
            return self.reader(chunks={}).read()
        return self.reader.read()

    # rebind so that read_chunked follows this class' to_dask override
    read_chunked = to_dask
