"""Base adapter shared by all the AQUA intake drivers (fdb, icechunk, netcdf/zarr).

It bridges the two intake interfaces: the classic (v1) ``DataSource`` one —
``to_dask``, ``read``, ``read_chunked``, ``get`` — which is what the AQUA YAML
catalogs and backends consume, and the intake 2 model (``intake.readers``),
where the actual reading happens. The adapter is a v1 ``DataSource`` on the
outside and delegates every read to the intake 2 reader stored in ``self.reader``.
Concrete sources set ``self.reader`` in their constructor and inherit the whole
contract.

Example usage (xarray-based sources should rather subclass
:class:`aqua.core.intake_drivers.xarray.base.IntakeXarraySourceAdapter`)::

    from intake import readers
    from aqua.core.intake_drivers.base import IntakeSourceAdapter


    class MySource(IntakeSourceAdapter):
        name = "mysource"

        def __init__(self, urlpath, metadata=None, **kwargs):
            # self.data is optional, exposed for backend introspection
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
