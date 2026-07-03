"""Adapter for the xarray-based sources, built on the shared AQUA intake 2 adapter."""

from ..base import IntakeSourceAdapter


class IntakeXarraySourceAdapter(IntakeSourceAdapter):
    """
    Adapter for xarray-based sources (netcdf, zarr).

    ``to_dask`` guarantees a dask-backed dataset by defaulting to ``chunks={}``
    when no chunking is configured, preserving the contract of the former
    ``intake_xarray.base.IntakeXarraySourceAdapter``. The rest of the interface
    (``read``, ``read_chunked``, ``get``) is inherited from
    :class:`aqua.core.intake_drivers.base.IntakeSourceAdapter`.
    """

    def to_dask(self):
        """Read the data as a dask-backed dataset, defaulting to ``chunks={}``."""
        if "chunks" not in self.reader.kwargs:
            return self.reader(chunks={}).read()
        return self.reader.read()

    # rebind so that read_chunked follows this class' to_dask override
    read_chunked = to_dask
