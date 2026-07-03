from ..base import IntakeSourceAdapter
from .datatypes import Icechunk
from .readers import IcechunkDatasetReader


class IntakeIcechunkSource(IntakeSourceAdapter):
    """
    Intake DataSource for reading xarray Datasets from an IceChunk repository.

    Opens a local icechunk Zarr repository via a read-only session and returns
    a lazy ``xr.Dataset`` backed by dask arrays.  The driver name registered
    with intake is ``icechunk``, so catalog entries should declare
    ``driver: icechunk``.

    Example catalog entry::

        sources:
          drop-r100:
            driver: icechunk
            args:
              repo_path: /path/to/archive.zarr
              branch: main         # optional, default "main"
              chunks: auto         # optional, default "auto"
            metadata:
              source_grid_name: lon-lat
              fixer_name: ERA5

    Example usage::

        from aqua.core.intake_drivers.icechunk import IntakeIcechunkSource

        source = IntakeIcechunkSource(repo_path="/path/to/archive.zarr")
        ds = source.to_dask()
    """

    container = "xarray"
    name = "icechunk"
    version = ""

    def __init__(
        self,
        urlpath,
        branch="main",
        chunks="auto",
        xarray_kwargs=None,
        metadata=None,
        path_as_pattern=True,
        storage_options=None,
        **kwargs,
    ):
        data = Icechunk(urlpath, storage_options=storage_options, branch=branch, metadata=metadata)
        reader = IcechunkDatasetReader(data, **(xarray_kwargs or {}), metadata=metadata, **kwargs)
        self.reader = reader
        self.reader.metadata = metadata
        super().__init__(metadata=metadata)

    def to_dask(self, **kwargs):
        """
        Return a lazy dask-enabled xarray Dataset from the IceChunk repository.

        Additional keyword arguments are forwarded to the underlying reader's
        ``read`` method. The rest of the interface (``read``, ``get``) is
        inherited from :class:`aqua.core.intake_drivers.base.IntakeSourceAdapter`.
        """
        return self.reader.read(**kwargs)

    # rebind so that read_chunked follows this class' to_dask override
    read_chunked = to_dask
