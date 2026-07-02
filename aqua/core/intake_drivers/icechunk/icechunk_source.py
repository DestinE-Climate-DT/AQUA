"""Intake DataSource for IceChunk Zarr repositories."""

import icechunk
import xarray as xr
from intake.source import base
from intake.source.base import Schema

from aqua.core.logger import log_configure

xr.set_options(keep_attrs=True)


class IcechunkSource(base.DataSource):
    """Intake DataSource for reading xarray Datasets from an IceChunk repository.

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

        from aqua.core.intake_drivers.icechunk import IcechunkSource

        source = IcechunkSource(repo_path="/path/to/archive.zarr")
        ds = source.to_dask()
    """

    container = "xarray"
    name = "icechunk"
    version = ""

    def __init__(
        self,
        repo_path: str,
        branch: str = "main",
        chunks: "str | dict" = "auto",
        xarray_kwargs: dict = None,
        metadata: dict = None,
        loglevel: str = "WARNING",
    ):
        """
        Initialize the IcechunkSource.

        Args:
            repo_path (str): Absolute path to the icechunk repository directory
                (e.g. ``/path/to/archive.zarr``).
            branch (str, optional): Branch (snapshot) to open for reading.
                Defaults to ``"main"``.
            chunks (str | dict, optional): Chunking strategy forwarded to
                ``xr.open_zarr``. Defaults to ``"auto"``.
            xarray_kwargs (dict, optional): Additional keyword arguments
                forwarded verbatim to ``xr.open_zarr``. Defaults to ``None``.
            metadata (dict, optional): Intake metadata dict passed to the base
                class. Defaults to ``None``.
            loglevel (str, optional): Logging level. Defaults to ``"WARNING"``.
        """
        self.repo_path = repo_path
        self.branch = branch
        self.chunks = chunks
        self.xarray_kwargs = xarray_kwargs or {}
        self._ds = None
        self.logger = log_configure(log_level=loglevel, log_name="IcechunkSource")
        super().__init__(metadata=metadata)

    def _open_dataset(self):
        """Open the icechunk repository and load the dataset lazily.

        Raises:
            ImportError: When the ``icechunk`` package is not installed.
        """
        self.logger.debug("Opening icechunk repository at %s (branch: %s)", self.repo_path, self.branch)
        storage = icechunk.local_filesystem_storage(self.repo_path)
        repo = icechunk.Repository.open(storage)
        session = repo.readonly_session(self.branch)
        self._ds = xr.open_zarr(
            session.store,
            consolidated=False,
            chunks=self.chunks,
            **self.xarray_kwargs,
        )

    def _get_schema(self):
        """Return a minimal intake Schema for the dataset.

        Returns:
            Schema: Intake schema dict with dtype/shape/npartitions set to None.
        """
        if self._ds is None:
            self._open_dataset()
        return Schema(
            dtype=None,
            shape=None,
            npartitions=None,
            extra_metadata=self.metadata,
        )

    def to_dask(self):
        """Return the dataset as a lazy dask-backed xarray.Dataset.

        Returns:
            xr.Dataset: Lazy dataset backed by dask arrays.
        """
        if self._ds is None:
            self._open_dataset()
        return self._ds

    def read(self):
        """Return the dataset lazily (equivalent to ``to_dask`` for zarr-backed stores).

        IceChunk stores are opened lazily by default via zarr; calling this
        method does **not** trigger an immediate ``.compute()``.

        Returns:
            xr.Dataset: Lazy dataset.
        """
        return self.to_dask()

    def _close(self):
        """Release the dataset reference."""
        self._ds = None
