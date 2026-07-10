"""Backend realization using the icechunk intake driver for data handling."""

import xarray as xr

from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer
from aqua.core.logger import log_history
from aqua.core.version import __version__ as aqua_version

from .backend import Backend
from .catalog_mixin import CatalogMixin

xr.set_options(keep_attrs=True)


class BackendIntakeIcechunk(Backend, CatalogMixin):
    """
    Concrete backend retrieving data from IceChunk Zarr repositories through
    the ``icechunk`` intake driver.

    A single catalog entry points to one icechunk repository (``archive.zarr``
    written by :class:`aqua.core.drop.IcechunkWriter`).  The full dataset is
    opened lazily once per backend instance; date and level selection are
    applied afterwards via ``_postprocess_data``.

    Example catalog entry::

        sources:
          drop-r100:
            driver: icechunk
            args:
              urlpath: /path/to/archive.zarr
              branch: main
            metadata:
              source_grid_name: lon-lat
              fixer_name: ERA5

    Example usage::

        backend = BackendIntakeIcechunk(
            model="ICON-5km",
            exp="baseline-hist",
            source="drop-r100",
            configurer=configurer,
        )
        data = backend.retrieve(var="2t", startdate="1990-01-01", enddate="1990-12-31")
    """

    def __init__(
        self,
        model: str,
        exp: str,
        source: str,
        configurer,
        catalog: str = None,
        chunks: "str | dict" = "auto",
        fixer: Fixer = None,
        datamodel: DataModel = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        """
        Initialize the BackendIntakeIcechunk instance.

        Args:
            model (str): Model name.
            exp (str): Experiment name.
            source (str): Data source identifier.
            configurer (ConfigPath): An instance of ConfigPath to manage
                configuration paths.
            catalog (str, optional): Catalog name. Defaults to None (auto-detect).
            chunks (str | dict, optional): Chunking strategy for xarray.
                Defaults to ``"auto"``.
            fixer (Fixer, optional): An instance of Fixer to apply data fixes.
                Defaults to None.
            datamodel (DataModel, optional): An instance of DataModel to define
                the data structure. Defaults to None.
            loglevel (str, optional): Logging level. Defaults to ``"WARNING"``.
            kwargs: Additional keyword arguments forwarded to the intake catalog
                source entry.
        """
        Backend.__init__(self, fixer=fixer, datamodel=datamodel, loglevel=loglevel)
        self.setup_catalog(model, exp, source, configurer, catalog, chunks, **kwargs)

    def retrieve_plain(self, startdate: str = None):
        """
        Retrieve minimal data from the catalog to initialize the Regridder.

        Opens the full lazy dataset and uses ``_grid_inspector`` to return the
        smallest representative sample needed for grid detection.

        Args:
            startdate (str, optional): Start date (YYYY-MM-DD) used to select a
                single time step. Defaults to None (uses first time step).

        Returns:
            xr.Dataset: Minimal sample dataset for grid inspection.
        """
        data = self.esmcat.to_dask()
        return self._grid_inspector(data, startdate)

    def retrieve(
        self,
        var: "str | list" = None,
        level: "str | list" = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ):
        """
        Retrieve data from an IceChunk repository as a lazy xarray.Dataset.

        Opens the full repository lazily, then applies variable selection,
        date slicing, level selection, fixer, and data model transformations
        via :meth:`_postprocess_data`.

        Args:
            var (str | list, optional): Variable(s) to retrieve. Defaults to
                None (all variables in the dataset).
            level (str | list, optional): Level(s) to select. Defaults to None.
            level_coord (str, optional): Name of the vertical coordinate.
                Defaults to None.
            startdate (str, optional): Start date (YYYY-MM-DD). Defaults to None.
            enddate (str, optional): End date (YYYY-MM-DD). Defaults to None.

        Returns:
            xr.Dataset: Dataset with fixes, data model, and date/level selection
            applied.
        """
        data = self.esmcat.to_dask()

        data = self._postprocess_data(
            data=data,
            var=var,
            level=level,
            level_coord=level_coord,
            startdate=startdate,
            enddate=enddate,
        )

        return data

    def log_history(self, data: xr.Dataset) -> xr.Dataset:
        """
        Log a message in the dataset's history attribute.
        """
        return log_history(
            data,
            f"Retrieved from {self.catalog} {self.model} {self.exp} {self.source} "
            f"using AQUA v{aqua_version} with IntakeIcechunk",
        )
