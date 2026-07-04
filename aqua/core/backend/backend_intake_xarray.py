"""Backend realization using the intake ``netcdf`` / ``zarr`` drivers for data handling."""

import os
import re
from glob import glob
from urllib.parse import urlparse

import pandas as pd

from aqua.core.configurer import ConfigPath
from aqua.core.data_model import DataModel
from aqua.core.exceptions import NoDataError
from aqua.core.fixer import Fixer
from aqua.core.util import DEFAULT_TIME_UNIT, files_exist, setup_time_decoding, to_list

from .backend import Backend
from .catalog_mixin import CatalogMixin


class BackendIntakeXarray(Backend, CatalogMixin):
    """
    Concrete backend retrieving data from NetCDF or Zarr files through the
    intake ``netcdf`` / ``zarr`` drivers provided by :mod:`aqua.core.intake_drivers.xarray`.

    Example usage::

        backend = BackendIntakeXarray(
            model="IFS",
            exp="test",
            source="2d",
            configurer=configurer,
        )
        data = backend.retrieve(var="2t", startdate="2020-01-01", enddate="2020-01-31")
    """

    def __init__(
        self,
        model: str,
        exp: str,
        source: str,
        configurer: ConfigPath,
        catalog: str = None,
        chunks: str | dict = "auto",
        fixer: Fixer = None,
        datamodel: DataModel = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        """
        Initialize the BackendIntakeXarray instance.

        Args:
            model (str): Model name.
            exp (str): Experiment name.
            source (str): Data source.
            configurer (ConfigPath): An instance of ConfigPath to manage configuration paths.
            catalog (str, optional): Catalog name. Defaults to None.
            chunks (str | dict, optional): Chunking strategy for xarray. Defaults to "auto".
            fixer (Fixer, optional): An instance of Fixer to apply data fixes. Defaults to None.
            datamodel (DataModel, optional): An instance of DataModel to define the data structure. Defaults to None.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
            kwargs: Additional keyword arguments forwarded to the intake catalog source entry.
        """
        Backend.__init__(self, fixer=fixer, datamodel=datamodel, loglevel=loglevel)
        self.setup_catalog(model, exp, source, configurer, catalog, chunks, **kwargs)

        # The AQUA intake 2 sources expose the data object (url list), metadata and
        # xarray_kwargs directly, so no reader introspection is needed here.
        # The check must run for both netcdf and zarr sources: besides the meaningful
        # error message for local sources (see #943), its glob expansion is required
        # because intake only detects globs in plain-string urls, and to_list() has
        # just turned the url into a list (e.g. DROP-generated zarr entries use globs).
        self.esmcat.data.url = to_list(self.esmcat.data.url)
        self._check_files_exist()

        # Snapshot the full (glob-expanded) URL list so that _filter_netcdf_files always
        # filters from the complete set, not from a previously-filtered subset.
        # Without this, a second retrieve() call with different dates would start from
        # the already-narrowed list produced by the first call.
        self._all_urls = list(self.esmcat.data.url)

    def _setup_xarray_kwargs(self, esmcat):
        """
        Build the kwargs for the reader read call from the catalog xarray_kwargs.

        A CFDatetimeCoder is set as time decoder via setup_time_decoding, using the
        'time_coder' metadata entry when present and DEFAULT_TIME_UNIT (microseconds)
        otherwise. A legacy 'use_cftime' entry in the catalog xarray_kwargs is folded
        into the coder, and an explicit 'decode_times: False' from the catalog is respected.
        """
        read_kwargs = getattr(esmcat, "xarray_kwargs", {}).copy()

        if "time_coder" in esmcat.metadata:
            self.logger.info("Using custom pandas/xarray time coder: %s", esmcat.metadata["time_coder"])
            time_unit = esmcat.metadata["time_coder"]
        else:
            time_unit = DEFAULT_TIME_UNIT

        return setup_time_decoding(read_kwargs, time_unit=time_unit, loglevel=self.loglevel)

    def _setup_intake_catalog(self, esmcat, startdate: str = None, enddate: str = None):
        """
        Prepare the intake source for data retrieval, narrowing its file list to the
        requested date range when the catalog opts in via the 'filter_key' metadata
        entry (e.g. yearly-split LRA files). Sources without 'filter_key' are
        returned untouched.

        Args:
            esmcat: The intake source entry to prepare.
            startdate (str, optional): Start date (YYYY-MM-DD) for file filtering. Defaults to None.
            enddate (str, optional): End date (YYYY-MM-DD) for file filtering. Defaults to None.

        Returns:
            The intake source, with the url list filtered when applicable.
        """

        # Only apply year-based file filtering when the catalog explicitly requests it
        # via the 'filter_key' metadata entry. Unconditional filtering would drop all
        # files for catalogs whose filenames do not contain year tokens.
        filter_key = esmcat.metadata.get("filter_key")
        if filter_key:
            self.logger.info("Filtering netcdf files in the catalog based on %s", filter_key)
            esmcat = self._filter_netcdf_files(esmcat, filter_key=filter_key, startdate=startdate, enddate=enddate)

        return esmcat

    def _check_files_exist(self):
        """
        Check if the netcdf files or zarr stores exist in the catalog. Raise NoDataError if any is missing.
        """
        # Manually expand globs to ensure xarray always receives an explicit list of files/stores.
        # This avoids issues where xarray fails on a list of glob strings or single globs in lists.
        # We assume all the files in the catalog have the same scheme (e.g., 'file', 'http', 's3', etc.)
        self.logger.info("Checking existence of data files in the catalog: %s", self.esmcat.data.url)
        if urlparse(self.esmcat.data.url[0]).scheme in ["file", ""]:
            if not files_exist(self.esmcat.data.url):
                raise NoDataError(
                    f"Some data files are missing for {self.model} {self.exp} {self.source}, "
                    + f"please check the url: {self.esmcat.data.url}"
                )
            self.esmcat.data.url = sorted([f for x in self.esmcat.data.url for f in glob(x)])
        else:
            self.logger.info("Remote files detected, skipping existence check for data files in the catalog.")

    def retrieve_plain(self, startdate: str = None):
        """
        Retrieve minimal data from the catalog to fetch the Regridder init.

        Args:
            startdate (str, optional): Start date (YYYY-MM-DD). Defaults to None.
            enddate (str, optional): End date (YYYY-MM-DD). Defaults to None.
        """
        read_kwargs = self._setup_xarray_kwargs(esmcat=self.esmcat)
        esmcat = self._setup_intake_catalog(esmcat=self.esmcat, startdate=startdate, enddate=startdate)
        data = esmcat.reader.read(**read_kwargs)
        data = self._grid_inspector(data, startdate)
        return data

    def retrieve(
        self,
        var: str | list = None,
        level: str | list = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ):
        """
        Retrieve data from a NetCDF or Zarr source as an xarray.Dataset.

        Args:
            var (str | list, optional): Variable(s) to retrieve. Defaults to None (all).
            level (str | list, optional): Level(s) to select. Defaults to None.
            level_coord (str, optional): Name of the vertical coordinate. Defaults to None.
            startdate (str, optional): Start date (YYYY-MM-DD). Defaults to None.
            enddate (str, optional): End date (YYYY-MM-DD). Defaults to None.

        Returns:
            xr.Dataset: Dataset with fixes, data model, and date/level selection applied.
        """
        read_kwargs = self._setup_xarray_kwargs(esmcat=self.esmcat)
        esmcat = self._setup_intake_catalog(esmcat=self.esmcat, startdate=startdate, enddate=enddate)
        data = esmcat.reader.read(**read_kwargs)

        data = self._postprocess_data(
            data=data,
            var=var,
            level=level,
            level_coord=level_coord,
            startdate=startdate,
            enddate=enddate,
        )

        return data

    def _filter_netcdf_files(self, esmcat, filter_key="year", startdate=None, enddate=None):
        """
        Filter the esmcat to include only netcdf files based on specific filter_key.

        Filters from ``self._all_urls`` (the full snapshot saved at init time) so that
        repeated calls with different date ranges always start from the complete file list.

        Args:
            esmcat (intake.catalog.Catalog): your catalog
            filter_key (str): type of filter to apply (default is "year")
            startdate (str): start date in format YYYY-MM-DD
            enddate (str): end date in format YYYY-MM-DD

        Returns:
            intake.catalog.Catalog: filtered catalog
        """
        # Filter from the immutable snapshot saved at init, not from esmcat.data.url
        # which may already be narrowed by a previous retrieve() call.
        files = list(self._all_urls)
        self.logger.debug("Total files before filtering: %s", len(files))

        # this will consider only files that have "year" in their filename
        # within the startdate and enddate range
        if filter_key == "year":
            if startdate and enddate:
                keys = list(range(pd.Timestamp(startdate).year, pd.Timestamp(enddate).year + 1))
                # create regex pattern for each year: only yyyy will be detected
                pattern = [re.compile(rf"(?<!\d){yr}(?!\d)") for yr in keys]
                files = [f for f in files if any(p.search(os.path.basename(f)) for p in pattern)]
        else:
            raise ValueError(f"Filter type {filter_key} not recognized.")

        # replace the url with the expanded/filtered list
        esmcat.data.url = files
        self.logger.debug("Total files after filtering: %s", len(esmcat.data.url))

        if len(esmcat.data.url) == 0:
            raise NoDataError("No files found after filtering the catalog!")

        self.logger.debug(
            "Selected: %s files from %s to %s",
            len(esmcat.data.url),
            esmcat.data.url[0],
            esmcat.data.url[-1],
        )

        return esmcat
