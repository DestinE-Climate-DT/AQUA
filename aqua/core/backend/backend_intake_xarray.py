"""Backend realization using intake-xarray for data handling."""

import os
import re
from glob import glob

import intake_xarray
import pandas as pd
import xarray as xr

from aqua.core.configurer import ConfigPath
from aqua.core.data_model import DataModel
from aqua.core.exceptions import NoDataError
from aqua.core.fixer import Fixer
from aqua.core.util import DEFAULT_TIME_UNIT, files_exist, to_list

from .backend_intake import BackendIntake


class BackendIntakeXarray(BackendIntake):
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
            format (str, optional): Format of the data file. If None, it will be detected automatically.
            chunks (str | dict, optional): Chunking strategy for xarray. Defaults to "auto".
            fixer (Fixer, optional): An instance of Fixer to apply data fixes. Defaults to None.
            datamodel (DataModel, optional): An instance of DataModel to define the data structure. Defaults to None.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
            kwargs: Additional keyword arguments to pass to xarray's open_dataset or open_zarr functions.
        """

        super().__init__(
            model,
            exp,
            source,
            configurer=configurer,
            catalog=catalog,
            chunks=chunks,
            fixer=fixer,
            datamodel=datamodel,
            loglevel=loglevel,
        )

        # HACK convenience to get expanded url, xarray_kwargs and metadata for netcdf/zarr sources for intake2.
        # This provides direct access to the intake data object and is xarray-specific (moved here from
        # BackendIntake.__init__ so that non-xarray intake backends, e.g. FDB, do not inherit it).
        self.esmcat.data = self.esmcat.reader.kwargs["args"][0]
        self.esmcat.metadata = self.esmcat.reader.metadata
        self.esmcat.xarray_kwargs = self.esmcat._entry._captured_init_kwargs.get("args", {}).get("xarray_kwargs", {})

        # HACK: Manually expand globs to ensure xarray/intake2 always receives an explicit list of files.
        # This avoids issues where xarray fails on a list of glob strings or single globs in lists.
        url_input = to_list(self.esmcat.data.url)
        self.esmcat.data.url = sorted([f for x in url_input for f in glob(x)])
        self.logger.debug("Using url: %s", self.esmcat.data.url)

        # Manual safety check for netcdf sources (see #943), we output a more meaningful error message
        if not files_exist(self.esmcat.data.url):
            raise NoDataError(
                f"No NetCDF files available for {self.model} {self.exp} {self.source}, "
                + f"please check the url: {self.esmcat.data.url}"
            )

    def retrieve(
        self,
        var: str | list = None,
        level: str | list = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ):
        esmcat = self.esmcat
        read_kwargs = getattr(esmcat, "xarray_kwargs", {}).copy()

        # HACK: forcing to netcdf4 for intake2
        if isinstance(esmcat, intake_xarray.netcdf.NetCDFSource) and "engine" not in read_kwargs:
            read_kwargs.setdefault("engine", "netcdf4")

        esmcat = self._filter_netcdf_files(esmcat, filter_key="year", startdate=startdate, enddate=enddate)

        # The coder introduces the possibility to specify a time decoder for the time axis.
        # Default is set to DEFAULT_TIME_UNIT (microseconds) if not specified in the esmcat.xarray_kwargs
        if "time_coder" in esmcat.metadata:
            self.logger.info("Using custom pandas/xarray time coder: %s", esmcat.metadata["time_coder"])
            coder = xr.coders.CFDatetimeCoder(time_unit=esmcat.metadata["time_coder"])
        else:
            coder = xr.coders.CFDatetimeCoder(time_unit=DEFAULT_TIME_UNIT)

        esmcat.xarray_kwargs.update({"decode_times": coder})

        data = esmcat.reader.read(**read_kwargs)

        data = super()._postprocess_data(
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
        Filter the esmcat to include only netcdf files based on specific filter_key
        Args:
            esmcat (intake.catalog.Catalog): your catalog
            filter_key (str): type of filter to apply (default is "year")
            startdate (str): start date in format YYYY-MM-DD
            enddate (str): end date in format YYYY-MM-DD

        Returns:
            intake.catalog.Catalog: filtered catalog
        """

        # list available files in folder.
        files = to_list(esmcat.data.url)
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

    def _seldate(self, data: xr.Dataset, startdate: str = None, enddate: str = None):
        return super()._seldate(data=data, startdate=startdate, enddate=enddate)

    def _sellevel(self, data: xr.Dataset, level: str | list = None, level_coord: str = None):
        return super()._sellevel(data=data, level=level, level_coord=level_coord)

    def _selvar(self, data: xr.Dataset, var: str | list = None):
        return super()._selvar(data=data, var=var)
