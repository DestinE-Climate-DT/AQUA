"""Backend realization using intake-xarray for data handling."""

from glob import glob

import intake_xarray
import xarray as xr

from aqua.core.configurer import ConfigPath
from aqua.core.data_model import DataModel
from aqua.core.exceptions import NoDataError
from aqua.core.fixer import Fixer
from aqua.core.util import default_time_unit, files_exist, to_list

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

        # TODO: Possible _filter_netcdf_files to be refined and placed here

        # The coder introduces the possibility to specify a time decoder for the time axis.
        # Default is set to default_time_unit (microseconds) if not specified in the esmcat.xarray_kwargs
        if "time_coder" in esmcat.metadata:
            self.logger.info("Using custom pandas/xarray time coder: %s", esmcat.metadata["time_coder"])
            coder = xr.coders.CFDatetimeCoder(time_unit=esmcat.metadata["time_coder"])
        else:
            coder = xr.coders.CFDatetimeCoder(time_unit=default_time_unit)

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

    def _seldate(self, data: xr.Dataset, startdate: str = None, enddate: str = None):
        return super()._seldate(data=data, startdate=startdate, enddate=enddate)

    def _sellevel(self, data: xr.Dataset, level: str | list = None, level_coord: str = None):
        return super()._sellevel(data=data, level=level, level_coord=level_coord)

    def _selvar(self, data: xr.Dataset, var: str | list = None):
        return super()._selvar(data=data, var=var)
