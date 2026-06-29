"""Backend realization using xarray for data handling."""

import xarray as xr

from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer

from .backend import Backend


class BackendXarray(Backend):
    """
    Concrete retriever that opens data directly with xarray from a path.
    No intake catalog is involved. Supports netCDF and zarr.
    """

    SUPPORTED_FORMATS = ("netcdf", "zarr")

    def __init__(
        self,
        path: str,
        format: str = None,
        chunks: str | dict = "auto",
        fixer: Fixer = None,
        datamodel: DataModel = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        """
        Initialize the BackendXarray instance.

        Args:
            path (str): Path to the data file or directory.
            format (str, optional): Format of the data file. If None, it will be detected automatically.
            chunks (str | dict, optional): Chunking strategy for xarray. Defaults to "auto".
            fixer (Fixer, optional): An instance of Fixer to apply data fixes. Defaults to None.
            datamodel (DataModel, optional): An instance of DataModel to define the data structure. Defaults to None.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
            kwargs: Additional keyword arguments to pass to xarray's open_dataset or open_zarr functions.
        """

        super().__init__(fixer=fixer, datamodel=datamodel, loglevel=loglevel)
        detected_format = self._detect_format(path)
        format = format or detected_format

        if format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"format must be one of {self.SUPPORTED_FORMATS}, got {format!r}")
        self.path = path
        self.format = format
        self.chunks = chunks
        self.xr_kwargs = kwargs

    def retrieve(
        self,
        var: str | list = None,
        level: str | list = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ):

        if self.format == "netcdf":
            data = xr.open_mfdataset(self.path, chunks=self.chunks, **self.xr_kwargs)
        elif self.format == "zarr":
            data = xr.open_zarr(self.path, chunks=self.chunks, **self.xr_kwargs)

        # Apply the fixer first and the datamodel as second
        if self.fixer:
            self.logger.debug("Applying variable fixes")
            data = self.fixer.fixer(data, var)
            data = self.fixer.fixerdatamodel.apply(data)
        if self.datamodel:
            self.logger.debug("Applying data model")
            data = self.datamodel.apply(data)

        if var:
            data = self._selvar(data=data, var=var)
        if startdate or enddate:
            data = self._seldate(data=data, startdate=startdate, enddate=enddate)
        if level:
            data = self._sellevel(data=data, level=level, level_coord=level_coord)

        return data

    def _seldate(self, data: xr.Dataset, startdate: str = None, enddate: str = None):
        return super()._seldate(data=data, startdate=startdate, enddate=enddate)

    def _sellevel(self, data: xr.Dataset, level: str | list = None, level_coord: str = None):
        return super()._sellevel(data=data, level=level, level_coord=level_coord)

    def _selvar(
        self,
        data: xr.Dataset,
        var: str | list = None,
    ):
        return super()._selvar(data=data, var=var)

    @staticmethod
    def _detect_format(path):
        """
        Detect the format of the data file based on its extension.

        Args:
            path (str): Path to the data file or directory.
        """
        # TODO: expand with glob to detect that are also all files of the same format
        # Raise NoDataError if the expanded glob does not contain any files of the same format
        if path.endswith(".zarr"):
            return "zarr"
        elif path.endswith(".nc") or path.endswith(".nc4"):
            return "netcdf"
        else:
            raise ValueError(f"Could not detect format from path: {path}")
