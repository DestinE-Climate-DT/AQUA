from abc import ABC, abstractmethod

import xarray as xr
from smmregrid import GridInspector

from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer
from aqua.core.logger import log_configure
from aqua.core.util import find_vert_coord, to_list


class Backend(ABC):
    """
    Abstract base class for all data backends in AQUA.
    Defines the mandatory interface that every concrete backend must implement.
    """

    def __init__(self, fixer: Fixer = None, datamodel: DataModel = None, loglevel: str = "WARNING"):
        """
        Initialize the backend with a logger and default attributes.

        Args:
            fixer (Fixer, optional): An instance of Fixer to apply data fixes. Defaults to None.
            datamodel (DataModel, optional): An instance of DataModel to define the data structure. Defaults to None.
            loglevel (str): Logging level for the backend logger. Default is 'WARNING'.
        """
        self.fixer = fixer
        self.datamodel = datamodel
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name=self.__class__.__name__)

    @abstractmethod
    def retrieve(
        self,
        var: str | list = None,
        level: str | list = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ):
        """Open data, apply filters, return xr.Dataset."""

    @abstractmethod
    def retrieve_plain(self, startdate: str = None):
        """Open minimal data to fetch the Regridder init."""

    def _fixer_and_datamodel(self, data: xr.Dataset, var: str | list = None) -> xr.Dataset:
        """
        Apply fixer and datamodel transformations to the dataset.

        Args:
            data (xr.Dataset): The input dataset to be processed.
            var (str | list, optional): Variable(s) to apply fixes to. Defaults to None.
        """
        # Apply the fixer first and the datamodel as second.
        # The Fixer expects destvar as a list (or None for "fix all"), so coerce a bare string.
        if self.fixer:
            self.logger.debug("Applying variable fixes")
            data = self.fixer.fixer(data, to_list(var))
            data = self.fixer.fixerdatamodel.apply(data)
        if self.datamodel:
            self.logger.debug("Applying data model")
            data = self.datamodel.apply(data)
        return data

    def _postprocess_data(
        self,
        data: xr.Dataset,
        var: str | list = None,
        level: str | list = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ):
        """
        Apply post-processing steps to the dataset, variable selection,
        fixing and data model application, date selection, and level selection
        """
        if not self.is_dask(data):
            self.logger.warning("Dataset is not a dask-backed array.")

        data = self._fixer_and_datamodel(data, var=var)

        if var:
            data = self._selvar(data=data, var=var)
        if startdate or enddate:
            data = self._seldate(data=data, startdate=startdate, enddate=enddate)
        if level:
            data = self._sellevel(data=data, level=level, level_coord=level_coord)

        return data

    def _select_minimum_sample(self, data, startdate: str = None):
        """
        Use smmregrid GridInspector to get minimum sample data.
        If a startdate is provided, it will select the nearest time step to that date,
        otherwise it will select the first time step available in the dataset.
        Variable selection is done based on the minimal set of variables across all smmregrid gridtypes.

        Args:
            data (xarray.Dataset): input data
            startdate (str, optional): Startdate for time selection. Defaults to None.

        Returns:
            A xarray.Dataset containing the required miminal sample data.
        """

        # get gridtypes from smmregrid
        gridinspect = GridInspector(data, loglevel=self.loglevel)
        gridtypes = gridinspect.get_gridtype()

        # extract the time dimension and variables
        time_dimension = gridinspect.get_gridtype_attr(gridtypes, "time_dims")

        # extract the minimal set of variables across all the gridtypes using smmregrid feature
        minimal_variables = gridinspect.get_gridtype_sample_variable(gridtypes)

        if minimal_variables:
            self.logger.debug("Selecting variables %s for _retrieve_plain", minimal_variables)
            data = data[minimal_variables]
        if time_dimension:
            self.logger.debug("Time dimensions found: %s", time_dimension)
            if startdate:
                self.logger.debug("Selecting startdate: %s", startdate)
                data = data.sel({time_dimension[0]: startdate}, method="nearest")
            else:
                data = data.isel({time_dimension[0]: 0})
        # check if variables, coords and dimensions are still present after selection, if not log a warning
        if not data.data_vars or not data.coords or 0 in data.sizes.values():
            self.logger.warning("No data available after applying _select_minimum_sample selections.")
        return data

    def _seldate(self, data: xr.Dataset, startdate: str = None, enddate: str = None):
        """Store date bounds for lazy application."""
        return data.sel(time=slice(startdate, enddate))

    def _sellevel(self, data: xr.Dataset, level: str | list = None, level_coord: str = None):
        """Store level selection for lazy application."""
        # find the vertical coordinate, which can be the smmregrid one or
        # any other with a dimension compatible (Pa, cm, etc)
        full_vert_coord = find_vert_coord(data)

        if level_coord:
            if level_coord not in full_vert_coord:
                self.logger.error("Specified vertical coordinate %s not found in data!", level_coord)
                return data
            full_vert_coord = [level_coord]

        # return if no vertical coordinate is found
        if not full_vert_coord:
            self.logger.error("Levels selected but no vertical coordinate found in data!")
            return data

        # ensure that level is a list
        level = to_list(level)

        # do the selection on the first vertical coordinate found
        if len(full_vert_coord) > 1:
            self.logger.error("Found more than one vertical coordinate, using the first one: %s", full_vert_coord[0])
            self.logger.error("You can specify the vertical coordinate to use with the 'level_coord' argument.")

        # check if levels are among the values in the coordinate
        if not all(l in data[full_vert_coord[0]].values for l in level):
            self.logger.error("Levels %s not found in vertical coordinate %s!", level, full_vert_coord[0])
        else:
            self.logger.debug("Selecting vertical coordinate %s = %s", full_vert_coord[0], level)
            data = data.sel(**{full_vert_coord[0]: level})
            # data = log_history(data, f"Selecting levels {level} from vertical coordinate {full_vert_coord[0]}")

        return data

    def _selvar(self, data: xr.Dataset, var: str | list = None):
        if isinstance(var, str):
            var = str(var).split()
        self.logger.info("Retrieving variables: %s", var)
        # Conversion to list guarantees that a Dataset is produced
        var = to_list(var)

        matched_var = [element for element in var if element in data.data_vars]

        if matched_var == var:
            data = data[var]
        else:
            missing_var = [element for element in var if element not in data.data_vars]
            self.logger.warning("The following requested variables were not found in the dataset: %s", missing_var)
            if matched_var:
                self.logger.warning("Retrieving available variables: %s", matched_var)
                data = data[matched_var]
            else:
                self.logger.error("None of the requested variables %s were found in the dataset.", var)
                return xr.Dataset()  # Return an empty Dataset if no variables match

        return data

    @staticmethod
    def is_dask(dataset: xr.Dataset) -> bool:
        """Verify that the dataset is backed by dask arrays."""
        return bool(dataset.chunks)
