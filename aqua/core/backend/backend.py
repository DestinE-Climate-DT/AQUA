from abc import ABC, abstractmethod

import dask.array as da
import xarray as xr
from smmregrid import GridInspector

from aqua.core.data_model import DataModel
from aqua.core.data_model.coordidentifier import CoordIdentifier
from aqua.core.fixer import Fixer
from aqua.core.logger import log_configure
from aqua.core.util import fix_calendar, set_attrs, to_list


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
    ) -> xr.Dataset:
        """Open data, apply filters, return xr.Dataset."""

    @abstractmethod
    def retrieve_plain(self, startdate: str = None) -> xr.Dataset:
        """Open minimal data to fetch the Regridder init."""

    @abstractmethod
    def log_history(self, data: xr.Dataset) -> xr.Dataset:
        """Log a message in the dataset's history attribute."""

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
            # Time threatment: we want to ensure that time is always in Gregorian calendar
            # and to change the default numpy datetime64 resolution to microseconds
            if "time" in data.coords:
                # Fix the calendar to Gregorian if needed
                data = fix_calendar(data, loglevel=self.loglevel)
        return data

    def _set_metadata(self, data: xr.Dataset, metadata: dict) -> xr.Dataset:
        """Set AQUA-specific metadata on the dataset."""
        aqua_metadata = {f"AQUA_{key}": str(value) for key, value in metadata.items()}
        data = set_attrs(data, aqua_metadata)
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
            self.logger.error("Dataset is not a dask-backed array. Please verify the data access.")

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

        # get the minimal sample data based on the extracted time dimension and variables
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

            # safety check for long time chunks, if present log a warning
            # assume time is the first dimension
            for var in data.data_vars:
                chunks = data[var].encoding.get("chunks", None)
                if chunks and chunks[0] > 1:
                    self.logger.warning(
                        "Variable %s has time chunks: %s. This might slowdown areas/weights generation", var, chunks[0]
                    )

        # check if variables, coords and dimensions are still present after selection, if not log a warning
        if not data.data_vars or not data.coords or 0 in data.sizes.values():
            self.logger.error("No data available after applying _select_minimum_sample selections.")
        return data

    def _seldate(self, data: xr.Dataset, startdate: str = None, enddate: str = None):
        """Store date bounds for lazy application."""
        return data.sel(time=slice(startdate, enddate))

    def _sellevel(
        self,
        data: xr.Dataset,
        level: str | list = None,
        level_coord: str = None,
        vertical_coords: list = ["isobaric", "depth", "height"],
    ):
        """
        Vertical level selection based on provided level(s) and optional vertical coordinate name.
        Args:
            data (xr.Dataset): The input dataset to be processed.
            level (str | list, optional): Level(s) to select. Defaults to None.
            level_coord (str, optional): Name of the vertical coordinate. Defaults to None.
            vertical_coords (list, optional): List of recognized vertical coordinate from data model.
                                              Defaults to ["isobaric", "depth", "height"].
        """
        if level is None:
            self.logger.error("No level(s) provided, no vertical selection applied.")
            return data

        if level_coord:
            if level_coord not in data.coords:
                self.logger.error(
                    "Specified vertical coordinate %s not found in data! No vertical selection will be applied.", level_coord
                )
                return data
            select_coord = to_list(level_coord)

        else:
            # use data model to identify vertical coordinates (isobaric, depth and height)
            # TODO: modify data model so that this info is available without instantiating the data model class
            coords = CoordIdentifier(data.coords).identify_coords()
            data_level_coord = [y["name"] for x, y in coords.items() if y is not None and x in vertical_coords]

            # return if no vertical coordinate is found
            if not data_level_coord:
                self.logger.error(
                    "Levels selected but no level coordinate found in data! "
                    "Try specifying the coordinate with the 'level_coord' argument."
                )
                return data

            select_coord = to_list(data_level_coord)

        # error for multiple vertical coordinates found or provided
        if len(select_coord) > 1:
            self.logger.error("Multiple vertical coordinates found in data: %s. No selection will be applied", select_coord)
            return data

        # pick only one coordinate
        select_coord = select_coord[0]

        # ensure that level is a list
        level = to_list(level)

        # check if levels are among the values in the coordinate
        available = data[select_coord].values
        missing = [lev for lev in level if lev not in available]
        if missing:
            self.logger.error(
                "Levels %s not found in vertical coordinate %s! Returning unfiltered data!", missing, select_coord
            )
            return data

        # return selection of any levels that are available in the coordinate
        self.logger.debug("Selecting vertical coordinate %s = %s", select_coord, level)
        return data.sel(**{select_coord: level})

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

    # @staticmethod
    # def is_dask(dataset: xr.Dataset) -> bool:
    #    """Verify that the dataset is backed by dask arrays."""
    #    return bool(dataset.chunks)
    @staticmethod
    def is_dask(dataset: xr.Dataset | xr.DataArray) -> bool:
        """Verify that the xarray object is backed by dask arrays."""
        if isinstance(dataset, xr.Dataset):
            return any(isinstance(var.data, da.Array) for var in dataset.variables.values())
        if isinstance(dataset, xr.DataArray):
            return isinstance(dataset.data, da.Array)
        return False
