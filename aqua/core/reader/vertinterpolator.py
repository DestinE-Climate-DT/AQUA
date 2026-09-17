"""Simple module for vertical interpolation for AQUA."""

import xarray as xr

from aqua.core.logger import log_configure, log_history


class VertInterpolator:
    """A class to perform vertical interpolation on xarray objects."""

    def __init__(self, loglevel: str = "WARNING"):
        """
        Initialize the VertInterpolator class with optional default settings.

        Args:
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, "VertInterpolator")

    def vertinterp(self, data, levels=None, level_coord="plev", units=None, method="linear"):
        """
        A basic vertical interpolation based on interp function
        of xarray within AQUA. Given an xarray object, will interpolate the
        vertical dimension along the level_coord.
        If it is a Dataset, only variables with the required vertical
        coordinate will be interpolated.

        Args:
            data (DataArray, Dataset): your dataset
            levels (float, or list): The level you want to interpolate the vertical coordinate
            units (str, optional, ): The units of your vertical axis. Default 'Pa'
            level_coord (str, optional): The name of the vertical coordinate. Default 'plev'
            method (str, optional): The type of interpolation method supported by interp()

        Return
            A DataArray or a Dataset with the new interpolated vertical dimension
        """

        if levels is None:
            raise KeyError("Levels for interpolation must be specified")

        # error if level_coord is not there
        if level_coord not in data.coords:
            raise KeyError(f"The level_coord={level_coord} is not in the data!")

        # if you not specified the units, guessing from the data
        if units is None:
            if hasattr(data[level_coord], "units"):
                self.logger.warning("Units of level_coord=%s has not defined, reading from the data", level_coord)
                units = data[level_coord].units
            else:
                raise ValueError("Original dataset has not unit on the vertical axis, failing!")

        if isinstance(data, xr.DataArray):
            final = self._vertinterp(data=data, levels=levels, units=units, level_coord=level_coord, method=method)

        elif isinstance(data, xr.Dataset):
            selected_vars = [da for da in data.data_vars if level_coord in data[da].coords]
            final = data[selected_vars].map(
                self._vertinterp, keep_attrs=True, levels=levels, units=units, level_coord=level_coord, method=method
            )
        else:
            raise ValueError("This is not an xarray object!")

        final = log_history(
            final,
            f"Interpolated from original levels {data[level_coord].values} "
            f"{data[level_coord].units} to level {levels} using {method} method.",
        )

        return final

    def _vertinterp(self, data, levels=None, units="Pa", level_coord="plev", method="linear"):

        # verify units are good
        if data[level_coord].units != units:
            self.logger.warning("Converting level_coord units to interpolate from %s to %s", data[level_coord].units, units)
            data = data.metpy.convert_coordinate_units(level_coord, units)

        # very simple interpolation
        final = data.interp({level_coord: levels}, method=method)

        return final
