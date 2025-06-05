"""Class for handling trend and detrending of xarray objects."""

import xarray as xr
from aqua.logger import log_configure, log_history


class Trender:
    """
    A class to handle trend and detrending of xarray objects using polynomial fitting.
    """

    def __init__(self, loglevel: str = 'WARNING'):
        """
        Initialize the Trender class with optional default settings.

        Args:
            loglevel (str): Logging level. Default is 'WARNING'.
        """
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, "Trender")

    def trend(self, data: xr.DataArray | xr.Dataset, dim: str = 'time',
              degree: int = 1, skipna: bool = False) -> xr.DataArray | xr.Dataset:
        """
        Estimate the trend of an xarray object using polynomial fitting.

        Args:
            data (DataArray or Dataset): The input data.
            dim (str): Dimension to apply trend along. Defaults to 'time'.
            degree (int): Degree of the polynomial. Defaults to 1.
            skipna (bool): Whether to skip NaNs. Defaults to False.

        Returns:
            DataArray or Dataset: The trend component.
        """
        return self._apply_trend_or_detrend(data, self._trend, dim, degree, skipna)

    def detrend(self, data: xr.DataArray | xr.Dataset, dim: str = 'time',
                degree: int = 1, skipna: bool = False) -> xr.DataArray | xr.Dataset:
        """
        Remove the trend from an xarray object using polynomial fitting.

        Args:
            data (DataArray or Dataset): The input data.
            dim (str): Dimension to apply detrend along. Defaults to 'time'.
            degree (int): Degree of the polynomial. Defaults to 1.
            skipna (bool): Whether to skip NaNs. Defaults to False.

        Returns:
            DataArray or Dataset: The detrended data.
        """
        return self._apply_trend_or_detrend(data, self._detrend, dim, degree, skipna)

    def _apply_trend_or_detrend(self, data, func, dim, degree, skipna):
        """
        Internal dispatcher for trend/detrend logic.
        """
        action = func.__name__.capitalize() # Get the action name (Trend or Detrend)
        
        self.logger.info(
            "Applying %s with polynomial of order %d along '%s' dimension.", action, degree, dim
        )

        if isinstance(data, xr.DataArray):
            final = func(data=data, dim=dim, degree=degree, skipna=skipna)

        elif isinstance(data, xr.Dataset):
            selected_vars = [da for da in data.data_vars if dim in data[da].coords]
            final = data[selected_vars].map(func, keep_attrs=True,
                                            dim=dim, degree=degree, skipna=skipna)
        else:
            raise ValueError("Input must be an xarray DataArray or Dataset.")

        return log_history(final, f"{action} with polynomial of order {degree} along '{dim}' dimension")

    def _trend(self, data: xr.DataArray, dim: str, degree: int, skipna: bool) -> xr.DataArray:
        """
        Compute the trend component using polynomial fit.
        Taken from https://ncar.github.io/esds/posts/2022/dask-debug-detrend/
        According to the post, current implementation is not the most efficient one.

        Args:
            data (DataArray): Input data.
            dim (str): Dimension to apply fit along.
            degree (int): Polynomial degree.
            skipna (bool): Whether to skip NaNs.

        Returns:
            DataArray: Trend component.

        """
        coeffs = data.polyfit(dim=dim, deg=degree, skipna=skipna)
        fit = xr.polyval(data[dim], coeffs.polyfit_coefficients)
        return fit

    def _detrend(self, data: xr.DataArray, dim: str, degree: int, skipna: bool) -> xr.DataArray:
        """
        Subtract the trend from the data.

        Args:
            data (DataArray): Input data.
            dim (str): Dimension to detrend along.
            degree (int): Polynomial degree.
            skipna (bool): Whether to skip NaNs.

        Returns:
            DataArray: Detrended data.
        """
        return data - self._trend(data, dim=dim, degree=degree, skipna=skipna)
