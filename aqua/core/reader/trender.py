"""Class for handling trend and detrending of xarray objects."""

import xarray as xr
import numpy as np
import pandas as pd
from aqua.core.logger import log_configure, log_history



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
    
    def coeffs(self, data: xr.DataArray | xr.Dataset, dim: str = 'time',
               degree: int = 1, skipna: bool = False, normalize: bool = False) -> xr.DataArray | xr.Dataset:
        """"
        Compute the polynomial coefficients for the trend.
        """

        return self._apply_trend_or_detrend(data, self._coeffs, dim, degree, skipna, normalize=normalize)

    def _coeffs(self, data: xr.DataArray | xr.Dataset, dim: str, degree: int, skipna: bool, normalize: bool) -> xr.DataArray | xr.Dataset:
        """
        Compute the polynomial coefficients for the trend, adjusted to the input data.

        Args:
            data (DataArray or Dataset): Input data.
            dim (str): Dimension to apply fit along.
            degree (int): Polynomial degree.
            skipna (bool): Whether to skip NaNs.
            normalize (bool): Whether to normalize coefficients for the 'time' dimension.
                              This applies a scaling factor based on the inferred frequency of the time dimension.
                              It is applied only if `dim` is 'time' and it is scaled to all degrees of the polynomial.

        Returns:
            DataArray or Dataset: Coefficients of the polynomial fit adjusted to the input data.
        """
        coeffs = data.polyfit(dim=dim, deg=degree, skipna=skipna)

        # keep consistency with datasets
        # coeffs.rename_vars({"polyfit_coefficients": data.name})

        # time axis are scaled to nanoseconds, which is not very user-friendly.
        # we try to adjust the coefficients to the input data frequency
        if dim == 'time' and normalize:
            self.logger.debug('Normalizing coefficients for time dimension.')
            # get the inferred frequency of the time dimension and convert to pandas offset
            time_values = data[dim].to_index()
            inferred_freq = time_values.inferred_freq
            self.logger.debug("Inferred frequency for 'time' dimension: %s", inferred_freq)
            if inferred_freq is None:
                raise ValueError("Inferred frequency for 'time' dimension is None. "
                                 "Ensure that the time dimension has a pandas compatible frequency.")
            offset = pd.tseries.frequencies.to_offset(inferred_freq)
            self.logger.debug("Offset for normalization: %s", offset)

            # offset cannot be converted to timedelta, so we use the mean of the time values
            delta = ((time_values + offset) - time_values).mean()
            factor = delta.value # convert to nanoscend

            self.logger.debug("Time normalization factor: %s", factor)

            # create an array of factor for each degree and convert to DataArray
            factor_scales = np.array([factor**(degree - i) for i in range(degree + 1)])
            self.logger.debug("Factor scales for polynomial degrees: %s", factor_scales)
            factor_da = xr.DataArray(factor_scales, dims="degree", coords={"degree": coeffs.coords["degree"]})

            # adjust the coefficients by the factor
            coeffs = coeffs * factor_da

        return coeffs.polyfit_coefficients

    def _apply_trend_or_detrend(self, data, func, dim, degree, skipna, **kwargs):
        """
        Internal dispatcher for trend/detrend logic.
        """
        action = func.__name__.capitalize() # Get the action name (Trend or Detrend)
        
        self.logger.info(
            "Applying %s with polynomial of order %d along '%s' dimension.", action, degree, dim
        )

        if isinstance(data, xr.DataArray):
            final = func(data=data, dim=dim, degree=degree, skipna=skipna, **kwargs)

        elif isinstance(data, xr.Dataset):
            selected_vars = [da for da in data.data_vars if dim in data[da].coords]
            final = data[selected_vars].map(func, keep_attrs=True,
                                            dim=dim, degree=degree, skipna=skipna, **kwargs)
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
        return xr.polyval(data[dim], coeffs.polyfit_coefficients)

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
