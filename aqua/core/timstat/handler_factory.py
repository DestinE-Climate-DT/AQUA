"""
Time handler factory and base interfaces.

This module provides the factory for detecting and creating appropriate time handlers.
"""
import numpy as np
import xarray as xr
import cftime


class TimeHandlerFactory:
    """
    Factory class for creating appropriate time handlers.

    Automatically detects the datetime type in xarray data and returns
    the corresponding handler (PandasTimeHandler or CFTimeHandler).
    """

    @staticmethod
    def detect_time_type(data: xr.Dataset) -> str:
        """
        Detect whether the dataset uses pandas datetime or CFTime.

        Args:
            data: xarray Dataset with time coordinate

        Returns:
            'pandas' or 'cftime' string

        Raises:
            ValueError: If no time coordinate found or type cannot be determined
        """
        if 'time' not in data.coords:
            raise ValueError("No 'time' coordinate found in dataset")

        # Get the time coordinate variable
        time_var = data.time

        if np.issubdtype(time_var.dtype, np.datetime64):
            return 'pandas'

        if np.issubdtype(time_var.dtype, np.object_):
            first_val = time_var.values[0]
            if isinstance(first_val, cftime.datetime) and hasattr(
                    first_val, 'calendar'):
                return 'cftime'

        raise ValueError("Unable to determine time type from dataset")

    @staticmethod
    def get_handler(data: xr.Dataset):
        """
        Get the appropriate time handler for the dataset.

        Args:
            data: xarray Dataset with time coordinate

        Returns:
            PandasTimeHandler or CFTimeHandler instance

        Raises:
            ValueError: If time type cannot be determined
        """
        from .pandas_handler import PandasTimeHandler
        from .cftime_handler import CFTimeHandler

        time_type = TimeHandlerFactory.detect_time_type(data)

        if time_type == 'pandas':
            return PandasTimeHandler()
        if time_type == 'cftime':
            return CFTimeHandler()
        raise ValueError(f"Unknown time type: {time_type}")
