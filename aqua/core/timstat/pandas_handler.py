"""
Pandas/numpy datetime64 time handler.

This module handles all time operations for standard pandas datetime64 objects.
"""
import pandas as pd
import numpy as np
import xarray as xr
from aqua.core.util.time import check_chunk_completeness, check_seasonal_chunk_completeness


class PandasTimeHandler:
    """
    Time handler for pandas/numpy datetime64 objects.
    
    This handler uses pandas operations for all datetime manipulations,
    which is the default behavior for most climate datasets.
    """
    
    def to_datetime(self, values):
        """Convert values to pandas datetime."""
        return pd.to_datetime(values)
    
    def infer_freq(self, time_coord):
        """
        Infer frequency using pandas infer_freq.
        
        Args:
            time_coord: xarray time coordinate
            
        Returns:
            pandas frequency offset object
        """
        time_values = pd.to_datetime(time_coord.values[:2])
        return pd.tseries.frequencies.to_offset(time_values[1] - time_values[0])
    
    def create_date_range(self, start, end, freq):
        """Create date range using pandas.date_range."""
        return pd.date_range(start=start, end=end, freq=freq)
    
    def average_datetimes(self, dt1, dt2):
        """
        Average two pandas datetime objects or DatetimeIndexes.
        
        Args:
            dt1: First datetime or DatetimeIndex
            dt2: Second datetime or DatetimeIndex
            
        Returns:
            Averaged datetime(s)
        """
        # Convert to DatetimeIndex if needed for uniform handling
        if not isinstance(dt1, pd.DatetimeIndex):
            dt1 = pd.to_datetime([dt1])
            dt2 = pd.to_datetime([dt2])
            return pd.to_datetime((dt1.view("int64") + dt2.view("int64")) // 2)[0]
        return pd.to_datetime((dt1.view("int64") + dt2.view("int64")) // 2)
    
    def add_offset(self, dt, offset):
        """Add pandas offset to datetime."""
        return dt + offset
    
    def center_time_axis(self, avg_data: xr.Dataset, resample_freq: str) -> xr.Dataset:
        """
        Move the time axis toward the center of the averaging period.
        
        Args:
            avg_data: Dataset with time coordinate to center
            resample_freq: Resampling frequency string (e.g., '1D', '5D', '1MS')
            
        Returns:
            Dataset with centered time axis
        """
        # Convert resample_freq string to offset
        offset = pd.tseries.frequencies.to_offset(resample_freq)

        # Get current time as DatetimeIndex
        time_idx = pd.to_datetime(avg_data['time'])

        # Calculate end time by adding offset
        time_idx_end = time_idx + offset

        # Average the two to get center
        centered_time = self.average_datetimes(time_idx, time_idx_end)

        # Update the dataset
        avg_data['time'] = centered_time

        return avg_data
    
    def has_nat(self, time_coord):
        """
        Check if time coordinate contains NaT (Not a Time) values.
        
        Args:
            time_coord: xarray time coordinate
            
        Returns:
            bool: True if any NaT values present
        """
        return bool(np.any(np.isnat(time_coord)))
    
    def check_chunk_completeness(self, xdataset, resample_frequency='1D', loglevel='WARNING'):
        """
        Verify that all chunks in a dataset are complete for the given resample frequency.
        
        Args:
            xdataset: The original dataset before averaging
            resample_frequency: Frequency for resampling (pandas frequency string)
            loglevel: Logging level
            
        Returns:
            xr.DataArray: Boolean mask (True for complete chunks, False for incomplete)
        """
        return check_chunk_completeness(xdataset, resample_frequency, loglevel)
    
    def check_seasonal_chunk_completeness(self, xdataset, resample_frequency='QS-DEC', loglevel='WARNING'):
        """
        Verify that all seasonal (quarterly) chunks have complete months.
        
        Args:
            xdataset: The original dataset before averaging
            resample_frequency: Seasonal frequency (e.g., 'QS-DEC')
            loglevel: Logging level
            
        Returns:
            xr.DataArray: Boolean mask (True for complete quarters, False for incomplete)
        """
        return check_seasonal_chunk_completeness(xdataset, resample_frequency, loglevel)
