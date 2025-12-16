"""
CFTime time handler for non-standard calendars.

This module handles all time operations for cftime datetime objects,
supporting various non-standard calendars like noleap, 360_day, etc.
"""
import numpy as np
import xarray as xr
import cftime
from pandas.tseries.frequencies import to_offset
from aqua.core.logger import log_configure
from aqua.core.util import xarray_to_pandas_freq

class CFTimeHandler:
    """
    Time handler for cftime datetime objects.
    
    This handler works with non-standard calendars such as noleap (365_day),
    360_day, all_leap (366_day), and others defined by CFTime.
    """
    
    def infer_freq(self, time_coord):
        """
        Infer frequency from CFTime coordinate.
        
        Args:
            time_coord: xarray time coordinate with cftime values
            
        Returns:
            pandas frequency offset object
        """
        if len(time_coord) < 2:
            raise ValueError("Need at least 2 time values to infer frequency")
        
        dt0 = time_coord.values[0]
        dt1 = time_coord.values[1]
        
        # Calculate difference
        delta_days = (dt1 - dt0).days
        delta_seconds = (dt1 - dt0).seconds
        
        # Determine frequency based on delta
        if delta_seconds > 0 and delta_days == 0:
            # Sub-daily frequency
            hours = delta_seconds // 3600
            if hours >= 1:
                return to_offset(f'{hours}h')
            else:
                minutes = delta_seconds // 60
                return to_offset(f'{minutes}min')
        
        # Daily or longer
        if delta_days == 1:
            return to_offset('D')
        elif 28 <= delta_days <= 31:
            # Monthly frequency
            return to_offset('MS')
        elif 89 <= delta_days <= 92:
            # Quarterly frequency  
            return to_offset('QS')
        elif 365 <= delta_days <= 366:
            # Yearly frequency
            return to_offset('YS')
        else:
            # Multi-day frequency
            return to_offset(f'{delta_days}D')
    
    def average_datetimes(self, dt1, dt2):
        """
        Average two cftime datetime objects or arrays.
        
        Args:
            dt1: First cftime datetime or array
            dt2: Second cftime datetime or array
            
        Returns:
            Averaged cftime datetime(s)
        """
        # Handle scalar case
        if isinstance(dt1, cftime.datetime):
            # Convert to numpy datetime64 for averaging
            num1 = cftime.date2num(dt1, units='days since 1850-01-01', calendar=dt1.calendar)
            num2 = cftime.date2num(dt2, units='days since 1850-01-01', calendar=dt2.calendar)
            avg_num = (num1 + num2) / 2
            return cftime.num2date(avg_num, units='days since 1850-01-01', calendar=dt1.calendar)
        
        # Handle array case
        if len(dt1) == 0:
            return dt1
        
        calendar = dt1[0].calendar
        num1 = cftime.date2num(dt1, units='days since 1850-01-01', calendar=calendar)
        num2 = cftime.date2num(dt2, units='days since 1850-01-01', calendar=calendar)
        avg_num = (num1 + num2) / 2
        return cftime.num2date(avg_num, units='days since 1850-01-01', calendar=calendar)
    
    def add_offset(self, dt, offset):
        """
        Add pandas offset to cftime datetime(s).
        
        Args:
            dt: cftime datetime or array of datetimes
            offset: pandas offset object
            
        Returns:
            cftime datetime(s) with offset added
        """
        # Handle scalar case
        if isinstance(dt, cftime.datetime):
            calendar = dt.calendar
            
            # Convert to numeric, add offset, convert back
            base_date = cftime.datetime(1850, 1, 1, calendar=calendar)
            days_since = (dt - base_date).days + (dt - base_date).seconds / 86400.0
            
            # Get offset in days
            if hasattr(offset, 'delta'):
                offset_days = offset.delta.total_seconds() / 86400.0
            elif hasattr(offset, 'n'):
                # For month/year offsets, approximate
                if 'M' in str(offset):
                    offset_days = offset.n * 30  # Approximate month
                elif 'Y' in str(offset):
                    offset_days = offset.n * 365  # Approximate year
                else:
                    offset_days = offset.n
            else:
                offset_days = 0
            
            new_days = days_since + offset_days
            return cftime.num2date(new_days, units='days since 1850-01-01', calendar=calendar)
        
        # Handle array case
        if len(dt) == 0:
            return dt
        
        calendar = dt[0].calendar
        result = []
        for d in dt:
            result.append(self.add_offset(d, offset))
        return np.array(result)
    
    def center_time_axis(self, avg_data: xr.Dataset, resample_freq: str) -> xr.Dataset:
        """
        Move the time axis toward the center of the averaging period.
        
        Args:
            avg_data: Dataset with time coordinate to center
            resample_freq: Resampling frequency string (e.g., '1D', '5D', '1MS')
            
        Returns:
            Dataset with centered time axis
        """
        offset = to_offset(resample_freq)
        time_values = avg_data['time'].values
        
        if len(time_values) == 0:
            return avg_data
        
        calendar = time_values[0].calendar
        
        # For monthly/yearly frequencies, handle variable-length periods
        if 'MS' in resample_freq or 'YS' in resample_freq:
            centered_times = []
            for t in time_values:
                # Get start of period (already have it)
                start = t
                
                # Calculate end by adding offset
                if 'MS' in resample_freq:
                    # Monthly: go to next month
                    if t.month == 12:
                        end = cftime.datetime(t.year + 1, 1, 1, calendar=calendar)
                    else:
                        end = cftime.datetime(t.year, t.month + 1, 1, calendar=calendar)
                elif 'YS' in resample_freq:
                    # Yearly: go to next year
                    end = cftime.datetime(t.year + 1, 1, 1, calendar=calendar)
                else:
                    # Fallback to numeric addition
                    end = self.add_offset(start, offset)
                
                # Average start and end
                centered = self.average_datetimes(start, end)
                centered_times.append(centered)
            
            avg_data['time'] = np.array(centered_times)
        else:
            # For fixed-length periods (days, hours), use add_offset
            time_end = self.add_offset(time_values, offset)
            centered_time = self.average_datetimes(time_values, time_end)
            avg_data['time'] = centered_time
        
        return avg_data
    
    def has_nat(self, time_coord):
        """
        Check if time coordinate contains NaT values.
        
        For CFTime, NaT is not a standard concept, so we check for None or invalid dates.
        
        Args:
            time_coord: xarray time coordinate
            
        Returns:
            bool: True if any invalid values present
        """
        try:
            # Check if any values are None
            return any(t is None for t in time_coord.values)
        except (TypeError, AttributeError):
            return False
    
    def check_chunk_completeness(self, xdataset, resample_frequency='1D', loglevel='WARNING'):
        """
        Verify that all chunks in a CFTime dataset are complete.
        
        Args:
            xdataset: The original dataset with CFTime coordinate
            resample_frequency: Frequency for resampling
            loglevel: Logging level
            
        Returns:
            xr.DataArray: Boolean mask (True for complete chunks, False for incomplete)
        """
        logger = log_configure(loglevel, 'timmean_cftime_chunk_completeness')
        
        # Get data frequency
        data_frequency = xarray_to_pandas_freq(xdataset)
        logger.debug('Data frequency detected as: %s', data_frequency)
        
        # Get unique chunk starts using resample
        taxis = xdataset.time.resample(time=resample_frequency).mean()
        chunks = taxis.time.values
        
        logger.info('%s chunks from %s to %s at %s frequency to be analysed',
                   len(chunks), chunks[0], chunks[-1], resample_frequency)
        
        if len(chunks) == 0:
            raise ValueError(f'No chunks! Cannot compute average on {resample_frequency} period')
        
        check_completeness = []
        
        for chunk_start in chunks:
            # Calculate expected end date
            end_date = self._find_end_date_cftime(chunk_start, resample_frequency)
            logger.debug('Processing chunk from %s to %s', chunk_start, end_date)
            
            # Generate expected time series
            expected_timeseries = self._generate_expected_time_series_cftime(
                chunk_start, data_frequency, resample_frequency)
            expected_len = len(expected_timeseries)
            
            # Get effective length
            effective_len = len(xdataset.time[(xdataset['time'] >= chunk_start) & 
                                             (xdataset['time'] < end_date)])
            logger.debug('Expected: %s, Effective: %s', expected_len, effective_len)
            
            if expected_len == effective_len:
                check_completeness.append(True)
            else:
                logger.warning('Chunk %s->%s has %s elements instead of %s, excluding',
                             expected_timeseries[0], expected_timeseries[-1],
                             effective_len, expected_len)
                check_completeness.append(False)
        
        if sum(check_completeness) == 0:
            logger.warning('No complete chunks for %s period, returning empty array',
                         resample_frequency)
        
        return xr.DataArray(check_completeness, dims=('time',), coords={'time': taxis.time})
    
    def check_seasonal_chunk_completeness(self, xdataset, resample_frequency='QS-DEC', loglevel='WARNING'):
        """Not yet implemented for CFTime."""
        raise NotImplementedError("Seasonal chunk completeness not yet implemented for CFTime")
    
    def _find_end_date_cftime(self, start_date, offset_str):
        """
        Find end date for a CFTime chunk.
        
        Args:
            start_date: cftime datetime
            offset_str: Frequency string (e.g., '1D', '1MS')
            
        Returns:
            cftime datetime representing end of period
        """
        calendar = start_date.calendar
        
        # Parse offset string
        if 'D' in offset_str:
            # Use xr.cftime_range to get end date
            dates = xr.cftime_range(start=start_date, periods=2, freq=offset_str, calendar=calendar)
            return dates[-1]
        elif 'MS' in offset_str:
            # Monthly offset
            months = int(offset_str.replace('MS', '') or '1')
            new_month = start_date.month + months
            new_year = start_date.year + (new_month - 1) // 12
            new_month = ((new_month - 1) % 12) + 1
            return cftime.datetime(new_year, new_month, 1, calendar=calendar)
        elif 'YS' in offset_str:
            # Yearly offset
            years = int(offset_str.replace('YS', '') or '1')
            return cftime.datetime(start_date.year + years, 1, 1, calendar=calendar)
        elif 'h' in offset_str or 'H' in offset_str:
            # Hourly offset
            dates = xr.cftime_range(start=start_date, periods=2, freq=offset_str, calendar=calendar)
            return dates[-1]
        else:
            # Generic case: use xr.cftime_range
            dates = xr.cftime_range(start=start_date, periods=2, freq=offset_str, calendar=calendar)
            return dates[-1]
    
    def _generate_expected_time_series_cftime(self, start_date, frequency, time_period):
        """
        Generate expected time series for a CFTime chunk.
        
        Args:
            start_date: cftime datetime start of chunk
            frequency: Data frequency string
            time_period: Aggregation period string
            
        Returns:
            array of cftime datetimes
        """
        calendar = start_date.calendar
        end_date = self._find_end_date_cftime(start_date, time_period)
        
        # Generate time series using xr.cftime_range
        try:
            time_series = xr.cftime_range(start=start_date, end=end_date, 
                                         freq=frequency, calendar=calendar)
            # Exclude end date (half-open interval)
            time_series = [t for t in time_series if t < end_date]
            return np.array(time_series)
        except Exception as e:
            logger = log_configure('DEBUG', 'cftime_expected_series')
            logger.warning('Could not generate expected series: %s', e)
            return np.array([])
