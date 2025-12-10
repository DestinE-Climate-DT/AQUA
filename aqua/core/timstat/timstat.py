"""Timmean mixin for the Reader class"""
import pandas as pd
import xarray as xr
import numpy as np
from functools import partial
from aqua.core.util import check_chunk_completeness, check_seasonal_chunk_completeness, frequency_string_to_pandas
from aqua.core.util import extract_literal_and_numeric
from aqua.core.logger import log_history, log_configure
from aqua.core.histogram import histogram


class TimStat():
    """
    Time statistic AQUA module
    """


    def __init__(self, loglevel='WARNING'):
        self.loglevel = loglevel
        self.orig_freq = None
        self.logger = log_configure(loglevel, 'TimStat')

    @property
    def AVAILABLE_STATS(self):
        """Return the list of available statistics."""
        return ['mean', 'std', 'max', 'min', 'sum', 'histogram']

    def timstat(self, data, stat='mean', freq=None, exclude_incomplete=False,
                time_bounds=False, center_time=False, func_kwargs={}, **kwargs):
        """
        Compute a time statistic on the input data. The statistic is computed over a time window defined by the frequency
        parameter. The frequency can be a string (e.g. '1D', '1M', '1Y', 'QS-DEC') or a pandas frequency object. The statistic can be
        'mean', 'std', 'max', 'min' or 'histogram'. The output is a new xarray dataset with the time dimension resampled to the desired
        frequency and the statistic computed over the time window.

        Args: 
            data (xarray.Dataset): Input data to compute the statistic on.
            stat (str, func): Statistic to compute. Can be a string in ['mean', 'std', 'max', 'min', 'histogram'] or a custom function.
            freq (str): Frequency to resample the data to. Can be a string (e.g. '1D', '1M', '1Y') or a pandas frequency object.
            exclude_incomplete (bool): If True, exclude incomplete chunks from the output.
            time_bounds (bool): If True, add time bounds to the output data.
            center_time (bool): If True, center the time axis of the output data.
            func_kwargs (dict): Additional keyword arguments to pass to the custom function if stat is callable.
            kwargs (dict): Additional keyword arguments to pass to the statistic function.

        Returns:
            xarray.Dataset: Output data with the required statistic computed at the desired frequency.
        """

        if isinstance(stat, str) and stat not in self.AVAILABLE_STATS:
            raise KeyError(f'{stat} is not a statistic supported by AQUA')

        if not isinstance(stat, str) and not callable(stat):
            raise TypeError('stat must be a string or a callable function')
        
        if stat == 'histogram':  # convert to callable function
            stat = histogram
        
        resample_freq = frequency_string_to_pandas(freq)

        # disabling all options if total averaging is selected
        if resample_freq is None:
            exclude_incomplete = False
            center_time = False
            time_bnds = False

        if 'time' not in data.dims:
            raise ValueError(f'Time dimension not found in the input data. Cannot compute tim{stat} statistic')

        # Get original frequency (for history)
        if len(data.time) > 1:
            time_values = pd.to_datetime(data['time'].values[:2])
            self.orig_freq = pd.tseries.frequencies.to_offset(time_values[1] - time_values[0])
            #orig_freq = (time_values[1] - time_values[0]).total_seconds() / 3600
            #self.orig_freq = round(orig_freq)
        else:
            # this block is likely never run, as the check for time dimension is done before
            self.logger.warning('A single timestep is available, is this correct?')
            self.orig_freq = 'Unknown'
            if exclude_incomplete:
                self.logger.warning('Disabling exclude incomplete since it cannot work if we have a single tstep!')
                exclude_incomplete = False

        # if we have a frequency
        if resample_freq is not None:
            try:
                # Resample to the desired frequency
                resample_data = data.resample(time=resample_freq)
            except ValueError as exc:
                raise ValueError(f'Cant find a frequency to resample, using resample_freq={resample_freq} not work, aborting!') from exc
            
        # if frequency is undefined, meaning that we operate on the entire set
        else:
            resample_data = data

        # compact call, equivalent of "out = resample_data.mean()""
        if isinstance(stat, str):  # we already checked if it is one of the allowable stats
            self.logger.info(f'Resampling to %s frequency and computing {stat}...', str(resample_freq))
            # use the kwargs to feed the time dimension to define the method and its options
            extra_kwargs = {} if resample_freq is not None else {'dim': 'time'}
            out = getattr(resample_data, stat)(**extra_kwargs)
        else:  # we can safely assume that it is a callable function now
            self.logger.info(f'Resampling to %s frequency and computing custom function...', str(resample_freq))
            if resample_freq is not None:
                out = resample_data.map(partial(stat, **func_kwargs, **kwargs))
            else:
                out = stat(resample_data, **func_kwargs, **kwargs)

        if exclude_incomplete and freq not in [None]:
            self.logger.info('Checking if incomplete chunks has been produced...')
            if 'Q' in resample_freq:
                boolean_mask = check_seasonal_chunk_completeness(data,
                                                                 resample_frequency=resample_freq,
                                                                 loglevel=self.loglevel)
            else:
                boolean_mask = check_chunk_completeness(data,
                                                        resample_frequency=resample_freq,
                                                        loglevel=self.loglevel)
            out = out.where(boolean_mask, drop=True)

        # Set time:
        # if not center_time as the first timestamp of each month/day according to the sampling frequency
        # if center_time as the middle timestamp of each month/day according to the sampling frequency
        if center_time:
            out = self.center_time_axis(out, resample_freq)

        # Check time is correct
        if resample_freq is not None:
            if np.any(np.isnat(out.time)):
                raise ValueError('Resampling cannot produce output for all frequency step, is your input data correct?')

        out = log_history(out, f"resampled from frequency {self.orig_freq} to frequency {freq} by AQUA tim{stat}")

        # Add a variable to create time_bounds
        if time_bounds:
            resampled = data.time.resample(time=resample_freq)
            time_bnds = xr.concat([resampled.min(), resampled.max()], dim='bnds', coords='different').transpose()
            time_bnds['time'] = out.time
            time_bnds.name = 'time_bnds'
            out = xr.merge([out, time_bnds])
            if np.any(np.isnat(out.time_bnds)):
                raise ValueError('Resampling cannot produce output for all time_bnds step!')
            log_history(out, f"time_bnds added by by AQUA tim{stat}")

        return out
    
    # this is not yet a great solution, but is more general than the previous one
    def center_time_axis(self, avg_data: xr.Dataset, resample_freq: str) -> xr.Dataset:
        """
        Move the time axis of the averaged data toward the center of the averaging period.

        Args:
            avg_data (xr.Dataset): The dataset with averaged data.
            resample_freq (str): The resampling frequency (e.g., '1D', '1M', '1Y').

        Returns:
            xr.Dataset: The dataset with the time axis centered.
        """

        literal, numeric = extract_literal_and_numeric(resample_freq)
        self.logger.debug('Frequency is %s with numeric part %s', literal, numeric)

        if literal in ["M", "Y", "ME", "YE"]:
            raise ValueError(f"Centering not implemented for frequency '{resample_freq}'")

        def average_datetimeindex(idx1: pd.DatetimeIndex, idx2: pd.DatetimeIndex) -> pd.DatetimeIndex:
            return pd.to_datetime((idx1.view("int64") + idx2.view("int64")) // 2)
        
        offset = pd.tseries.frequencies.to_offset(resample_freq)

        avg_data['time'] = average_datetimeindex(pd.to_datetime(avg_data['time']),
                              pd.to_datetime(avg_data['time']) + offset)
        
        return avg_data
