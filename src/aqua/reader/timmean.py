"""Timmean mixin for the Reader class"""
import types
import pandas as pd
import xarray as xr
import numpy as np
from aqua.util import check_chunk_completeness, frequency_string_to_pandas
from aqua.util import extract_literal_and_numeric
from aqua.logger import log_history


class TimmeanMixin():

    def timmean(self, data, freq=None, exclude_incomplete=False,
                time_bounds=False, center_time=False):
        """
        Perform daily, monthly and yearly averaging.
        Wrapper for _timmean function, which is called differently if data is a generator or not.

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
            freq (str):         the frequency of the time averaging.
                                Valid values are monthly, daily, yearly. Defaults to None.
            exclude_incomplete (bool):  Check if averages is done on complete chunks, and remove from the output
                                        chunks which have not all the expected records.
            time_bound (bool):  option to create the time bounds.
            center_time (bool): option to center the time coordinate to the middle of the averaging period.

        Returns:
            A xarray.Dataset containing the time averaged data.
        """
        if isinstance(data, types.GeneratorType):
            return self._timmeangen(data, freq=freq, exclude_incomplete=exclude_incomplete,
                                    time_bounds=time_bounds, center_time=center_time)
        else:
            return self._timmean(data, freq=freq, exclude_incomplete=exclude_incomplete,
                                 time_bounds=time_bounds, center_time=center_time)

    def _timmeangen(self, data, **kwargs):
        """Call the timmean function for iterators"""
        for ds in data:
            yield self._timmean(ds, **kwargs)

    def _timmean(self, data, freq=None, exclude_incomplete=False,
                 time_bounds=False, center_time=False):
        """
        Perform daily, monthly and yearly averaging.
        See timmean for more details.
        """
        resample_freq = frequency_string_to_pandas(freq)

        if 'time' not in data.dims:
            raise ValueError('Time dimension not found in the input data. Cannot compute timmean.')
            
        # Get original frequency (for history)
        if len(data.time) > 1:
            orig_freq = data['time'].values[1]-data['time'].values[0]
            # Convert time difference to hours
            self.orig_freq = round(np.timedelta64(orig_freq, 'ns') / np.timedelta64(1, 'h'))
        else:
            # this block is likely never run, as the check for time dimension is done before
            self.logger.warning('A single timestep is available, is this correct?')
            self.orig_freq = 'Unknown'
            if exclude_incomplete:
                self.logger.warning('Disabling exclude incomplete since it cannot work if we have a single tstep!')
                exclude_incomplete = False

        try:
            # Resample to the desired frequency
            self.logger.info('Resampling to %s frequency...', str(resample_freq))
            out = data.resample(time=resample_freq).mean()
        except ValueError as exc:
            raise ValueError('Cant find a frequency to resample, aborting!') from exc

        if exclude_incomplete:

            self.logger.info('Checking if incomplete chunks has been produced...')
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
        if np.any(np.isnat(out.time)):
            raise ValueError('Resampling cannot produce output for all frequency step, is your input data correct?')

        out = log_history(out, f"resampled from frequency {self.orig_freq}h to frequency {freq} by AQUA timmean")

        # Add a variable to create time_bounds
        if time_bounds:
            resampled = data.time.resample(time=resample_freq)
            time_bnds = xr.concat([resampled.min(), resampled.max()], dim='bnds').transpose()
            time_bnds['time'] = out.time
            time_bnds.name = 'time_bnds'
            out = xr.merge([out, time_bnds])
            if np.any(np.isnat(out.time_bnds)):
                raise ValueError('Resampling cannot produce output for all time_bnds step!')
            log_history(out, "time_bnds added by by AQUA timmean")

        out.aqua.set_default(self)  # This links the dataset accessor to this instance of the Reader class

        return out
    
    def center_time_axis(self, avg_data, resample_freq):

        # decipher the frequency
        literal, numeric = extract_literal_and_numeric(resample_freq) 
        self.logger.debug('Frequency is %s with numeric part %s', literal, numeric)

        # if we have monthly/yearly time windows we cannot use timedelta and need to do some tricky magic
        if any(check in literal for check in ["YS", "MS", "M", "Y", "ME", "YE"]):
            if 'YS' in resample_freq:
                offset = pd.DateOffset(months=6*numeric)
            elif 'MS' in resample_freq:
                if numeric % 2 == 1:
                    offset = pd.DateOffset(days=14, months=(numeric // 2))
                else:  
                    offset = pd.DateOffset(month=(numeric // 2))
            else:
                self.logger.error("center_time cannot be not implemented for end of the frequency %s", resample_freq)
                return avg_data
            self.logger.debug('Time offset (DateOffset) for time centering will be %s', offset)
            avg_data["time"] = avg_data.get_index("time") + offset
        
        # otherwise we can use timedelta (which works with fractions)
        else:
            offset = pd.Timedelta(numeric/2, literal)
            self.logger.debug('Time offset (Timedelta) for time centering will be %s', offset)
            avg_data['time'] =  avg_data["time"] + offset
        
        return avg_data