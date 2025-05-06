"""Timeseries class for retrieve and netcdf saving of a single experiment"""
import xarray as xr
import pandas as pd
from aqua.util import to_list, frequency_string_to_pandas
from .util import loop_seasonalcycle, center_timestamp
from .base import BaseMixin

xr.set_options(keep_attrs=True)


class Timeseries(BaseMixin):
    """Timeseries class for retrieve and netcdf saving of a single experiment"""

    def __init__(self, catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 std_startdate: str = None, std_enddate: str = None,
                 region: str = None, lon_limits: list = None, lat_limits: list = None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the Timeseries class.

        Args:
            catalog (str): The catalog to be used. If None, the catalog will be determined by the Reader.
            model (str): The model to be used.
            exp (str): The experiment to be used.
            source (str): The source to be used.
            regrid (str): The target grid to be used for regridding. If None, no regridding will be done.
            startdate (str): The start date of the data to be retrieved.
                             If None, all available data will be retrieved.
            enddate (str): The end date of the data to be retrieved.
                           If None, all available data will be retrieved.
            std_startdate (str): The start date of the standard period.
            std_enddate (str): The end date of the standard period.
            region (str): The region to select. This will define the lon and lat limits.
            lon_limits (list): The longitude limits to be used. Overriden by region.
            lat_limits (list): The latitude limits to be used. Overriden by region.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """
        super().__init__(catalog=catalog, model=model, exp=exp, source=source, regrid=regrid,
                         startdate=startdate, enddate=enddate,
                         std_startdate=std_startdate, std_enddate=std_enddate,
                         region=region, lon_limits=lon_limits,
                         lat_limits=lat_limits, loglevel=loglevel)

    def run(self, var: str, formula: bool = False, long_name: str = None,
            units: str = None, standard_name: str = None, std: bool = False,
            freq: list = ['monthly', 'annual'], extend: bool = True,
            exclude_incomplete: bool = True, center_time: bool = True,
            box_brd: bool = True, outputdir: str = './', rebuild: bool = True):
        """
        Run all the steps necessary for the computation of the Timeseries.
        Save the results to netcdf files.
        Can evaluate different frequencies.

        Args:
            var (str): The variable to be retrieved.
            formula (bool): If True, the variable is a formula.
            long_name (str): The long name of the variable, if different from the variable name.
            units (str): The units of the variable, if different from the original units.
            standard_name (str): The standard name of the variable, if different from the variable name.
            std (bool): If True, compute the standard deviation. Default is False.
            freq (list): The frequencies to be used for the computation. Available options are 'hourly', 'daily',
                         'monthly' and 'annual'. Default is ['monthly', 'annual'].
            extend (bool): If True, extend the data if needed.
            exclude_incomplete (bool): If True, exclude incomplete periods.
            center_time (bool): If True, the time will be centered.
            box_brd (bool): choose if coordinates are comprised or not in area selection.
            outputdir (str): The directory to save the data.
            rebuild (bool): If True, rebuild the data from the original files.
        """
        self.logger.info('Running Timeseries for %s', var)
        self.retrieve(var=var, formula=formula, long_name=long_name, units=units, standard_name=standard_name)
        freq = to_list(freq)

        for f in freq:
            self.logger.info('Computing %s mean', f)
            self.compute(freq=f, extend=extend, exclude_incomplete=exclude_incomplete,
                         center_time=center_time, box_brd=box_brd)
            if std:
                self.compute_std(freq=f, exclude_incomplete=exclude_incomplete, center_time=center_time,
                                 box_brd=box_brd)
            self.save_netcdf(diagnostic='timeseries', freq=f, outputdir=outputdir, rebuild=rebuild) 

    def compute(self, freq: str, extend: bool = True, exclude_incomplete: bool = True,
                center_time: bool = True, box_brd: bool = True):
        """
        Compute the mean of the data. Support for hourly, daily, monthly and annual means.

        Args:
            freq (str): The frequency to be used for the resampling.
            exclude_incomplete (bool): If True, exclude incomplete periods.
            center_time (bool): If True, the time will be centered.
            box_brd (bool,opt): choose if coordinates are comprised or not in area selection.
                                Default is True
        """
        if freq is None:
            self.logger.error('Frequency not provided, cannot compute mean')
            return

        freq = frequency_string_to_pandas(freq)
        str_freq = self._str_freq(freq)

        self.logger.info('Computing %s mean', str_freq)
        data = self.data.sel(time=slice(self.plt_startdate, self.plt_enddate))

        # Field and time average
        data = self.reader.fldmean(data, box_brd=box_brd, lon_limits=self.lon_limits, lat_limits=self.lat_limits)
        data = self.reader.timmean(data, freq=freq, exclude_incomplete=exclude_incomplete, center_time=center_time)

        if extend:
            data = self._extend_data(data=data, freq=str_freq, center_time=center_time)

        if self.region is not None:
            data.attrs['region'] = self.region

        if str_freq == 'hourly':
            self.hourly = data
        elif str_freq == 'daily':
            self.daily = data
        elif str_freq == 'monthly':
            self.monthly = data
        elif str_freq == 'annual':
            self.annual = data

    def _extend_data(self, data: xr.DataArray,
                     freq: str = None, center_time: bool = True):
        """
        Extend the data with a loop if needed.
        This works only for monthly and annual frequencies.

        Args:
            data (xr.DataArray): The data to be extended.
            freq (str): The frequency of the data.
            center_time (bool): If True, the time will be centered.
        """
        if freq == 'monthly' or freq == 'annual':
            class_startdate = pd.Timestamp(self.startdate)
            class_enddate = pd.Timestamp(self.enddate)
            start_date = pd.Timestamp(self.data.time[0].values)
            end_date = pd.Timestamp(self.data.time[-1].values)

            # if the center_time is True, we need to center the time of the start_date and end_date
            # to be able to compare them with the class_startdate and class_enddate
            if center_time:
                start_date = center_timestamp(time=start_date, freq=freq)
                end_date = center_timestamp(time=end_date, freq=freq)
                class_startdate = center_timestamp(time=class_startdate, freq=freq)
                class_enddate = center_timestamp(time=class_enddate, freq=freq)

            # Extend the data if needed
            if class_startdate < start_date:
                self.logger.info('Extending back the start date from %s to %s', start_date, class_startdate)
                loop = loop_seasonalcycle(data=data, startdate=class_startdate, enddate=start_date,
                                          freq=freq, center_time=center_time, loglevel=self.loglevel)
                data = xr.concat([loop, data], dim='time')
                data = data.sortby('time')
            else:
                self.logger.debug(f'No extension needed for the start date: {start_date} <= {class_startdate}')

            if class_enddate > end_date:
                self.logger.info('Extending the end date from %s to %s', end_date, class_enddate)
                loop = loop_seasonalcycle(data=data, startdate=end_date, enddate=class_enddate,
                                          freq=freq, center_time=center_time, loglevel=self.loglevel)
                data = xr.concat([data, loop], dim='time')
                data = data.sortby('time')
            else:
                self.logger.debug(f'No extension needed for the end date: {class_enddate} >= {end_date}')

            return data
        else:
            self.logger.warning(f"The frequency {freq} does not support extension")
            return data
