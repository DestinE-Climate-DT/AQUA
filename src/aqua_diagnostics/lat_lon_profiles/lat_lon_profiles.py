import xarray as xr

from aqua.logger import log_configure
from aqua.util import to_list, frequency_string_to_pandas, time_to_string

from aqua.diagnostics.core import Diagnostic, start_end_dates, OutputSaver              

class LatLonProfiles(Diagnostic):

        def __init__(self, catalog: str = None, model: str = None,
                     exp: str = None, source: str = None,
                     regrid: str = None,
                     startdate: str = None, enddate: str = None,
                     std_startdate: str = None, std_enddate: str = None,
                     region: str = None, lon_limits: list = None, lat_limits: list = None,
                     mean_type: str = 'zonal',
                     loglevel: str = 'WARNING'):
                
                super().__init__(catalog=catalog, model=model, exp=exp, source=source, regrid=regrid,
                                 loglevel=loglevel)
                
                self.logger = log_configure(log_level=loglevel, log_name='LatLonProfiles')

                # We want to make sure we retrieve the required amount of data with a single Reader instance
                self.startdate, self.enddate = start_end_dates(startdate=startdate, enddate=enddate,
                                                               start_std=std_startdate, end_std=std_enddate)
                # They need to be stored to evaluate the std on the correct period
                self.std_startdate = self.startdate if std_startdate is None else std_startdate
                self.std_enddate = self.enddate if std_enddate is None else std_enddate
                # Finally we need to set the start and end dates of the data
                self.plt_startdate = startdate
                self.plt_enddate = enddate
                self.logger.debug(f"Retrieve start date: {self.startdate}, End date: {self.enddate}")
                self.logger.debug(f"Plot start date: {self.plt_startdate}, End date: {self.plt_enddate}")
                self.logger.debug(f"Std start date: {self.std_startdate}, Std end date: {self.std_enddate}")

                # Set the region based on the region name or the lon and lat limits
                self.region, self.lon_limits, self.lat_limits = self._set_region(region=region,
                                                                                 diagnostic='timeseries',
                                                                                 lon_limits=lon_limits,
                                                                                 lat_limits=lat_limits)

                # Initialize the possible results
                self.hourly = None
                self.daily = None
                self.monthly = None
                self.annual = None
                self.std_hourly = None
                self.std_daily = None
                self.std_monthly = None
                self.std_annual = None
                
                # Initialize seasonal and annual means
                self.seasonal_annual_means = None  # List of [DJF, MAM, JJA, SON, Annual]

                self.mean_type = mean_type

        def retrieve(self, var: str, formula: bool = False, long_name: str = None, 
                     units: str = None, standard_name: str = None):

                # If the user requires a formula the evaluation requires the retrieval
                # of all the variables
                if formula:
                        super().retrieve()
                        self.logger.debug("Evaluating formula %s", var)
                        self.data = eval_formula(mystring=var, xdataset=self.data)
                        if self.data is None:
                                self.logger.error('Error evaluating formula %s', var)
                else:
                        super().retrieve(var=var)
                        if self.data is None:
                                self.logger.error('Error retrieving variable %s', var)
                        # Get the xr.DataArray to be aligned with the formula code
                        self.data = self.data[var]

                if self.plt_startdate is None:
                        self.plt_startdate = self.data.time.min().values
                        self.logger.debug('Plot start date set to %s', self.plt_startdate)
                if self.plt_enddate is None:
                        self.plt_enddate = self.data.time.max().values
                        self.logger.debug('Plot end date set to %s', self.plt_enddate)

                # Customization of the data, expecially needed for formula
                if units is not None:
                        self._check_data(var, units)
                if long_name is not None:
                        self.data.attrs['long_name'] = long_name
                # We use the standard_name as the name of the variable
                # to be always used in plots
                if standard_name is not None:
                        self.data.attrs['standard_name'] = standard_name
                        self.data.name = standard_name
                else:
                        self.data.attrs['standard_name'] = var

        def compute_std(self, freq: str, exclude_incomplete: bool = True, center_time: bool = True,
                        box_brd: bool = True):
                """
                Compute the standard deviation of the data. Support for monthly and annual frequencies.

                Args:
                freq (str): The frequency to be used for the resampling.
                exclude_incomplete (bool): If True, exclude incomplete periods.
                center_time (bool): If True, the time will be centered.
                box_brd (bool,opt): choose if coordinates are comprised or not in area selection.
                                        Default is True
                """
                if freq is None:
                        self.logger.error('Frequency not provided')
                        raise ValueError('Frequency not provided')

                freq = frequency_string_to_pandas(freq)
                str_freq = self._str_freq(freq)
                self.logger.info('Computing %s standard deviation', str_freq)

                freq_dict = {'hourly': {'data': self.hourly, 'groupdby': 'time.hour'},
                             'daily': {'data': self.daily, 'groupdby': 'time.dayofyear'},
                             'monthly': {'data': self.monthly, 'groupdby': 'time.month'},
                             'annual': {'data': self.annual, 'groupdby': None}}

                data = self.data
                data = self.reader.fldmean(data, box_brd=box_brd,
                                           lon_limits=self.lon_limits, lat_limits=self.lat_limits)
                data = self.reader.timmean(data, freq=freq, exclude_incomplete=exclude_incomplete,
                                           center_time=center_time)
                data = data.sel(time=slice(self.std_startdate, self.std_enddate))
                if freq_dict[str_freq]['groupdby'] is not None:
                        data = data.groupby(freq_dict[str_freq]['groupdby']).std('time')
                else:  # For annual data, we compute the std over all years
                        data = data.std('time')

                # Store start and end dates for the standard deviation.
                # pd.Timestamp cannot be used as attribute, so we convert to a string
                data.attrs['std_startdate'] = time_to_string(self.std_startdate)
                data.attrs['std_enddate'] = time_to_string(self.std_enddate)

                # Assign the data to the correct attribute based on frequency
                if str_freq == 'hourly':
                        self.std_hourly = data
                elif str_freq == 'daily':
                        self.std_daily = data
                elif str_freq == 'monthly':
                        self.std_monthly = data
                elif str_freq == 'annual':
                        self.std_annual = data

        def save_netcdf(self, diagnostic: str, freq: str,
                    outputdir: str = './', rebuild: bool = True):
                """
                Save the data to a netcdf file.

                Args:
                diagnostic (str): The diagnostic to be saved.
                freq (str): The frequency of the data.
                outputdir (str): The directory to save the data.
                rebuild (bool): If True, rebuild the data from the original files.
                """
                str_freq = self._str_freq(freq)

                if str_freq == 'hourly':
                        data = self.hourly if self.hourly is not None else self.logger.error('No hourly data available')
                        data_std = self.std_hourly if self.std_hourly is not None else None
                elif str_freq == 'daily':
                        data = self.daily if self.daily is not None else self.logger.error('No daily data available')
                        data_std = self.std_daily if self.std_daily is not None else None
                elif str_freq == 'monthly':
                        data = self.monthly if self.monthly is not None else self.logger.error('No monthly data available')
                        data_std = self.std_monthly if self.std_monthly is not None else None
                elif str_freq == 'annual':
                        data = self.annual if self.annual is not None else self.logger.error('No annual data available')
                        data_std = self.std_annual if self.std_annual is not None else None

                diagnostic_product = getattr(data, 'standard_name', None)
                diagnostic_product += f'.{str_freq}'
                region = self.region.replace(' ', '').lower() if self.region is not None else None
                diagnostic_product += f'.{region}' if region is not None else ''
                self.logger.info('Saving %s data for %s to netcdf in %s', str_freq, diagnostic_product, outputdir)
                super().save_netcdf(data=data, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                                default_path=outputdir, rebuild=rebuild)
                if data_std is not None:
                        diagnostic_product = f'{diagnostic_product}.std'
                        self.logger.info('Saving %s data for %s to netcdf in %s', str_freq, diagnostic_product, outputdir)
                        super().save_netcdf(data=data_std, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                                        default_path=outputdir, rebuild=rebuild)

        def _check_data(self, var: str, units: str):
                """
                Make sure that the data is in the correct units.

                Args:
                var (str): The variable to be checked.
                units (str): The units to be checked.
                """
                self.data = super()._check_data(data=self.data, var=var, units=units)

        def _str_freq(self, freq: str):
                """
                Convert the frequency to a string representation.

                Args:
                freq (str): The frequency to be used.

                Returns:
                str_freq (str): The frequency as a string.
                """
                if freq in ['h', 'hourly']:
                        str_freq = 'hourly'
                elif freq in ['D', 'daily']:
                        str_freq = 'daily'
                elif freq in ['MS', 'ME', 'M', 'mon', 'monthly']:
                        str_freq = 'monthly'
                elif freq in ['YS', 'YE', 'Y', 'annual']:
                        str_freq = 'annual'
                else:
                        self.logger.error('Frequency %s not recognized', freq)

                return str_freq
        
        def compute_dim_mean(self, freq: str, exclude_incomplete: bool = True,
                        center_time: bool = True, box_brd: bool = True, var: str = None):
                """
                Compute the mean of the data. Support for hourly, daily, monthly and annual means.

                Args:
                freq (str): The frequency to be used for the resampling.
                exclude_incomplete (bool): If True, exclude incomplete periods.
                center_time (bool): If True, the time will be centered.
                box_brd (bool,opt): choose if coordinates are comprised or not in area selection.
                                        Default is True
                var (str): The variable to be used if not in metadata.
                """
                if freq is None:
                        self.logger.error('Frequency not provided, cannot compute mean')
                        return

                freq = frequency_string_to_pandas(freq)
                str_freq = self._str_freq(freq)

                if self.mean_type == 'zonal':
                        dims = ['lon']
                elif self.mean_type == 'meridional':
                        dims = ['lat']
                elif self.mean_type == 'global':
                        dims = ['lon', 'lat']
                else:
                        self.logger.error('Mean type %s not recognized', self.mean_type)
                        return

                self.logger.info('Computing %s mean', str_freq)
                data = self.data.sel(time=slice(self.plt_startdate, self.plt_enddate))
                if len(data.time) == 0:
                        self.logger.warning('No data available for the selected period %s - %s, using the standard period %s - %s',
                                            self.plt_startdate, self.plt_enddate, self.std_startdate, self.std_enddate)
                        data = self.data.sel(time=slice(self.std_startdate, self.std_enddate))

                # time and field average
                data = self.reader.timmean(data, 
                                           freq=freq, 
                                           exclude_incomplete=exclude_incomplete, 
                                           center_time=center_time)
                data = self.reader.fldmean(data, 
                                           box_brd=box_brd, 
                                           lon_limits=self.lon_limits, lat_limits=self.lat_limits,
                                           dims=dims)


                if self.region is not None:
                        data.attrs['region'] = self.region

                # Due to the possible usage of the standard period, the time may need to be reselected correctly
                data = data.sel(time=slice(self.plt_startdate, self.plt_enddate))

                if str_freq == 'hourly':
                        self.hourly = data
                elif str_freq == 'daily':
                        self.daily = data
                elif str_freq == 'monthly':
                        self.monthly = data
                elif str_freq == 'annual':
                        self.annual = data

        def compute_seasonal_and_annual_means(self, box_brd: bool = True):
                """
                Compute seasonal and annual means from monthly data.
                Uses reader.timmean for both seasonal and annual means for consistency.
                
                Args:
                    box_brd (bool): Choose if coordinates are comprised or not in area selection.
                                   Default is True
                """
                
                if self.monthly is None:
                    self.logger.error('Monthly data not available. Run compute_dim_mean with monthly frequency first.')
                    return
                
                self.logger.info('Computing seasonal and annual means from monthly data')
                
                # Get monthly data for the selected period
                data = self.monthly.sel(time=slice(self.plt_startdate, self.plt_enddate))
                
                if len(data.time) == 0:
                    self.logger.warning('No monthly data available for the selected period %s - %s, using all available data',
                                       self.plt_startdate, self.plt_enddate)
                    data = self.monthly
                
                # Check if we have at least 12 months of data
                if len(data.time) < 12:
                    self.logger.warning('Less than 12 months of data available (%d months). Results may not be representative.',
                                       len(data.time))
                
                # Compute seasonal means using reader.timmean
                self.logger.debug('Computing seasonal means using reader.timmean')
                seasonal_means = self.reader.timmean(data, freq='seasonal')
                
                # Compute annual mean using reader.timmean
                self.logger.debug('Computing annual mean using reader.timmean')
                annual_mean = self.reader.timmean(data, freq='annual')
                
                # Store results: [DJF, MAM, JJA, SON, Annual]
                self.seasonal_annual_means = seasonal_means + [annual_mean]
                
                # Add region information to all seasonal/annual means
                if self.region is not None:
                    for i, mean_data in enumerate(self.seasonal_annual_means):
                        mean_data.attrs['region'] = self.region
                
                self.logger.info('Seasonal and annual means computed successfully')

        def run(self, var: str, formula: bool = False, long_name: str = None,
                units: str = None, standard_name: str = None, std: bool = False,
                freq: list = ['monthly', 'annual'], extend: bool = True,
                exclude_incomplete: bool = True, center_time: bool = True,
                box_brd: bool = True, outputdir: str = './', rebuild: bool = True,
                mean_type: str = None, compute_seasonal_annual: bool = False):
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
                mean_type (str): The type of mean to compute ('zonal', 'meridional', 'global').
                compute_seasonal_annual (bool): If True, compute seasonal and annual means from monthly data.
                """
                self.logger.info('Running LatLonProfiles for %s', var)
                self.retrieve(var=var, formula=formula, long_name=long_name, units=units, standard_name=standard_name)
                freq = to_list(freq)

                if mean_type is not None:
                        self.mean_type = mean_type
                self.logger.info('Mean type set to %s', self.mean_type)

                for f in freq:
                        self.compute_dim_mean(freq=f, exclude_incomplete=exclude_incomplete,
                                              center_time=center_time, box_brd=box_brd, var=var)
                        if std: 
                                self.compute_std(freq=f, exclude_incomplete=exclude_incomplete, center_time=center_time,
                                                box_brd=box_brd)
                        self.save_netcdf(diagnostic='lat_lon_profiles', freq=f, outputdir=outputdir, rebuild=rebuild)
                
                # Compute seasonal and annual means if requested and monthly data is available
                if compute_seasonal_annual and 'monthly' in freq:
                        self.compute_seasonal_and_annual_means(box_brd=box_brd)