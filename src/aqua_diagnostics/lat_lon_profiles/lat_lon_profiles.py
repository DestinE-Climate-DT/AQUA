import xarray as xr

from aqua.logger import log_configure
from aqua.util import to_list, frequency_string_to_pandas, time_to_string

from aqua.diagnostics.core import Diagnostic, start_end_dates, OutputSaver              

class LatLonProfiles(Diagnostic):
        """
        Class to compute lat-lon profiles of a variable over a specified region.
        It retrieves the data from the catalog, computes the mean and standard deviation
        over the specified period and saves the results to netcdf files.
        The class supports different frequencies (hourly, daily, monthly, annual) and
        different types of means (zonal, meridional, global).

        Args:
        catalog (str): The catalog to be used for the retrieval of the data.
        model (str): The model to be used for the retrieval of the data.
        exp (str): The experiment to be used for the retrieval of the data.
        source (str): The source to be used for the retrieval of the data.
        regrid (str): The regridding method to be used for the retrieval of the data.
        startdate (str): The start date of the data to be retrieved.
        enddate (str): The end date of the data to be retrieved.
        std_startdate (str): The start date of the standard deviation period.
        std_enddate (str): The end date of the standard deviation period.
        region (str): The region to be used for the retrieval of the data.
        lon_limits (list): The longitude limits of the region.
        lat_limits (list): The latitude limits of the region.
        mean_type (str): The type of mean to compute ('zonal', 'meridional', 'global').
        loglevel (str): The log level to be used for the logging.

        """    
        def __init__(self, catalog: str = None, model: str = None,
                     exp: str = None, source: str = None,
                     regrid: str = None,
                     startdate: str = None, enddate: str = None,
                     std_startdate: str = None, std_enddate: str = None,
                     region: str = None, lon_limits: list = None, lat_limits: list = None,
                     mean_type: str = 'zonal',
                     loglevel: str = 'WARNING'):
                """
                Initialize the LatLonProfiles class.

                Args:
                catalog (str): The catalog to be used for the retrieval of the data.
                model (str): The model to be used for the retrieval of the data.
                exp (str): The experiment to be used for the retrieval of the data.
                source (str): The source to be used for the retrieval of the data.
                regrid (str): The regridding method to be used for the retrieval of the data.
                startdate (str): The start date of the data to be retrieved.
                enddate (str): The end date of the data to be retrieved.
                std_startdate (str): The start date of the standard deviation period.
                std_enddate (str): The end date of the standard deviation period.
                region (str): The region to be used for the retrieval of the data.
                lon_limits (list): The longitude limits of the region.
                lat_limits (list): The latitude limits of the region.
                mean_type (str): The type of mean to compute ('zonal', 'meridional', 'global').
                loglevel (str): The log level to be used for the logging.
                
                """
                # Initialize the Diagnostic class with the provided parameters
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
                                                                                 diagnostic='lat_lon_profiles',
                                                                                 lon_limits=lon_limits,
                                                                                 lat_limits=lat_limits)

                # Initialize the possible results
                self.seasonal = None  # Seasonal means [DJF, MAM, JJA, SON]
                self.annual = None    # Annual mean
                self.std_seasonal = None  # Seasonal std deviations
                self.std_annual = None   # Annual std deviation
                self.direct_profile = None  # Direct profile for specific timestep

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
        
        def select_timestep(self, timestep: str, box_brd: bool = True):
                """
                Select and compute profile for a specific timestep from already loaded data.
                This method preserves existing seasonal/annual data and returns the profile.
                
                Args:
                        timestep (str): The specific timestep to select (e.g., '1995-07', '1995-07-15')
                        box_brd (bool): Choose if coordinates are comprised or not in area selection
                
                Returns:
                        xr.DataArray: The profile for the selected timestep
                """
                if self.data is None:
                        self.logger.error('No data available. Run retrieve() or run() first.')
                        return None
                
                self.logger.info(f'Selecting timestep: {timestep}')
                
                # Use the existing _compute_direct_profile method
                self._compute_direct_profile(timestep=timestep, box_brd=box_brd)
                
                # Return the computed profile for easy variable assignment
                return self.direct_profile
        
        def compute_std(self, freq: str, exclude_incomplete: bool = True, center_time: bool = True,
                        box_brd: bool = True):
                """
                Compute the standard deviation of the data. Support for seasonal and annual frequencies.

                Args:
                freq (str): The frequency to be used ('seasonal' or 'annual').
                exclude_incomplete (bool): If True, exclude incomplete periods.
                center_time (bool): If True, the time will be centered.
                box_brd (bool,opt): choose if coordinates are comprised or not in area selection.
                                        Default is True
                """
                if freq is None:
                        self.logger.error('Frequency not provided')
                        raise ValueError('Frequency not provided')

                str_freq = self._str_freq(freq)
                if str_freq is None:
                        return
                        
                self.logger.info('Computing %s standard deviation', str_freq)

                # Start with monthly data for both seasonal and annual std calculations
                data = self.data
                data = self.reader.fldmean(data, box_brd=box_brd,
                                        lon_limits=self.lon_limits, lat_limits=self.lat_limits)
                monthly_data = self.reader.timmean(data, freq='monthly', exclude_incomplete=exclude_incomplete,
                                                center_time=center_time)
                monthly_data = monthly_data.sel(time=slice(self.std_startdate, self.std_enddate))

                if str_freq == 'seasonal':
                        # Group by season and compute std
                        seasonal_std = monthly_data.groupby('time.season').std('time')
                        seasonal_std.attrs['std_startdate'] = time_to_string(self.std_startdate)
                        seasonal_std.attrs['std_enddate'] = time_to_string(self.std_enddate)
                        self.std_seasonal = seasonal_std
                        
                elif str_freq == 'annual':
                        # Group by year and compute std across years
                        annual_data = monthly_data.groupby('time.year').mean('time')
                        annual_std = annual_data.std('year')
                        annual_std.attrs['std_startdate'] = time_to_string(self.std_startdate)
                        annual_std.attrs['std_enddate'] = time_to_string(self.std_enddate)
                        self.std_annual = annual_std
        
        def _compute_direct_profile(self, timestep: str, box_brd: bool = True):
                """
                Compute direct profile for a specific timestep.
                
                Args:
                        timestep (str): The specific timestep to compute profile for (e.g., '1990-06-15')
                        box_brd (bool): Choose if coordinates are comprised or not in area selection
                """
                self.logger.info(f'Computing direct profile for timestep: {timestep}')
                
                # Select data for the specific timestep
                try:
                        timestep_data = self.data.sel(time=timestep, method='nearest')
                except KeyError:
                        self.logger.error(f'Timestep {timestep} not found in data')
                        return
                
                # Determine dimensions for averaging based on mean_type
                if self.mean_type == 'zonal':
                        dims = ['lon']
                elif self.mean_type == 'meridional':
                        dims = ['lat']
                elif self.mean_type == 'global':
                        dims = ['lon', 'lat']
                else:
                        self.logger.error('Mean type %s not recognized', self.mean_type)
                        return
                
                # Apply spatial averaging for the direct profile
                direct_data = self.reader.fldmean(timestep_data, 
                                                box_brd=box_brd,
                                                lon_limits=self.lon_limits, 
                                                lat_limits=self.lat_limits,
                                                dims=dims)
                
                # Add metadata
                if self.region is not None:
                        direct_data.attrs['region'] = self.region
                direct_data.attrs['timestep'] = timestep
                direct_data.attrs['mean_type'] = self.mean_type
                
                self.direct_profile = direct_data
        def save_netcdf(self, diagnostic: str, freq: str,
                        outdir: str = './', rebuild: bool = True):
                """
                Save the data to a netcdf file.

                Args:
                diagnostic (str): The diagnostic to be saved.
                freq (str): The frequency of the data ('seasonal' or 'annual').
                outdir (str): The directory to save the data.
                rebuild (bool): If True, rebuild the data from the original files.
                """
                str_freq = self._str_freq(freq)
                if str_freq is None:
                        return

                if str_freq == 'seasonal':
                        data = self.seasonal if self.seasonal is not None else None
                        data_std = self.std_seasonal if self.std_seasonal is not None else None
                        if data is None:
                                self.logger.error('No seasonal data available')
                                return
                elif str_freq == 'annual':
                        data = self.annual if self.annual is not None else None
                        data_std = self.std_annual if self.std_annual is not None else None
                        if data is None:
                                self.logger.error('No annual data available')
                                return

                # Handle seasonal data (list of seasons)
                if str_freq == 'seasonal':
                        seasons = ['DJF', 'MAM', 'JJA', 'SON']
                        for i, season_data in enumerate(data):
                                diagnostic_product = getattr(season_data, 'standard_name', 'unknown')
                                diagnostic_product += f'.{str_freq}.{seasons[i]}'
                                region = self.region.replace(' ', '').lower() if self.region is not None else None
                                diagnostic_product += f'.{region}' if region is not None else ''
                                
                                self.logger.info('Saving %s data for %s to netcdf in %s', seasons[i], diagnostic_product, outdir)
                                super().save_netcdf(data=season_data, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                                                outdir=outdir, rebuild=rebuild)
                else:
                        # Handle annual data
                        diagnostic_product = getattr(data, 'standard_name', 'unknown')
                        diagnostic_product += f'.{str_freq}'
                        region = self.region.replace(' ', '').lower() if self.region is not None else None
                        diagnostic_product += f'.{region}' if region is not None else ''
                        outdir
                        self.logger.info('Saving %s data for %s to netcdf in %s', str_freq, diagnostic_product, outdir)
                        super().save_netcdf(data=data, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                                        outdir=outdir, rebuild=rebuild)

                # Save std data if available
                if data_std is not None:
                        if str_freq == 'seasonal':
                        # Seasonal std data handling (if it's also a list)
                                if hasattr(data_std, '__iter__') and not isinstance(data_std, str):
                                        seasons = ['DJF', 'MAM', 'JJA', 'SON']
                                        for i, std_data in enumerate(data_std):
                                                diagnostic_product = getattr(std_data, 'standard_name', 'unknown')
                                                diagnostic_product += f'.{str_freq}.{seasons[i]}.std'
                                                region = self.region.replace(' ', '').lower() if self.region is not None else None
                                                diagnostic_product += f'.{region}' if region is not None else ''
                                                super().save_netcdf(data=std_data, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                                                                outdir=outdir, rebuild=rebuild)
                        else:
                                diagnostic_product = getattr(data_std, 'standard_name', 'unknown')
                                diagnostic_product += f'.{str_freq}.std'
                                region = self.region.replace(' ', '').lower() if self.region is not None else None
                                diagnostic_product += f'.{region}' if region is not None else ''
                                super().save_netcdf(data=data_std, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                                                outdir=outdir, rebuild=rebuild)

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
                if freq in ['seasonal', 'season']:
                        str_freq = 'seasonal'
                elif freq in ['YS', 'YE', 'Y', 'annual']:
                        str_freq = 'annual'
                else:
                        self.logger.error('Frequency %s not recognized. Only "seasonal" and "annual" are supported', freq)
                        return None

                return str_freq
        
        def compute_dim_mean(self, freq: str, exclude_incomplete: bool = True,
                center_time: bool = True, box_brd: bool = True, var: str = None):
                """
                Compute the mean of the data. Support for seasonal and annual means.

                Args:
                freq (str): The frequency to be used ('seasonal' or 'annual').
                exclude_incomplete (bool): If True, exclude incomplete periods.
                center_time (bool): If True, the time will be centered.
                box_brd (bool,opt): choose if coordinates are comprised or not in area selection.
                                        Default is True
                var (str): The variable to be used if not in metadata.
                """
                if freq is None:
                        self.logger.error('Frequency not provided, cannot compute mean')
                        return

                str_freq = self._str_freq(freq)
                if str_freq is None:
                        return

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

                # First compute monthly means, then seasonal/annual
                monthly_data = self.reader.timmean(data, freq='monthly', 
                                                exclude_incomplete=exclude_incomplete, 
                                                center_time=center_time)
                
                # Apply spatial averaging to monthly data
                monthly_data = self.reader.fldmean(monthly_data, 
                                                box_brd=box_brd, 
                                                lon_limits=self.lon_limits, lat_limits=self.lat_limits,
                                                dims=dims)

                if str_freq == 'seasonal':
                        # Compute seasonal means from monthly data
                        seasonal_data = self.reader.timmean(monthly_data, freq='seasonal')
                        if self.region is not None:
                                for season_data in seasonal_data:
                                        season_data.attrs['region'] = self.region
                        self.seasonal = seasonal_data
                        
                elif str_freq == 'annual':
                        # Compute annual mean from monthly data
                        annual_data = self.reader.timmean(monthly_data, freq='annual')
                        if self.region is not None:
                                annual_data.attrs['region'] = self.region
                        self.annual = annual_data

        def run(self, var: str, formula: bool = False, long_name: str = None,
                units: str = None, standard_name: str = None, std: bool = False,
                freq: list = ['seasonal', 'annual'], extend: bool = True,
                exclude_incomplete: bool = True, center_time: bool = True,
                box_brd: bool = True, outdir: str = './', rebuild: bool = True,
                mean_type: str = None, timestep: str = None):
                """
                Run all the steps necessary for the computation of the LatLonProfiles.

                Args:
                        var (str): The variable to be retrieved and computed.
                        formula (bool): Whether to use a formula for the variable.
                        long_name (str): The long name of the variable.
                        units (str): The units of the variable.
                        standard_name (str): The standard name of the variable.
                        std (bool): Whether to compute the standard deviation.
                        freq (list): The frequencies to compute ('seasonal', 'annual').
                        extend (bool): Whether to extend the time dimension.
                        exclude_incomplete (bool): Whether to exclude incomplete time periods.
                        center_time (bool): Whether to center the time coordinate.
                        box_brd (bool): Whether to include the box boundaries.
                        outdir (str): The output directory to save the results.
                        rebuild (bool): Whether to rebuild existing files.
                        mean_type (str): The type of mean to compute ('zonal', 'meridional', 'global').
                        timestep (str): Specific timestep to compute (e.g., '1995-07', '1995-07-15').
                                If provided, only computes direct profile for this timestep.
                """
                self.logger.info('Running LatLonProfiles for %s', var)
                
                # Retrieve the data
                self.retrieve(var=var, formula=formula, long_name=long_name, 
                                units=units, standard_name=standard_name)
                
                # Set mean_type if provided
                if mean_type is not None:
                        self.mean_type = mean_type
                self.logger.info('Mean type set to %s', self.mean_type)
                
                # Check if data is valid
                self._check_data(var=var, units=units)
                
                # If timestep is provided, compute only direct profile
                if timestep is not None:
                        self.logger.info(f'Computing direct profile for timestep: {timestep}')
                        self._compute_direct_profile(timestep=timestep, box_brd=box_brd)
                        return
                
                # Compute temporal means (seasonal/annual)
                self.logger.info('Computing temporal means')
                freq = to_list(freq)
                for f in freq:
                        self.logger.info(f'Computing {f} mean')
                        self.compute_dim_mean(freq=f, exclude_incomplete=exclude_incomplete,
                                        center_time=center_time, box_brd=box_brd, var=var)
                        
                        if std: 
                                self.logger.info(f'Computing {f} standard deviation')
                                self.compute_std(freq=f, exclude_incomplete=exclude_incomplete, 
                                                center_time=center_time, box_brd=box_brd)
                        
                        self.logger.info(f'Saving {f} netcdf file')
                        self.save_netcdf(diagnostic='lat_lon_profiles', freq=f, 
                                        outdir=outdir, rebuild=rebuild)
                
                self.logger.info('LatLonProfiles computation completed')