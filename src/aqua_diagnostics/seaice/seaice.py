""" Seaice doc """
import xarray as xr

from aqua.diagnostics.core import Diagnostic
from aqua.exceptions import NoDataError
from aqua.logger import log_configure, log_history
from aqua.util import to_list, merge_attrs
from aqua.diagnostics.seaice.util import ensure_istype
from aqua.fldstat import FldStat

xr.set_options(keep_attrs=True)

class SeaIce(Diagnostic):
    """ 
    Sea ice diagnostic class for computing and analyzing sea ice metrics.
    
    This class provides methods to compute sea ice extent (million km²), volume (thousand km³), 
    fraction ([0-1]) and thickness (m) over specified regions (e.g., Arctic, Antarctic). 
    It supports both time series (integrated), with options for computing standard deviations, 
    seasonal cycles, and 2D monthly climatologies.

    Args:
        model   (str): The model name.
        exp     (str): The experiment name.
        source  (str): The data source.
        catalog (str, optional): The catalog name.
        regrid  (str, optional): The regrid option.
        startdate (str, optional): The start date for the data (format: "YYYY-MM-DD").
        enddate   (str, optional): The end date for the data (format: "YYYY-MM-DD").
        std_startdate (str, optional): Start date for standard deviation.
        std_enddate   (str, optional): End date for standard deviation.
        threshold (float, optional): Threshold for sea ice concentration over extent (default: 0.15; 15% conc).
        regions     (list, optional): A list of regions to analyze. Default: ['arctic', 'antarctic'].
        regions_file (str, optional): Path to YAML file defining regions definition file.
        outputdir (str, optional): The output directory (default: './').
        regions_definition (dict): The loaded regions definition from the YAML file.
        loglevel     (str, optional): The logging level. Defaults to 'WARNING'.
    """

    def __init__(self, model: str, exp: str, source: str,        
                 catalog=None,
                 regrid=None,
                 startdate=None, enddate=None,
                 std_startdate=None, std_enddate=None,
                 threshold=0.15,
                 regions=['arctic', 'antarctic'],
                 regions_file=None,
                 outputdir: str = './',
                 loglevel: str = 'WARNING'):

        self.outputdir = outputdir
        super().__init__(model=model, exp=exp, source=source,
                         regrid=regrid, catalog=catalog, 
                         startdate=startdate, enddate=enddate,
                         loglevel=loglevel)
        self.logger = log_configure(loglevel, 'SeaIce')

        # check region file and defined regions 
        self.load_regions(regions_file=regions_file, regions=regions)

        self.threshold = threshold
        
    def load_regions(self, regions_file=None, regions=None):
        """
        Loads region definitions from a .yaml configuration file and sets the selected regions.

        Args:
            regions_file (str): Full path to the region file. If None, a default path is used.
            regions (str or list of str): A region or list of region names to load. 
                If None, all regions from the configuration are used.
        """
        if regions_file is None:
            regions_file = self._get_default_regions_file(diagnostic='seaice')

        region_definitions = self._read_regions_file(regions_file).get('regions', {})
        self.regions_definition = region_definitions

        selected_regions = to_list(regions)

        if not selected_regions:
            self.logger.warning("No regions specified. Using all available regions.")
            self.regions = list(region_definitions.keys())
            return

        invalid_regions = [reg for reg in selected_regions if reg not in region_definitions]
        
        if invalid_regions:
            invalid_regions_str = ', '.join(str(i) for i in invalid_regions)
            raise ValueError(f"Invalid region name(s): [{invalid_regions_str}]. "
                             f"Please check regions names are lower case or the region file at: '{regions_file}'.")

        self.regions = selected_regions
        
    def compute_seaice(self, method: str = 'extent', var: str = None, *args, **kwargs):
        """
        Execute the seaice diagnostic based on the specified method.
        
        Args:
            var (str): The variable to be used for computation. Default is 'sithick' or 'siconc'.
            method (str): The method to compute sea ice metrics. Options are 'extent' or 'volume'.

        Kwargs:
            - threshold (float): The threshold value for which sea ice fraction is considered. Default is 0.15.
            - reader_kwargs (dict, optional): Additional keyword arguments to pass to the Reader.

        Returns:
            xr.DataArray or xr.Dataset: The computed sea ice metric. A Dataset is returned if multiple regions are requested.
        """
        default_method_vars = {'extent': 'siconc',
                               'volume': 'sithick',
                               'fraction':  'siconc',
                               'thickness': 'sithick'}

        valid_methods = list(default_method_vars)

        if method not in valid_methods:
            raise ValueError(f"Invalid method '{method}'. Please choose from: {valid_methods}")

        self.method = method
        self.var = var or default_method_vars.get(method)

        if not self.var:
            raise ValueError(f"Variable must be specified for method '{method}'")

        if self.method in ['fraction', 'thickness']:
            return self._compute_2d_bymethod(*args, **kwargs)
        else:
            return self._compute_ts_bymethod(*args, **kwargs)
            
    def _compute_ts_bymethod(self, calc_std_freq: str = None, 
                             get_seasonal_cycle: bool = False,
                             ts_monthly: bool = False, ts_monthly_std: bool = False,
                             ts_annual: bool = False,  ts_annual_std: bool = False,
                             reader_kwargs: dict = {}):
        """ 
        Compute sea ice result by integrating data over specified regions.
        
        If a standard deviation calculation frequency (`calc_std_freq`) is provided, also 
        the std deviation of the result is computed.
        The seasonal cycle (monthly climatology) can be computed on values and std.

        Args:
            calc_std_freq (str, optional): 
                The frequency for computing the standard deviation of sea ice result across 
                time (i.e., 'monthly', 'annual') after the integration in space. 
                If None, standard deviation is not computed. Default is None.
            get_seasonal_cycle (bool, optional):
                If True, the output result (and standard deviation if computed) is converted into a 
                seasonal cycle i.e. a monthly climatology. Defaults to False.

        Returns:
            xr.Dataset or Tuple[xr.Dataset, xr.Dataset]: 
                - If `calc_std_freq` is None, returns a dataset containing the integrated sea ice result.
                - If `calc_std_freq` is provided, returns a tuple containing:
                    1. `self.result` (xr.Dataset): The computed sea ice result.
                    2. `self.result_std` (xr.Dataset): The std deviation of sea ice result with specified frequency.
        Notes:
            - Standard deviation is computed across all years if `calc_std_freq` is provided.
        """
        # retrieve data with Diagnostic method
        super().retrieve(var=self.var, reader_kwargs=reader_kwargs)

        # get the sea ice masked by method
        masked_data = self._mask_data_bymethod()

        self.monthly = ts_monthly
        self.annual = ts_annual
        self.monthly_std = ts_monthly_std if ts_monthly else False
        self.annual_std = ts_annual_std if ts_annual else False

        # make a list to store the result DataArrays for each region
        regional_results = []
        # make a list to store the standard deviation of result DataArrays for each region across all years 
        regional_results_std = [] if calc_std_freq else None

        for region in self.regions:

            # integrate the seaice masked data masked_data over the regional spatial dimension to compute sea ice result
            seaice_result = self.integrate_seaice(masked_data, region)

            # make a deepcopy to compute seasonal cycle to avoid losing time coord
            original_si_result = seaice_result.copy(deep=True)

            log_history(seaice_result, f"Method used for seaice computation: {self.method}")

            if get_seasonal_cycle:
                seaice_result = self._compute_seasonal_cycle(seaice_result)
                log_history(seaice_result, "Data converted to seasonal means, grouped by month")

            seaice_result = self.add_seaice_attrs(seaice_result, region, self.startdate, self.enddate)

            regional_results.append(seaice_result)

            # compute standard deviation if frequency is provided
            if calc_std_freq is not None:
                
                seaice_std_result = self._calc_time_stat(original_si_result, stat='std', freq=calc_std_freq)
                log_history(seaice_std_result, f"Method used for standard deviation seaice computation: {self.method}")

                # update attributes and history
                seaice_std_result = self.add_seaice_attrs(seaice_std_result, region,
                                                          self.startdate, self.enddate, std_flag=True)
                self.logger.debug("Attributes updated")                    

                regional_results_std.append(seaice_std_result)

        # combine the result DataArrays into one Dataset and keep only the attributes common
        self.result = xr.merge(regional_results, combine_attrs='drop_conflicts')

        # merge the standard deviation DataArrays if computed
        self.result_std = xr.merge(regional_results_std, combine_attrs='drop_conflicts') if calc_std_freq else None

        self.logger.debug("Loading data in memory")
        self.result.load()
        if calc_std_freq:
            self.result_std.load()
        self.logger.debug("Loaded data in memory")

        # return a tuple if standard deviation was computed, otherwise just the result
        return (self.result, self.result_std) if calc_std_freq else self.result

    def _compute_2d_bymethod(self, reader_kwargs: dict = {}, **kwargs):
        """
        This method computes 2D sea ice climatology for each region.

        Args:
            reader_kwargs (dict, optional): Extra keyword arguments passed to the data reader.
            **kwargs: Additional keyword arguments.
                - stat (str): The statistic to compute ('mean' or 'std'). Default is 'mean'.
                - freq (str): The frequency for grouping the data ('monthly' or 'annual'). Default is 'monthly'.
        Returns:
            xr.Dataset: A dataset containing the computed 2D sea ice climatologies for all requested regions.
        """
        stat = kwargs.get('stat', 'mean')
        freq = kwargs.get('freq', 'monthly')

        super().retrieve(var=self.var, reader_kwargs=reader_kwargs)
        original_masked_data = self._mask_data_bymethod()

        regional_2d_results = []

        for region in self.regions:
            masked_data = original_masked_data.copy(deep=True)
            
            # get the area cells and coordinates for the masked data
            # areacello, space_coord = self.get_area_cells_and_coords(masked_data)
            # areacello = self.select_region_area_cell(areacello, region)

            masked_data_region = self._select_region(masked_data, region=region, diagnostic='seaice').get('data')

            if self.method in ['fraction','thickness']:
                seaice_2d_result = self._calc_time_stat(masked_data_region, stat=stat, freq=freq)
            else:
                raise ValueError(f"Method '{self.method}' is not supported for 2D computation.")

            seaice_result = self.add_seaice_attrs(seaice_2d_result, region, self.startdate, self.enddate)

            regional_2d_results.append(seaice_2d_result)

        # combine the result DataArrays into one Dataset and keep only the attributes common
        self.result = xr.merge(regional_2d_results, combine_attrs='drop_conflicts')

        self.logger.debug("Loading data in memory")
        self.result.load()
        self.logger.debug("Loaded data in memory")

        return self.result

    def _mask_data_bymethod(self):
        """
        Mask the data based on the specified method.
        
        The case with sea ice 'extent' is calculated by applying a threshold to the sea ice concentration variable
        and summing the masked data over the regional spatial dimension.

        Returns:
            method_masked_data (xr.DataArray): The masked data based on the specified method.
        """
        if self.data is None:
            self.logger.error(f"Variable {self.var} not found in dataset {self.model}, {self.exp}, {self.source}")
            raise NoDataError("Variable not found in dataset")

        self.logger.debug(f"Masking data for {self.var} with method {self.method}")

        if self.method == 'extent':
            method_masked_data = self.data[self.var].where((self.data[self.var] > self.threshold) &
                                                           (self.data[self.var] < 1.0))
        elif self.method == 'volume':
            method_masked_data = self.data[self.var].where((self.data[self.var] > 0) &
                                                           (self.data[self.var] < 99.0))
        else:
            method_masked_data = self.data[self.var].copy(deep=True)

        if method_masked_data is None:
            self.logger.error(f"Something wrong occurred: masked data is None. Check. "
                              f"Also check if var exist in: {self.model}, {self.exp}, {self.source}.")
            raise NoDataError("Variable not found")
        
        return method_masked_data

    def get_area_cells_and_coords(self, masked_data: xr.DataArray):
        """
        Get areacello and space coordinates

        Args:
            masked_data (xr.DataArray): The masked data to be checked if it is regridded or not

        Returns:
            xr.DataArray: The area grid cells (m^2).
        """
        if 'AQUA_regridded' in masked_data.attrs:
            self.logger.debug('Data has been regridded, using target grid area & coords')
            areacello = self.reader.tgt_grid_area
            space_coord = self.reader.tgt_space_coord
        else:
            self.logger.debug('Data has not been regridded, using source grid area & coords')
            areacello = self.reader.src_grid_area
            space_coord = self.reader.src_space_coord
        
        if areacello is None:
            areacello = self.reader.grid_area
            space_coord = self.reader.space_coord

        # get xr.DataArray with info on grid area that must be reinitialised for each region.
        if len(areacello.data_vars) > 1:
            self.logger.warning(f"Dataset 'areacello' has more than one variable. Searching for 'cell_area'")
            areacello = areacello['cell_area']
        else:
            var_name = list(areacello.data_vars)[0]
            areacello = areacello[var_name]

        return areacello, space_coord

    def select_region_area_cell(self, areacello: xr.DataArray, region: str, drop: bool = True):
        """
        Select the area cells for a specific region based on the region definition.

        Args:
            areacello (xr.DataArray): The area cells DataArray.
            region (str): The region for which to select the area cells.

        Returns:
            xr.DataArray: The area cells DataArray filtered by the region coordinates.
        """
        self.logger.debug(f'Selecting area cells for region: {region}')
        
        if region not in self.regions_definition:
            raise ValueError(f"Region '{region}' not found in regions definition.")

        if areacello is not None:
            ensure_istype(areacello, xr.DataArray, logger=self.logger)
        else:
            raise NoDataError("Area cells (areacello) is None. Cannot select region area cells.")

        # make area selection flexible to lon values from -180 to 180 or from 0 to 360
        try:
            lonmin = round(areacello.lon.min().values/180)*180
            lonmax = round(areacello.lon.max().values/180)*180
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

        # regional selection with lat-lon: use default dict to set dynamic lon bounds found above, and set lat from -90 to 90
        res_dict = self._select_region(areacello, region=region, diagnostic="seaice", drop=drop,
                                       default_coords={"lon_min": lonmin, "lon_max": lonmax, "lat_min": -90, "lat_max": 90})
        areacello = res_dict['data']

        return areacello
    
    def integrate_seaice(self, masked_data, region: str):
        """
        Integrate the masked data over the spatial dimension to compute sea ice metrics.
        If method is extent / volume, divide by 1e12 to convert to million km^2 / thousand km^3.

        Args:
            masked_data (xr.DataArray): The masked data to be integrated.
            region (str): The region for which the sea ice metric is computed.

        Returns:
            xr.DataArray: The computed sea ice metric.
        """
        areacello, space_coord = self.get_area_cells_and_coords(masked_data)
        areacello = self.select_region_area_cell(areacello, region)
        
        masked_data_region = self._select_region(masked_data, region=region, diagnostic='seaice').get('data')

        self.logger.info(f'Computing sea ice {self.method} for {region}')

        if 'AQUA_regridded' in masked_data.attrs:
            grid_name = self.reader.tgt_grid_name
        else:
            grid_name = self.reader.src_grid_name

        si_fldstat = FldStat(area=areacello, horizontal_dims=space_coord, 
                             grid_name=grid_name, loglevel=self.loglevel)

        if self.method == 'extent':
            # compute sea ice extent: exclude areas with no sea ice and sum over the spatial dimension; divide by 1e12 to convert to million km^2
            seaice_integrated = si_fldstat.fldstat(masked_data_region.notnull(), stat='areasum', dims=space_coord) / 1e12
        if self.method == 'volume':
            # compute sea ice volume: exclude areas with no sea ice; divide by 1e12 to convert to thousand km^3
            seaice_integrated = si_fldstat.fldstat(masked_data_region, stat='integral', dims=space_coord) / 1e12

            merge_attrs(seaice_integrated.attrs, masked_data.attrs)
            merge_attrs(seaice_integrated.attrs, areacello.attrs, overwrite=True)

        # ensure masked_data attrs are present
        merge_attrs(seaice_integrated.attrs, masked_data_region.attrs)

        # Add domain-specific sea ice attributes. This overwrites standard_name, long_name, units.
        seaice_integrated = self.add_seaice_attrs(seaice_integrated, region)

        return seaice_integrated

    def _calc_time_stat(self, computed_data: xr.DataArray, freq: str = 'monthly', stat: str = 'std'):
        """
        Compute the standard deviation or mean of the data grouped by a specified time frequency (`monthly` or `annual`).

        Args:
            computed_data (xarray.DataArray): 
                The input data on which the standard deviation will be computed.
            freq (str, optional): The time frequency for grouping before computing the time statistic:
                - 'monthly' (computes std per month)
                - 'annual'  (computes std per year)
            stat (str, optional): 
                The statistic to compute. Must be one of ('std', 'mean'). Default is 'std'.

        Returns:
            xarray.DataArray: A DataArray containing the computed time statistic.
        """
        if freq not in ['monthly', 'annual']:
            self.logger.warning(f"Frequency str: '{freq}' not recognized. Set to 'monthly' by default.")
            freq = 'monthly'

        if stat not in ['std', 'mean']:
            self.logger.warning(f"Statistic '{stat}' not recognized. Set to 'mean' by default.")
            stat = 'mean'

        freq_dict = {'monthly':'time.month',
                     'annual': 'time.year'}

        ensure_istype(computed_data, xr.DataArray, logger=self.logger)

        self.logger.debug(f"Computing '{stat}' for '{freq}' frequency")

        if 'time' not in computed_data.dims:
            raise ValueError(f"Cannot compute '{stat}' as 'time' dimension not present in data.")

        # select time, if None, the whole time will be taken in one or both boundaries
        computed_data = computed_data.sel(time=slice(self.startdate, self.enddate))

        if stat == 'std':
            return computed_data.groupby(freq_dict[freq]).std('time')
        else:
            return computed_data.groupby(freq_dict[freq]).mean('time')

    def _compute_seasonal_cycle(self, monthly_data):
        """
        Converts monthly data into a seasonal cycle by grouping over calendar months
        and computing the mean across the time dimension.

        Args:
            monthly_data (xarray.DataArray or list of xarray.DataArray): 
                Monthly time series data to be converted to seasonal cycles.
                If a list is provided, the operation is applied to each item in the list.

        Returns:
            xarray.DataArray or list of xarray.DataArray:
                The seasonal cycle(s), where each output DataArray has dimensions 
                grouped by calendar month and averaged over time. Returns 
                `None` if input is `None`.
        """
        if monthly_data is None:
            return None
            
        def _group_bymonth(arr):
            if 'time' not in arr.coords:
                raise KeyError("Cannot compute seasonal cycle as 'time' coordinate is missing.")
            return arr.groupby('time.month').mean('time')

        return [_group_bymonth(da) for da in monthly_data] if isinstance(monthly_data, list) else _group_bymonth(monthly_data)
    
    def add_seaice_attrs(self, da_seaice_computed: xr.DataArray, region: str,
                         startdate: str=None, enddate: str=None, std_flag=False):
        """
        Adds metadata attributes to a computed sea ice DataArray. This function assigns descriptive attributes 
        to an xr.DataArray representing computed sea ice (extent or volume) for a specific region and time period.

        Args:
            da_seaice_computed (xr.DataArray): The computed sea ice data to which attributes will be added.
            region (str): The geographical region over which sea ice data is computed.
            startdate (str, optional): The start date of the data (format "YYYY-MM-DD"). Default to None.
            enddate (str, optional): The end date of the data (format "YYYY-MM-DD"). Default to None.
            std_flag (bool, optional): If True, add the metadata related to the computed standard deviation. 
                Defaults to False.

        Returns:
            xr.DataArray
        """
        ensure_istype(da_seaice_computed, xr.DataArray, logger=self.logger)

        # set attributes: 'method','unit'  
        units_dict = {"extent": "million km^2",
                      "volume": "thousands km^3",
                      "fraction": "[0-1]",
                      "thickness": "m"}

        if self.method not in units_dict:
            raise NoDataError("Variable not found in dataset")
        else:
            da_seaice_computed.attrs["units"] = units_dict.get(self.method)

        da_seaice_computed.attrs["long_name"] = (f"{'Std ' if std_flag else ''}Sea ice {self.method} "
                                                 f"{'integrated ' if self.method in ['extent', 'volume'] else ''}"
                                                 f"over {da_seaice_computed.attrs['AQUA_region']}")
        da_seaice_computed.attrs["standard_name"] = f"{region}_{'std_' if std_flag else ''}sea_ice_{self.method}"
        da_seaice_computed.attrs["AQUA_method"] = f"{self.method}"
        if startdate is not None: da_seaice_computed.attrs["AQUA_startdate"] = f"{startdate}"
        if enddate is not None: da_seaice_computed.attrs["AQUA_enddate"] = f"{enddate}"
        da_seaice_computed.name = f"{'std_' if std_flag else ''}sea_ice_{self.method}_{region}"

        return da_seaice_computed

    def save_netcdf(self, seaice_data, diagnostic: str, diagnostic_product: str = None,
                    rebuild: bool = True, output_file: str = None,
                    **kwargs):
        """
        Save the computed sea ice data to a NetCDF file.

        Args:
            seaice_data (xr.DataArray or xr.Dataset): The computed sea ice metric data.
            diagnostic (str): The diagnostic name. It is expected 'SeaIce' for this class.
            diagnostic_product (str, optional): The diagnostic product. Can be used for namig the file more freely.
            rebuild (bool, optional): If True, rebuild (overwrite) the NetCDF file. Default is True.
            output_file (str, optional): The output file name.
            **kwargs: Additional keyword arguments for saving the data.
        """
        # Use parent method to handle saving, including metadata
        super().save_netcdf(seaice_data, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                            outputdir=self.outputdir, rebuild=rebuild, **kwargs)
