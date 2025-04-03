""" Seaice doc """
import os
import xarray as xr

from aqua.diagnostics.core import Diagnostic
from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure, log_history
from aqua.util import ConfigPath, OutputSaver
from aqua.util import load_yaml, area_selection, to_list
from aqua.diagnostics.timeseries import Timeseries
from aqua.util import frequency_string_to_pandas
from aqua.diagnostics.seaice.util import load_region_file

xr.set_options(keep_attrs=True)

class SeaIce(Diagnostic):
    """Class for seaice objects.
    This class provides methods to compute sea ice extent and volume over specified regions.
    It also allows for the integration of masked data and the calculation of standard deviations.
    Args:
        model   (str): The model name.
        exp     (str): The experiment name.
        source  (str): The data source.
        catalog (str, optional): The catalog name.
        regrid  (str, optional): The regrid option.
        startdate (str, optional): The start date for the data.
        enddate   (str, optional): The end date for the data.
        regions   (list, optional): A list of regions to analyze. Default is ['Arctic', 'Antarctic'].
        regions_file (str, optional): The path to the regions definition file.
        loglevel     (str, optional): The logging level. Default is 'WARNING'.
        regions_definition (dict): The loaded regions definition from the YAML file.
    Methods:
        load_regions(regions_file=None, regions=None):
            Loads region definitions from a .yaml configuration file and sets the regions.
        show_regions():
            Show the regions available in the region file.
        add_seaice_attrs(da_seaice_computed: xr.DataArray, method: str, region: str, startdate: str=None, enddate: str=None, std_flag=False):
            Set attributes for seaice_computed.
        integrate_seaice_masked_data(masked_data, method: str, region: str):
            Integrate the masked data over the spatial dimension to compute sea ice metrics.
        _calculate_std(computed_data: xr.DataArray, freq: str = None):
            Compute the standard deviation of the data integrated using the extent or volume method and grouped by a specified time frequency.
        _compute_extent(threshold: float = 0.15, var: str = 'siconc', calc_std_freq: str = None):
            Compute sea ice extent by integrating sea ice concentration data over specified regions.
        _compute_volume(var: str = 'sithick', calc_std_freq: str = None):
            Compute sea ice volume by integrating computed data over specified regions.
        compute_seaice(method: str, *args, **kwargs):
            Execute the seaice diagnostic based on the specified method. """

    def __init__(self, model: str, exp: str, source: str,        
                 catalog=None,
                 regrid=None, 
                 startdate=None, enddate=None,
                 regions=['Arctic', 'Antarctic'],
                 regions_file=None,
                 loglevel: str = 'WARNING'
                 ):

        super().__init__(model=model, exp=exp, source=source,
                         regrid=regrid, catalog=catalog, 
                         startdate=startdate, enddate=enddate,
                         loglevel=loglevel)
        self.logger = log_configure(loglevel, 'SeaIce')

        # check region file and defined regions 
        self.load_regions(regions_file=regions_file, regions=regions)

    def load_regions(self, regions_file=None, regions=None):
        """ Loads region definitions from a .yaml configuration file and sets the regions.
            If no regions are provided, it uses all available regions from the configuration.
        Args:
            regions_file (str): Full path to the region file. If None, a default path is used.
            regions (list): A region or list of str with regions name to load. If None, all regions are used.
        """
        # load the region file
        self.regions_definition = load_region_file(regions_file=regions_file)

        # check if specific regions were provided
        if regions is None:
            self.logger.warning('No regions defined. Using all available regions.')
            self.regions = list(self.regions_definition.keys())
        else:
            # if the region is not in the regions_definition file, it will be added to the invalid_regions list.
            # convert regions to a list if a single string was provided to avoid chars splitting
            invalid_regions = [reg for reg in to_list(regions) if reg not in self.regions_definition.keys()]

            # if any invalid regions are found, raise an error
            if invalid_regions:
                raise ValueError(f"Invalid region name(s): [{', '.join(f'{i}' for i in invalid_regions)}]. "
                                 f"Please check the region file at: '{regions_file}'.")
            
            self.regions = to_list(regions)

    def show_regions(self):
        """Show the regions available in the region file. Method for the user."""
        # check regions_definition is defined in the class or not None
        if not hasattr(self, 'regions_definition') or self.regions_definition is None:
            raise ValueError("No regions_definition found.")
        return dict(self.regions_definition)

    def add_seaice_attrs(self, da_seaice_computed: xr.DataArray, method: str, region: str,
                          startdate: str=None, enddate: str=None, std_flag=False) -> xr.DataArray:
        """ Adds metadata attributes to a computed sea ice DataArray. This function assigns descriptive attributes 
        to an xr.DataArray representing computed sea ice (extent or volume) for a specific region and time period.
        Args:
            da_seaice_computed (xr.DataArray): The computed sea ice data to which attributes will be added.
            method (str): The computation method used. Must be either "extent" or "volume".
            region (str): The geographical region over which sea ice data is computed.
            startdate (str, optional): The start date of the data (format "YYYY-MM-DD"). Default to None.
            enddate (str, optional): The end date of the data (format "YYYY-MM-DD"). Default to None.
            std_flag (bool, optional): If True, add the metadata related to the computed standard deviation. 
                Defaults to False.
        Returns:
            xr.DataArray
        """
        # set attributes: 'method','unit'   
        units_dict = {"extent": "million km^2",
                      "volume": "thousands km^3"}

        if method not in units_dict:
            raise NoDataError("Variable not found in dataset")
        else:
            da_seaice_computed.attrs["units"] = units_dict.get(method)

        da_seaice_computed.attrs["long_name"] = f"{'Std ' if std_flag else ''}Sea ice {method} integrated over region {region}"
        da_seaice_computed.attrs["standard_name"] = f"{region}_{'std_' if std_flag else ''}sea_ice_{method}"
        da_seaice_computed.attrs["AQUA_method"] = f"{method}"
        da_seaice_computed.attrs["AQUA_region"] = f"{region}"
        if startdate is not None: da_seaice_computed.attrs["AQUA_startdate"] = f"{startdate}"
        if enddate is not None: da_seaice_computed.attrs["AQUA_enddate"] = f"{enddate}"
        da_seaice_computed.name = f"{'std_' if std_flag else ''}sea_ice_{method}_{region.replace(' ', '_').lower()}"
        return da_seaice_computed

    def get_area_cells(self, region: str) -> xr.DataArray:
        """ Get areacello and select by provided region.
        Args:
            region (str): The region for which select the area cells.
        Returns:
            xr.DataArray: The area grid cells (m^2) selected by region coordinates.
        """
        self.logger.debug(f'Calculate cell areas for {region}')

        # get xr.DataArray with info on grid area that must be reinitialised for each region. 
        # # Note: areacello units are in (m^2)
        areacello = self.reader.grid_area

        # get the region box from the region definition file
        box = self.regions_definition[region]

        # make area selection flexible to lon values from -180 to 180 or from 0 to 360
        try:
            lonmin = round(areacello.lon.min().values/180)*180
            lonmax = round(areacello.lon.max().values/180)*180
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

        # regional selection with box, use default dict to set the search for lon bounds as above and lat from -90 to 90
        areacello = area_selection(areacello, lat=[box["latS"], box["latN"]], lon=[box["lonW"], box["lonE"]], 
                                   default={"lon_min": lonmin, "lon_max": lonmax, "lat_min": -90, "lat_max": 90},
                                   loglevel=self.loglevel)
        return areacello

    def integrate_seaice_masked_data(self, masked_data, method: str, region: str):
        """Integrate the masked data over the spatial dimension to compute sea ice metrics.
        Args:
            masked_data (xr.DataArray): The masked data to be integrated.
            method (str): The method to compute sea ice metrics. Options are 'extent' or 'volume'.
            region (str): The region for which the sea ice metric is computed.
        Returns:
            xr.DataArray: The computed sea ice metric.
        """

        # get areacells of region
        areacello = self.get_area_cells(region)

        # log the computation
        self.logger.info(f'Computing sea ice {method} for {region}')

        if method == 'extent':
            # compute sea ice extent: exclude areas with no sea ice and sum over the spatial dimension, divide by 1e12 to convert to million km^2
            seaice_metric = areacello.where(masked_data.notnull()).sum(skipna = True, min_count = 1, 
                                                                       dim=self.reader.space_coord) / 1e12
            # keep attributes from the retrieved data
            seaice_metric.attrs = masked_data.attrs
        elif method == 'volume':
            # compute sea ice volume: exclude areas with no sea ice and sum over the spatial dimension, divide by 1e12 to convert to thousand km^3
            seaice_metric = (masked_data * areacello.where(masked_data.notnull())).sum(skipna = True, min_count = 1, 
                                                                                       dim=self.reader.space_coord) / 1e12
        # add attributes
        seaice_metric = self.add_seaice_attrs(seaice_metric, method, region)

        return seaice_metric

    def _calculate_std(self, computed_data: xr.DataArray, freq: str = None):
        """ Compute the standard deviation of the data integrated using the extent or volume method and 
        grouped by a specified time frequency (`monthly` or `annual`).
        Args:
            computed_data (xarray.DataArray): 
                The input data on which the standard deviation will be computed.
            freq (str, optional): The time frequency for grouping before computing the standard deviation. 
                Must be one of (If `None`, an error is raised and set defaults to 'monthly'):
                - 'monthly' (computes std per month)
                - 'annual'  (computes std per year)        
        Returns:
            xarray.DataArray: A DataArray containing the computed standard deviation. """

        if freq is None:
            self.logger.error('Frequency not provided')
            raise ValueError( 'Frequency not provided')

        if freq not in ['monthly', 'annual']:
            self.logger.error(f"Frequency str: '{freq}' not recognized. Assign freq to 'monthly' by default.")
            freq = 'monthly'

        freq_dict = {'monthly':'time.month',
                     'annual': 'time.year'}

        if not isinstance(computed_data, xr.DataArray):
            self.logger.error('Input data is not an xarray DataArray')
            raise ValueError( 'Input data is not an xarray DataArray')

        self.logger.debug(f'Computing standard deviation for frequency: {freq}')

        if 'time' not in computed_data.dims:
            raise ValueError("Cannot compute std: 'time' dimension not present in data.")

        # select time, if None, the whole time will be taken in one or both boundaries
        computed_data = computed_data.sel(time=slice(self.startdate, self.enddate))
        # calculate the standard deviation using the frequency freq
        computed_data_std = computed_data.groupby(freq_dict[freq]).std('time')

        return computed_data_std

    def _compute_seasonal_cycle(self, monthly_data):
        """ Converts monthly data into a seasonal cycle by grouping over calendar months
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
        if isinstance(monthly_data, list):
            if 'time' not in monthly_data[0].coords:
                raise KeyError("Cannot compute seasonal cycle as 'time' coordinate is missing.")
            return [da.groupby('time.month').mean('time') for da in monthly_data]
        else:
            if 'time' not in monthly_data.coords:
                raise KeyError("Cannot compute seasonal cycle as 'time' coordinate is missing.")
            return monthly_data.groupby('time.month').mean('time')

    def _compute_extent(self, threshold: float = 0.15, var: str = 'siconc', 
                        calc_std_freq: str = None, 
                        get_seasonal_cycle: bool = False):
        """ Compute sea ice extent by integrating sea ice concentration data over specified regions.
        The sea ice extent is calculated by applying a threshold to the sea ice concentration variable
        and summing the masked data over the regional spatial dimension. If a standard deviation 
        calculation frequency (`calc_std_freq`) is provided, the standard deviation of the extent is also computed.
        It also derives the seasonal cycle (monthly climatology) of the computed extent values and std.
        Args:
            threshold (float, optional): 
                The threshold value for sea ice concentration above which a grid cell is considered 
                part of the sea ice extent. Default is 0.15.
            var (str, optional): 
                The name of the sea ice concentration variable in the dataset. Default is 'siconc'.
            calc_std_freq (str, optional): 
                The frequency for computing the standard deviation of sea ice extent across time (i.e., 'monthly', 'annual'). 
                If None, standard deviation is not computed. Default is None.
            get_seasonal_cycle (bool, optional):
                If True, the output extent (and standard deviation if computed) is converted into a 
                seasonal cycle i.e. a monthly climatology. Defaults to False.
        Returns:
            xr.Dataset or Tuple[xr.Dataset, xr.Dataset]: 
                - If `calc_std_freq` is None, returns a dataset containing the integrated sea ice extent.
                - If `calc_std_freq` is provided, returns a tuple containing:
                    1. `self.extent` (xr.Dataset): The computed sea ice extent.
                    2. `self.extent_std` (xr.Dataset): The std deviation of sea ice extent with specified frequency.
        Notes:
            - Standard deviation is computed across all years if `calc_std_freq` is provided."""

        # retrieve data with Diagnostic method
        super().retrieve(var=var)

        if self.data is None:
            self.logger.error(f"Variable {var} not found in dataset {self.model}, {self.exp}, {self.source}")
            raise NoDataError("Variable not found in dataset")

        # get the sea ice concentration mask
        ci_mask = self.data[var].where((self.data[var] > threshold) &
                                       (self.data[var] < 1.0))
        
        # make a list to store the extent DataArrays for each region
        regional_extents = []
        # make a list to store the standard deviation of extent DataArrays for each region across all years 
        regional_extents_std = [] if calc_std_freq else None

        for region in self.regions:

            # integrate the seaice masked data ci_mask over the regional spatial dimension to compute sea ice extent
            seaice_extent = self.integrate_seaice_masked_data(ci_mask, 'extent', region)

            # make a deepcopy to compute seasonal cycle to avoid losing time coord
            original_si_extent = seaice_extent.copy(deep=True)
            
            log_history(seaice_extent, "Method used for seaice computation: 'extent'")

            if get_seasonal_cycle:
                seaice_extent = self._compute_seasonal_cycle(seaice_extent)
                log_history(seaice_extent, "Data converted to seasonal means, grouped by month")

            # add attributes and history
            seaice_extent = self.add_seaice_attrs(seaice_extent, 'extent', region, self.startdate, self.enddate)
            
            regional_extents.append(seaice_extent)
        
            # compute standard deviation if frequency is provided
            if calc_std_freq is not None:

                # if not seasonal_cycle, original seaice_extent data is needed to calc std
                seaice_std_extent = self._calculate_std(original_si_extent, calc_std_freq)
                log_history(seaice_std_extent, f"Method used for standard deviation seaice computation: extent")

                # update attributes and history
                seaice_std_extent = self.add_seaice_attrs(seaice_std_extent, 'extent', region,
                                                          self.startdate, self.enddate, std_flag=True)
                self.logger.debug("Attributes updated")                    

                regional_extents_std.append(seaice_std_extent)
                
        # combine the extent DataArrays into a single Dataset and keep as global attributes 
        # only the attrs that are shared across all DataArrays
        self.extent = xr.merge(regional_extents, combine_attrs='drop_conflicts')
        
        # merge the standard deviation DataArrays if computed
        self.extent_std = xr.merge(regional_extents_std, combine_attrs='drop_conflicts') if calc_std_freq else None
        
        # return a tuple if standard deviation was computed, otherwise just the extent
        return (self.extent, self.extent_std) if calc_std_freq else self.extent

    def _compute_volume(self, var: str = 'sithick', 
                        calc_std_freq: str = None, 
                        get_seasonal_cycle: bool = False):
        """Compute sea ice volume by integrating computed data over specified regions."""

        # retrieve data with Diagnostic method
        super().retrieve(var=var)

        if self.data is None:
            self.logger.error(f"Variable {var} not found in dataset {self.model}, {self.exp}, {self.source}")
            raise NoDataError("Variable not found in dataset")

        # get the sea ice volume mask
        sivol_mask = self.data[var].where((self.data[var] > 0) &
                                          (self.data[var] < 99.0))

        # make a list to store the volume DataArrays for each region
        regional_volumes = []
        # make a list to store the standard deviation of volume DataArrays for each region across all years 
        regional_volumes_std = [] if calc_std_freq else None

        for region in self.regions:

            # integrate the seaice masked data sivol_mask over the regional spatial dimension to compute sea ice volume
            seaice_volume = self.integrate_seaice_masked_data(sivol_mask, 'volume', region)

            # make a deepcopy to compute seasonal cycle to avoid losing time coord
            original_si_volume = seaice_volume.copy(deep=True)

            log_history(seaice_volume, f"Method used for seaice computation: 'volume'")

            if get_seasonal_cycle:
                seaice_volume = self._compute_seasonal_cycle(seaice_volume)
                log_history(seaice_volume, "Data converted to seasonal means, grouped by month")

            # add attributes and history
            seaice_volume = self.add_seaice_attrs(seaice_volume, 'volume', region, self.startdate, self.enddate)

            regional_volumes.append(seaice_volume)

            # compute standard deviation if frequency is provided
            if calc_std_freq is not None:
                
                seaice_std_volume = self._calculate_std(original_si_volume, calc_std_freq)
                log_history(seaice_std_volume, f"Method used for standard deviation seaice computation: volume")

                # update attributes and history
                seaice_std_volume = self.add_seaice_attrs(seaice_std_volume, 'volume', region, 
                                                          self.startdate, self.enddate, std_flag=True)
                self.logger.debug("Attributes updated")                    

                regional_volumes_std.append(seaice_std_volume)

        # combine the volume DataArrays into a single Dataset and keep as global attributes 
        # only the attrs that are shared across all DataArrays
        self.volume = xr.merge(regional_volumes, combine_attrs='drop_conflicts')

        # merge the standard deviation DataArrays if computed
        self.volume_std = xr.merge(regional_volumes_std, combine_attrs='drop_conflicts') if calc_std_freq else None

        # return a tuple if standard deviation was computed, otherwise just the volume
        return (self.volume, self.volume_std) if calc_std_freq else self.volume

    def compute_seaice(self, method: str, *args, **kwargs):
        """ Execute the seaice diagnostic based on the specified method.
        Args:
            var (str): The variable to be used for computation.
            method (str): The method to compute sea ice metrics. Options are 'extent' or 'volume'.
            Kwargs:
                - threshold (float): The threshold value for which sea ice fraction is considered. Default is 0.15.
                - var (str): The variable to be used for computation. Default is 'sithick' or 'siconc'.
        Returns:
            xr.DataArray or xr.Dataset: The computed sea ice metric. A Dataset is returned if multiple regions are requested."""

        # create a dictionary with the available methods associated with the corresponding function
        methods = {
            'extent': self._compute_extent,
            'volume': self._compute_volume,
            }

        # check if the method is valid and call the corresponding function if so
        if method not in methods:
                valid_methods = list(methods.keys())
                raise ValueError(f"Invalid method '{method}'. Please choose from: {valid_methods}")
        else:
            # call the function associated with the method
            return methods[method](*args, **kwargs)
    
    def save_netcdf(self, seaice_data, diagnostic: str, diagnostic_product: str = None,
                    default_path: str = '.', rebuild: bool = True, output_file: str = None,
                    output_dir: str = None, **kwargs):
        """ Save the computed sea ice data to a NetCDF file.
        Args:
            seaice_data (xr.DataArray or xr.Dataset): The computed sea ice metric data.
            diagnostic (str): The diagnostic name. It is expected 'SeaIce' for this class.
            diagnostic_product (str, optional): The diagnostic product. Can be used for namig the file more freely.
            default_path (str, optional): The default path for saving. Default is '.'.
            rebuild (bool, optional): If True, rebuild (overwrite) the NetCDF file. Default is True.
            output_file (str, optional): The output file name.
            output_dir (str, optional): The output directory.
            **kwargs: Additional keyword arguments for saving the data."""

        # Use parent method to handle saving, including metadata
        super().save_netcdf(seaice_data, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                            default_path=default_path, rebuild=rebuild, **kwargs)
