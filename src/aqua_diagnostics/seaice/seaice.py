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

xr.set_options(keep_attrs=True)

class SeaIce(Diagnostic):
    """Class for seaice objects."""

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
        """
        Loads region definitions from a .yaml configuration file and sets the regions.
        If no regions are provided, it uses all available regions from the configuration.
        
        Args:
            regions_file (str): Full path to the region file. If None, a default path is used.
            regions (list): A region or list of str with regions name to load. If None, all regions are used.
        """
        # determine the region file path if not provided
        if regions_file is None:
            folderpath = ConfigPath().get_config_dir()
            regions_file = os.path.join(folderpath, 'diagnostics', 'seaice', 'config', 'regions_definition.yaml')

        # load the region file
        self.regions_definition = load_yaml(infile=regions_file)

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

    @staticmethod
    def set_seaice_update_attrs(seaice_computed, method: str, region: str, std_flag=False):
        """Set attributes for seaice_computed."""

        # set attributes: 'method','unit'   
        units_dict = {"extent": "million km^2", 
                      "volume": "thousands km^3"}

        if method not in units_dict:
            raise NoDataError("Variable not found in dataset")
        else:
            seaice_computed.attrs["units"] = units_dict.get(method)

        seaice_computed.attrs["long_name"] = f"{'Std ' if std_flag else ''}Sea ice {method} integrated over region {region}"
        seaice_computed.attrs["standard_name"] = f"{region}_{'std_' if std_flag else ''}sea_ice_{method}"
        seaice_computed.attrs["method"] = f"{method}"
        seaice_computed.attrs["region"] = f"{region}"
        seaice_computed.name = f"{'std_' if std_flag else ''}sea_ice_{method}_{region.replace(' ', '_').lower()}"

    def integrate_seaice_masked_data(self, masked_data, method: str, region: str):
        """Integrate the masked data over the spatial dimension to compute sea ice metrics.

        Args:
        masked_data (xr.DataArray): The masked data to be integrated.
        method (str): The method to compute sea ice metrics. Options are 'extent' or 'volume'.
        region (str): The region for which the sea ice metric is computed.

        Returns:
        xr.DataArray: The computed sea ice metric.
        """

        self.logger.debug(f'Calculate cell areas for {region}')

        # get info on grid area that must be reinitialised for each region
        areacello = self.reader.grid_area

        # get the region box from the region definition file
        box = self.regions_definition[region]

        # log the computation
        self.logger.info(f'Computing sea ice {method} for {region}')

        # regional selection
        areacello = area_selection(areacello, lat=[box["latS"], box["latN"]], lon=[box["lonW"], box["lonE"]], 
                                   loglevel=self.loglevel)
        if method == 'extent':
            # compute sea ice extent: exclude areas with no sea ice and sum over the spatial dimension, divide by 1e12 to convert to million km^2
            seaice_metric = areacello.where(masked_data.notnull()).sum(skipna = True, min_count = 1, 
                                                                       dim=self.reader.space_coord) / 1e12
            # keep attributes from the retrieved data
            seaice_metric.attrs = masked_data.attrs
        elif method == 'volume':
            # compute sea ice volume: exclude areas with no sea ice and sum over the spatial dimension, , divide by 1e12 to convert to thousand km^3
            seaice_metric = (masked_data * areacello.where(masked_data.notnull())).sum(skipna = True, min_count = 1, 
                                                                                       dim=self.reader.space_coord) / 1e12
        # add attributes
        self.set_seaice_update_attrs(seaice_metric, method, region)

        return seaice_metric

    def _calculate_std(self, computed_data, freq=None):
        """Compute the standard deviation of the data computed using the extent or volume method. 
           Support for monthly and annual frequencies."""

        if freq is None:
            self.logger.error('Frequency not provided')
            raise ValueError( 'Frequency not provided')

        # check the frequency string and assign to an agreed name convention
        freq = frequency_string_to_pandas(freq)
        self.logger.debug(f"Checking {freq} standard deviation")

        if freq in ['MS', 'ME', 'M', 'mon', 'monthly']:
            freq = 'monthly'
        elif freq in ['YS', 'YE', 'Y', 'annual']:
            freq = 'annual'
        else:
            self.logger.error(f"Frequency str: '{freq}' not recognized")
            raise ValueError( f"Frequency str: '{freq}' not recognized")

        freq_dict = {'monthly':'time.month',
                     'annual': 'time.year'}

        if not isinstance(computed_data, xr.DataArray):
            self.logger.error('Input data is not an xarray DataArray')
            raise ValueError( 'Input data is not an xarray DataArray')

        self.logger.debug(f'Computing standard deviation for frequency: {freq}')

        # select time, if None, the whole time will be taken in one or both boundaries
        computed_data = computed_data.sel(time=slice(self.startdate, self.enddate))
        # calculate the standard deviation using the frequency freq
        computed_data_std = computed_data.groupby(freq_dict[freq]).std('time')

        return computed_data_std

    def _compute_extent(self, threshold: float = 0.15, var: str = 'siconc', calc_std_freq: str = None):
        """Compute sea ice extent.
        threshold (float): The threshold value for which sea ice fraction is considered . Default is 0.15.
        """

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

            # add attributes and history
            self.set_seaice_update_attrs(seaice_extent, 'extent', region)
            log_history(seaice_extent, "Method used for seaice computation: extent")

            regional_extents.append(seaice_extent)
        
            # compute standard deviation if frequency is provided
            if calc_std_freq is not None:

                seaice_std_extent = self._calculate_std(seaice_extent, calc_std_freq)

                # update attributes and history
                self.set_seaice_update_attrs(seaice_std_extent, 'extent', region, std_flag=True)
                log_history(seaice_std_extent, f"Method used for standard deviation seaice computation: extent")

                regional_extents_std.append(seaice_std_extent)

        # combine the extent DataArrays into a single Dataset and keep as global attributes 
        # only the attrs that are shared across all DataArrays
        self.extent = xr.merge(regional_extents, combine_attrs='drop_conflicts')
        
        # merge the standard deviation DataArrays if computed
        self.extent_std = xr.merge(regional_extents_std, combine_attrs='drop_conflicts') if calc_std_freq else None
        
        # return a tuple if standard deviation was computed, otherwise just the extent
        return (self.extent, self.extent_std) if calc_std_freq else self.extent

    def _compute_volume(self, var: str = 'sithick', calc_std_freq: str = None):
        """Compute sea ice volume."""

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

            # add attributes and history
            self.set_seaice_update_attrs(seaice_volume, 'volume', region)
            log_history(seaice_volume, f"Method used for seaice computation: 'volume'")

            regional_volumes.append(seaice_volume)

            # compute standard deviation if frequency is provided
            if calc_std_freq is not None:
                
                seaice_std_volume = self._calculate_std(seaice_volume, calc_std_freq)

                # update attributes and history
                self.set_seaice_update_attrs(seaice_std_volume, 'volume', region, std_flag=True)
                log_history(seaice_std_volume, f"Method used for standard deviation seaice computation: volume")

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

        Parameters:
        var (str): The variable to be used for computation.
        method (str): The method to compute sea ice metrics. Options are 'extent' or 'volume'.
        Kwargs:
            - threshold (float): The threshold value for which sea ice fraction is considered. Default is 0.15.
            - var (str): The variable to be used for computation. Default is 'sithick'.
        Returns:
        xr.DataArray or xr.Dataset: The computed sea ice metric. A Dataset is returned if multiple regions are requested.
        
        Raises:
        ValueError: If an invalid method is specified.
        """

        # create a dictionary with the available methods associated with the corresponding function
        methods = {
            'extent': self._compute_extent,
            'volume': self._compute_volume,
            }

        # check if the method is valid and call the corresponding function if so
        if method not in methods:
                valid_methods = ', '.join(f"'{key}'" for key in methods.keys())
                raise ValueError(f"Invalid method '{method}'. Please choose from: [ {valid_methods} ]")
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
            **kwargs: Additional keyword arguments for saving the data.

        Returns:
            None 
        """

        # Use parent method to handle saving, including metadata
        super().save_netcdf(seaice_data, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                            default_path=default_path, rebuild=rebuild, **kwargs)

        return None
