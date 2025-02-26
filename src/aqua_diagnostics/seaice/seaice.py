""" Seaice doc """
import os
import xarray as xr

from aqua.diagnostics.core import Diagnostic
from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure, log_history
from aqua.util import ConfigPath, OutputSaver
from aqua.util import load_yaml, area_selection, to_list
from aqua.diagnostics.timeseries import Timeseries

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
        
        # region file is the fullpath to the region file
        if regions_file is None:
            folderpath = ConfigPath().get_config_dir()
            regions_file = os.path.join(folderpath, 'diagnostics', 'seaice', 'config', 'regions_definition.yaml')

        # load the region file
        self.regions_definition = load_yaml(infile=regions_file)

        if regions is None:
            self.logger.warning('No regions defined. Using all available regions.')
            self.regions = list(self.regions_definition.keys())
        else:
            # if only one region is given as a string, it is converted to a list of strings with only one element
            # to avoid characters splitting. If the region is not in the regions_definition file, it will be added to the invalid_regions list.
            invalid_regions = [reg for reg in to_list(regions) if reg not in self.regions_definition.keys()]

            # If invalid regions are found, raise an error and list them
            if invalid_regions:
                raise ValueError(f"Invalid region name(s): [{', '.join(f'{i}' for i in invalid_regions)}]. Please check the region file at: f'{regions_file}'.")
            
            self.regions = to_list(regions)
        
        self.extent = None

    def seaice_timeseries(self):
        """Call the seaice_timeseries method from the Timeseries instance."""
        return self.timeseries.seaice_timeseries()

    def show_regions(self):
        """Show the regions available in the region file."""

        return dict(self.regions_definition)

    def get_regions_file_path(self):
        """Get and show the .yaml file path location, 
        as a new user may not know where the file is located."""
        pathloc = f"Regions file location path: {ConfigPath().get_config_dir()}"
        print(pathloc)

        return self.regions_definition

    @staticmethod
    def set_seaice_update_attrs(seaice_computed, method: str, region: str):
        """Set attributes for seaice_computed."""

        # set attributes: 'method','unit'   
        units_dict = {"extent": "million km^2", 
                      "volume": "thousands km^3"}

        if method not in units_dict:
            raise NoDataError("Variable not found in dataset")
        else:
            seaice_computed.attrs["units"] = units_dict.get(method)

        seaice_computed.attrs["long_name"] = f"Sea ice {method} integrated over {region} region"
        seaice_computed.attrs["standard_name"] = f"{region} sea ice {method}"
        seaice_computed.name = f"sea_ice_{method}_{region.replace(' ', '_').lower()}"

        log_history(seaice_computed, f"Method used for seaice computation: {method}")
        log_history(seaice_computed, f"Region selected for seaice computation: {region}")

        return seaice_computed

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
        elif method == 'volume':
            # compute sea ice metric: exclude areas with no sea ice and sum over the spatial dimension
            seaice_metric = (masked_data * areacello.where(masked_data.notnull())).sum(skipna = True, min_count = 1, 
                                                                                       dim=self.reader.space_coord) /1e12
        # add attributes
        self.set_seaice_update_attrs(seaice_metric, method, region)

        return seaice_metric

    def _compute_extent(self, threshold: float = 0.15, var: str = 'siconc'):
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
        for region in self.regions:
            
            # integrate the seaice masked data ci_mask over the regional spatial dimension to compute sea ice extent
            seaice_extent = self.integrate_seaice_masked_data(ci_mask, 'extent', region)

            # add attributes
            self.set_seaice_update_attrs(seaice_extent, 'extent', region)

            regional_extents.append(seaice_extent)
        
        # combine the extent DataArrays into a single Dataset and keep as global attributes 
        # only the attrs that are shared across all DataArrays
        self.extent = xr.merge(regional_extents, combine_attrs='drop_conflicts')
       
        return self.extent

    def _compute_volume(self, var: str = 'sithick'):
        """Compute sea ice volume."""

        # retrieve data with Diagnostic method
        super().retrieve(var=var)

        if self.data is None:
            self.logger.error(f"Variable {var} not found in dataset {self.model}, {self.exp}, {self.source}")
            raise NoDataError("Variable not found in dataset")

        # get the sea ice volume
        sivol_mask = self.data[var].where((self.data[var] > 0) &
                                          (self.data[var] < 99.0))

        # make a list to store the volume DataArrays for each region
        regional_volumes = []
        for region in self.regions:
            
            # integrate the seaice masked data sivol_mask over the regional spatial dimension to compute sea ice volume
            seaice_volume = self.integrate_seaice_masked_data(sivol_mask, 'volume', region)

            # add attributes
            self.set_seaice_update_attrs(seaice_volume, 'volume', region)

            regional_volumes.append(seaice_volume)

        # combine the volume DataArrays into a single Dataset and keep as global attributes 
        # only the attrs that are shared across all DataArrays
        self.volume = xr.merge(regional_volumes, combine_attrs='drop_conflicts')
       
        return self.volume

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
    
    def save_netcdf(self, seaice_computed, diagnostic: str, diagnostic_product: str = None,
                    default_path: str = '.', rebuild: bool = True, output_file: str = None,
                    output_dir: str = None, **kwargs):
        """ Save the computed sea ice data to a NetCDF file.

        Args:
            seaice_computed (xr.DataArray or xr.Dataset): The computed sea ice metric data.
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
        super().save_netcdf(seaice_computed, diagnostic=diagnostic, diagnostic_product=diagnostic_product,
                            default_path=default_path, rebuild=rebuild, **kwargs)

        return None
