import pandas as pd
import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic, convert_data_units
from aqua.util import select_season
from .util import select_pressure_level

xr.set_options(keep_attrs=True)

class GlobalBiases(Diagnostic):
    """
    Initialize the Base class.

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
        plev (float): Pressure level to select.
        var (str): Name of the variable to analyze.
        save_netcdf (bool): If True, the data will be saved as a NetCDF file.
        outputdir (str): The directory where the NetCDF file will be saved.
        loglevel (str): The log level to be used. Default is 'WARNING'.
        """
    def __init__(self, catalog=None, model=None, exp=None, source=None,
                regrid=None, startdate=None, enddate=None,
                plev=None, var=None, 
                save_netcdf=False, outputdir='./', loglevel='WARNING'):
                
        super().__init__(catalog=catalog, model=model, exp=exp, source=source,
                        regrid=regrid, startdate=startdate, enddate=enddate,
                        loglevel=loglevel)

        self.logger = log_configure(log_level=loglevel, log_name='Global Biases')
        self.plev = plev
        self.var = var
        self.save_netcdf = save_netcdf
        self.outputdir = outputdir
        self.startdate = startdate
        self.enddate = enddate

    def retrieve(self, var=None, plev=None, units=None):
        """
        Retrieves and pre-processes the dataset,    
        Optionally selects a pressure level and converts variable units. 

        Args:
            var (str): Variable to retrieve.
            plev (int): Pressure level to select.
            units (str) : Target units for conversion (e.g., 'mm/day').
        """
        self.var = var or self.var
        self.plev = plev or self.plev

        super().retrieve(var=self.var)
        
        if self.data is None:
            self.logger.error(f"Variable {var} not found in dataset {self.model}, {self.exp}, {self.source}")
            raise NoDataError("Variable not found in dataset")
        
        self.startdate = self.startdate or pd.to_datetime(self.data.time[0].values).strftime('%Y-%m-%d')
        self.enddate = self.enddate or pd.to_datetime(self.data.time[-1].values).strftime('%Y-%m-%d')
        
        if units is not None:
            self.logger.info(f'Adjusting units for variable {self.var} to {units}.')
            self.data = convert_data_units(self.data, self.var, units, loglevel=self.loglevel)
            
        if self.plev is not None:
            self.logger.info(f'Selecting pressure level {self.plev} for variable {self.var}.')
            self.data = select_pressure_level(self.data, self.plev, self.var)
        elif 'plev' in self.data[self.var].dims:
            self.logger.warning(f"Variable {self.var} has multiple pressure levels but none was selected")


    def compute_bias(self, data_ref, var=None): 
        """
        Computes the bias between two datasets.
        Args:
            data (xarray.DataArray): The dataset.
            data_ref (xarray.DataArray): The reference dataset.
            var (str): The variable to compute the bias for. If None, uses the class variable.
        Returns:
            xarray.DataArray: The bias between the two datasets.
        """
        var = var or self.var

        # Check if the variable has pressure levels but no specific level is selected
        if 'plev' in self.data.get(self.var, {}).dims:
            if self.plev is None:
                self.logger.warning(
                    f"Variable {self.var} has multiple pressure levels, but no specific level was selected. "
                    "Skipping 2D bias plotting."
                )
                return None 
        
            # If a pressure level is specified, select it
            self.logger.info(f"Selecting pressure level {self.plev} for variable {self.var}.")
            data = select_pressure_level(self.data, self.plev, self.var)
            data_ref = select_pressure_level(self.data_ref, self.plev, self.var)

        # If a pressure level is specified but the variable has no pressure levels
        elif self.plev is not None:
            self.logger.warning(f"Variable {self.var} does not have pressure levels!")

        self.bias = self.data[self.var].mean(dim='time') - data_ref[self.var].mean(dim='time')

        if self.save_netcdf:
            super().save_netcdf(data=self.bias, diagnostic='global_biases', diagnostic_product='bias', 
                                default_path=self.outputdir)

    def compute_seasonal_bias(self, data_ref, var=None, seasons_stat='mean'):
        """
        Computes the seasonal bias between two datasets.
        Args:
            data (xarray.DataArray): The dataset.
            data_ref (xarray.DataArray): The reference dataset.  
            var (str): The variable to compute the bias for. If None, uses the class variable.
            seasons_stat (str): The statistic to compute for each season.
                Options are 'mean', 'std', 'max', 'min'.
        Returns:
            xarray.Dataset: A dataset containing the seasonal biases for each season.
        """
        var = var or self.var

        # Check if the variable has pressure levels but no specific level is selected
        if 'plev' in self.data.get(self.var, {}).dims:
            if self.plev is None:
                self.logger.warning(
                    f"Variable {self.var} has multiple pressure levels, but no specific level was selected. "
                    "Skipping 2D bias plotting."
                )
                return None 
        
            # If a pressure level is specified, select it
            self.logger.info(f"Selecting pressure level {self.plev} for variable {self.var}.")
            data = select_pressure_level(self.data, self.plev, self.var)
            data_ref = select_pressure_level(self.data_ref, self.plev, self.var)

        # If a pressure level is specified but the variable has no pressure levels
        elif self.plev is not None:
            self.logger.warning(f"Variable {self.var} does not have pressure levels!")


        stat_funcs = {'mean': 'mean', 'max': 'max', 'min': 'min', 'std': 'std'}
        if seasons_stat not in stat_funcs:
            raise ValueError("Invalid statistic. Please choose one of 'mean', 'std', 'max', 'min'.")

        self.seasonal_bias = {}
        seasons = ['DJF', 'MAM', 'JJA', 'SON']

        for season in seasons:
            # Select the seasonal data 
            seasonal_data = select_season(self.data[self.var], season)
            seasonal_data_ref = select_season(data_ref[self.var], season)
            # Compute seasonal statistics
            data_stat = getattr(seasonal_data, stat_funcs[seasons_stat])(dim='time')
            data_ref_stat = getattr(seasonal_data_ref, stat_funcs[seasons_stat])(dim='time')
            # Compute bias and store in dictionary
            bias = data_stat - data_ref_stat
            self.seasonal_bias[season] = bias

        # Combine seasonal biases into an xarray.Dataset
        self.seasonal_bias = xr.Dataset(self.seasonal_bias)
        
        if self.save_netcdf:
            super().save_netcdf(data=self.seasonal_bias, diagnostic='global_biases', diagnostic_product='seasonal_bias', 
                                default_path=self.outputdir)

    def compute_vertical_bias(self, data_ref, plev_min=None, plev_max=None, var=None):
        """
        Computes the vertical bias between two datasets.
        Args:
            data_ref (xarray.DataArray): The reference dataset.
            plev_min (float): Minimum pressure level to select. 
            plev_max (float): Maximum pressure level to select.
            var (str): The variable to compute the bias for. If None, uses the class variable.
        Returns:
            xarray.DataArray: The vertical bias between the two datasets.
        """
        
        var = var or self.var

        # Calculate the bias between the two datasets
        bias = self.data[self.var].mean(dim='time') - data_ref[self.var].mean(dim='time')

        # Filter pressure levels
        if plev_min is None:
            plev_min = bias['plev'].min().item()
        if plev_max is None:
            plev_max = bias['plev'].max().item()

        bias = bias.sel(plev=slice(plev_max, plev_min))

        # Calculate the zonal mean bias
        self.vertical_bias = bias.mean(dim='lon')

        if self.save_netcdf:
            super().save_netcdf(data=self.vertical_bias, diagnostic='global_biases', diagnostic_product='vertical_bias', 
                                default_path=self.outputdir)





