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
        self.var = var
        self.plev = plev
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

    def compute_climatology(self, save_netcdf=True):

        save_netcdf = save_netcdf or self.save_netcdf

        self.logger.info(f'Computing climatology for variable {self.var}.')

        self.climatology = xr.Dataset({self.var: self.data[self.var].mean(dim='time')})
    
        self.climatology.attrs['startdate'] = self.startdate
        self.climatology.attrs['enddate'] = self.enddate

        if save_netcdf:
            super().save_netcdf(data=self.climatology, diagnostic='global_biases', diagnostic_product='climatology', 
                                default_path=self.outputdir)

    def compute_seasonal_climatology(self, seasons_stat='mean', save_netcdf=True):

        save_netcdf = save_netcdf or self.save_netcdf

        self.logger.info(f'Computing seasonal climatology for variable {self.var}.')
        stat_funcs = {'mean': 'mean', 'max': 'max', 'min': 'min', 'std': 'std'}
        if seasons_stat not in stat_funcs:
            raise ValueError("Invalid statistic. Please choose one of 'mean', 'std', 'max', 'min'.")
        season_list = ['DJF', 'MAM', 'JJA', 'SON']
        seasonal_climatology = {}
        for season in season_list:
            data_season = select_season(self.data[self.var], season)
            data_stat = getattr(data_season, stat_funcs[seasons_stat])(dim='time')
            seasonal_climatology[season] = data_stat
        self.seasonal_climatology = xr.Dataset(seasonal_climatology)

        self.seasonal_climatology.attrs['startdate'] = self.startdate
        self.seasonal_climatology.attrs['enddate'] = self.enddate

        if save_netcdf:
            super().save_netcdf(data=self.seasonal_climatology, diagnostic='global_biases',
                                diagnostic_product='seasonal_climatology', default_path=self.outputdir)

