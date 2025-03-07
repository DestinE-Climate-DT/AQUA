import pandas as pd
import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from .util import fix_precipitation_units, select_pressure_level

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
        loglevel (str): The log level to be used. Default is 'WARNING'.
        """
    def __init__(self, catalog=None, model=None, exp=None, source=None,
                regrid=None, startdate=None, enddate=None,
                plev=None, var=None, loglevel='WARNING'):
                
        super().__init__(catalog=catalog, model=model, exp=exp, source=source,
                        regrid=regrid, startdate=startdate, enddate=enddate,
                        loglevel=loglevel)

        self.logger = log_configure(log_level=loglevel, log_name='Global Biases')
        self.plev = plev
        self.var = var

    def retrieve_and_process(self, plev=None, var=None):
        """
        Retrieves and pre-processes the dataset, converting precipitation units if necessary 
        and selecting the specified pressure level.
        """
        self.var = var or self.var
        self.plev = plev or self.plev

        super().retrieve(var=self.var)
        
        if self.data is None:
            self.logger.error(f"Variable {var} not found in dataset {self.model}, {self.exp}, {self.source}")
            raise NoDataError("Variable not found in dataset")
        
        self.startdate = self.startdate or pd.to_datetime(self.data.time[0].values).strftime('%Y-%m-%d')
        self.enddate = self.enddate or pd.to_datetime(self.data.time[-1].values).strftime('%Y-%m-%d')
        
        if self.var in ['tprate', 'mtpr']:
            self.logger.info(f'Adjusting units for precipitation variable: {self.var}.')
            self.data = fix_precipitation_units(self.data, self.var)
        
        if self.plev is not None:
            self.logger.info(f'Selecting pressure level {self.plev} for variable {self.var}.')
            self.data = select_pressure_level(self.data, self.plev, self.var)
        elif 'plev' in self.data[self.var].dims:
            self.logger.warning(f"Variable {self.var} has multiple pressure levels but none selected. Skipping 2D plotting for bias maps.")



