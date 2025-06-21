import pandas as pd
import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic, convert_data_units
from aqua.util import select_season
from aqua.exceptions import NoDataError
from aqua.graphics import boxplot


class Boxplots(Diagnostic):
    def __init__(self, catalog=None, model=None, exp=None, source=None,
                var=None, startdate=None, enddate=None,
                save_netcdf=False, outputdir='./', loglevel='WARNING'):

        super().__init__(catalog=catalog, model=model, exp=exp, source=source,
                         startdate=startdate, enddate=enddate,
                         loglevel=loglevel)

        self.logger = log_configure(log_level=loglevel, log_name='Boxplots')
        self.var = var
        self.save_netcdf = save_netcdf
        self.outputdir = outputdir
    
    def retrieve_and_compute_fldmean(self, var: None, units: str = None) -> None:
        """
        Retrieve and preprocess dataset, selecting pressure level and/or converting units if needed.

        Args:
            var (str or list of str, optional): list of variables to retrieve. If None, uses self.var.
            units (str, optional): Target units (e.g., 'mm/day').

        Raises:
            NoDataError: If variable not found in dataset.
            KeyError: If the variable is missing from the data.
        """
         
        if var is not None:
            self.var = [v.lstrip('-') for v in (var if isinstance(var, list) else [var])] # Ensure var is a list

        super().retrieve(var=self.var)

        if self.data is None:
            self.logger.error(f"Variable {self.var} not found in dataset {self.model}, {self.exp}, {self.source}")
            raise NoDataError("Variable not found in dataset")

        #if self.var not in self.data:
         #   raise KeyError(f"Variable '{self.var}' not found in dataset variables: {list(self.data.data_vars)}")

        self.startdate = self.startdate or pd.to_datetime(self.data.time[0].values).strftime('%Y-%m-%d')
        self.enddate = self.enddate or pd.to_datetime(self.data.time[-1].values).strftime('%Y-%m-%d')

        if units:
            self.logger.info(f'Adjusting units for variable {self.var} to {units}.')
            self.data = convert_data_units(self.data, self.var, units, loglevel=self.loglevel)

        # Compute field means
        fldmeans = {}
        for var_name in self.var:
            if var_name not in self.data: 
                self.logger.warning(f"Variable {var_name} not found in dataset.")
                continue
            fldmean = self.data[var_name].aqua.fldmean()
            fldmeans[var_name] = fldmean

        self.fldmeans = xr.Dataset(fldmeans)
        self.logger.info(f"Field means computed for variables: {self.var}")

        self.fldmeans.attrs.update({
            'catalog': self.catalog,
            'model': self.model,
            'exp': self.exp,
            'startdate': str(self.startdate),
            'enddate': str(self.enddate)
            })

        # Save field means to NetCDF if required
        if self.save_netcdf:
            super().save_netcdf(
                data=fldmeans,
                diagnostic='boxplots',
                diagnostic_product='boxplot',
                default_path=self.outputdir,
                var=var
            )
            self.logger.info(f"Field means saved to {self.outputdir}.")
