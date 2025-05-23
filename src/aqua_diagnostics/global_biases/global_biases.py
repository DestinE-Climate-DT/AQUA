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
        var (str): Name of the variable to analyze.
        plev (float): Pressure level to select.
        save_netcdf (bool): If True, climatologies will be saved as a NetCDF file.
        outputdir (str): The directory where the NetCDF file will be saved.
        loglevel (str): The log level to be used. Default is 'WARNING'.
        """
    def __init__(self, catalog=None, model=None, exp=None, source=None,
                regrid=None, startdate=None, enddate=None,
                var=None, plev=None,
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
        Retrieves and preprocesses the dataset, optionally selecting pressure level and converting units.

        Args:
            var (str, optional): Variable to retrieve. If None, uses the default from initialization.
            plev (float, optional): Pressure level to extract, if applicable.
            units (str, optional): Target units for variable conversion (e.g., 'mm/day').

        Raises:
            NoDataError: If the variable is not found in the dataset.
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


    def compute_climatology(self, data=None, var=None, plev=None, save_netcdf=True,
                        seasonal=False, seasons_stat='mean'):
        """
        Computes climatology (annual or seasonal) for the selected variable.

        Args:
            data (xarray.Dataset, optional): Input dataset. If None, uses self.data.
            var (str, optional): Variable name. If None, uses self.var.
            plev (float, optional): Pressure level (placeholder, not used).
            save_netcdf (bool, optional): If True, saves output to NetCDF. Default is True.
            seasonal (bool, optional): If True, computes seasonal climatology (DJF, MAM, JJA, SON).
            seasons_stat (str, optional): Statistic to apply to each season ('mean', 'std', 'max', 'min').

        Raises:
            ValueError: If an invalid `seasons_stat` is provided.
        """

        data = data or self.data
        var = var or self.var
        save_netcdf = save_netcdf or self.save_netcdf

        self.logger.info(f'Computing {"seasonal " if seasonal else ""}climatology for variable {var}.')

        self.climatology = xr.Dataset({var: data[var].mean(dim='time')})

        self.climatology.attrs['model'] = self.model
        self.climatology.attrs['exp'] = self.exp
        self.climatology.attrs['startdate'] = self.startdate
        self.climatology.attrs['enddate'] = self.enddate

        if save_netcdf:
            super().save_netcdf(
                data=self.climatology,
                diagnostic='global_biases',
                diagnostic_product='climatology',
                default_path=self.outputdir
            )

        if seasonal:
            stat_funcs = {'mean': 'mean', 'max': 'max', 'min': 'min', 'std': 'std'}
            if seasons_stat not in stat_funcs:
                raise ValueError("Invalid statistic. Please choose one of 'mean', 'std', 'max', 'min'.")

            season_list = ['DJF', 'MAM', 'JJA', 'SON']
            seasonal_data = []

            for season in season_list:
                data_season = select_season(data[var], season)
                data_stat = getattr(data_season, stat_funcs[seasons_stat])(dim='time')
                seasonal_data.append(data_stat.expand_dims(season=[season]))

            self.seasonal_climatology = xr.concat(seasonal_data, dim='season')
            self.seasonal_climatology = self.seasonal_climatology.to_dataset(name=var)

            self.seasonal_climatology.attrs['model'] = self.model
            self.seasonal_climatology.attrs['exp'] = self.exp
            self.seasonal_climatology.attrs['startdate'] = self.startdate
            self.seasonal_climatology.attrs['enddate'] = self.enddate

            if save_netcdf:
                super().save_netcdf(
                    data=self.seasonal_climatology,
                    diagnostic='global_biases',
                    diagnostic_product='seasonal_climatology',
                    default_path=self.outputdir
                )
                self.logger.info(f'Seasonal climatology saved to {self.outputdir}.')
