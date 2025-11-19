import pandas as pd
import xarray as xr
from aqua.logger import log_configure
from aqua.util import select_season, convert_data_units
from aqua.diagnostics.core import Diagnostic
from aqua.fixer import EvaluateFormula
from aqua.exceptions import NoDataError
from .util import handle_pressure_level


xr.set_options(keep_attrs=True)


class GlobalBiases(Diagnostic):
    """
    Diagnostic class for computing global and seasonal climatologies of a given variable.

    This class handles data retrieval, pressure level selection, unit conversion, 
    and computation of mean climatologies (total or seasonal).

    Inherits from `Diagnostic`.

    Args:
        catalog (str): The catalog to be used. If None, inferred from Reader.
        model (str): Model to be used.
        exp (str): Experiment name.
        source (str): Source name.
        regrid (str): Target grid for regridding. If None, no regridding.
        startdate (str): Start date for data selection.
        enddate (str): End date for data selection.
        var (str): Variable name to analyze.
        plev (float): Pressure level to select (if applicable).
        diagnostic (str): Name of the diagnostic.
        save_netcdf (bool): If True, saves output climatologies.
        outputdir (str): Output directory for NetCDF files.
        loglevel (str): Log level. Default is 'WARNING'.
    """
    def __init__(self, catalog=None, model=None, exp=None, source=None,
                 regrid=None, startdate=None, enddate=None,
                 var=None, plev=None,
                 diagnostic='globalbiases',
                 save_netcdf=True, outputdir='./', loglevel='WARNING'):

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
        self.diagnostic = diagnostic

    def _check_data(self, var: str, units: str):
        """
        Make sure that the data is in the correct units.

        Args:
            var (str): The variable to be checked.
            units (str): The units to be checked.
        """
        self.data[self.var] = super()._check_data(data=self.data[self.var], var=var, units=units)


    def retrieve(self, var: str = None, formula: bool = False,
                 long_name: str = None, short_name: str = None,
                 plev: float = None, units: str = None,
                 reader_kwargs: dict = {}) -> None:
        """
        Retrieve and preprocess dataset, selecting pressure level and/or converting units if needed.

        Args:
            var (str, optional): Variable to retrieve. If None, uses self.var.
            formula (bool): If True, the variable is a formula.
            long_name (str): The long name of the variable, if different from the variable name.
            short_name (str): The short name of the variable, if different from the variable name.
            plev (float, optional): Pressure level to extract.
            units (str): The units of the variable, if different from the original units.
            reader_kwargs (dict, optional): Additional keyword arguments for the Reader.
        Raises:
            NoDataError: If variable not found in dataset.
            KeyError: If the variable is missing from the data.
        """
        if var is not None:
            self.var = var   
        if formula:
            super().retrieve()
            self.logger.info("Evaluating formula: %s", self.var)
            formula_values = EvaluateFormula(data=self.data, formula=self.var, long_name=long_name,
                                             short_name=short_name, units=units,
                                             loglevel=self.loglevel).evaluate()
            if formula_values is None:
                raise ValueError(f'Error evaluating formula {var}. '
                                 'Check the variable names and the formula syntax.')
            self.data[self.var] = formula_values
        else:
            super().retrieve(var=self.var, reader_kwargs=reader_kwargs)

        if self.data is None:
            self.logger.error("Data could not be retrieved for %s, %s, %s", self.AQUA_model, self.AQUA_exp, self.AQUA_source)
            raise NoDataError("No data retrieved.")

        # Customize metadata and attributes
        if units is not None:
            self._check_data(var=self.var, units=units)

        if short_name is not None:
            self.data = self.data.rename_vars({self.var: short_name})
            self.var = short_name
        else:
            self.data.attrs['short_name'] = self.var

        self.startdate = pd.to_datetime(self.startdate or self.data.time[0].values).strftime("%Y-%m-%d")
        self.enddate   = pd.to_datetime(self.enddate   or self.data.time[-1].values).strftime("%Y-%m-%d")
        if plev is not None:
            self.plev = plev

        # Final validation and pressure level handling
        if self.var:
            if self.var not in self.data.data_vars:
                raise KeyError(f"Variable '{self.var}' not found in dataset. Available variables: {list(self.data.data_vars)}")

            if self.plev is not None:
                self.logger.info("Selecting pressure level %s for variable '%s'.", self.plev, self.var)
                self.data = handle_pressure_level(self.data, self.var, self.plev, loglevel=self.loglevel)
            elif 'plev' in self.data[self.var].dims:
                self.logger.warning("Variable '%s' has multiple pressure levels, but none was specified.", self.var)
        else:
            self.logger.info("All variables retrieved; no variable-specific operations applied.")

    def savenetcdf(self, data: xr.Dataset, diagnostic_product: str, 
                    rebuild: bool = True, create_catalog_entry: bool = False, extra_keys = None,
                    dict_catalog_entry: dict = {'jinjalist': ['realization'],
                                                'wildcardlist': ['var']}):
        """
        data (xr.Dataset): Input dataset.
        diagnostic_product (str): The product name to be used in the filename (e.g., 'annual_climatology').
        rebuild (bool): If True, rebuild the data from the original files.
        create_catalog_entry (bool): If True, create a catalog entry for the data. Default is False.
        extra_keys (dict): Extra keys for filename generation.
        dict_catalog_entry (dict): A dictionary with catalog entry information. 
            Default is {'jinjalist': ['freq', 'region', 'realization'], 'wildcardlist': ['var']}.
        """
        super().save_netcdf(data=data,
                diagnostic=self.diagnostic,
                diagnostic_product=diagnostic_product,
                outputdir=self.outputdir,
                create_catalog_entry=create_catalog_entry,
                dict_catalog_entry=dict_catalog_entry,
                extra_keys=extra_keys)

    
    def compute_climatology(self,
                            data: xr.Dataset = None,
                            var: str = None,
                            plev: float = None,
                            save_netcdf: bool = None,
                            seasonal: bool = False,
                            seasons_stat: str = 'mean',
                            create_catalog_entry: bool = False
                            ) -> None:
        """
        Compute total and optionally seasonal climatology for a variable.

        Args:
            data (xarray.Dataset, optional): Input dataset. If None, uses self.data.
            var (str, optional): Variable name. If None, uses self.var.
            plev (float, optional): Pressure level (currently unused).
            save_netcdf (bool, optional): If True, save output to NetCDF.
            seasonal (bool): If True, compute seasonal climatology (DJF, MAM, JJA, SON).
            seasons_stat (str): Aggregation statistic: 'mean', 'std', 'max', 'min'.
            create_catalog_entry (bool): If True, create a catalog entry for the data. Default is False.
        Raises:
            ValueError: If `seasons_stat` is invalid.
        """
        data = data or self.data
        var = var or self.var

        if save_netcdf is None:
            save_netcdf = self.save_netcdf

        if data is None:
            raise ValueError("No data provided or retrieved; cannot compute climatology.")

        self.logger.info(f'Computing climatology for variable {var}.')

        self.climatology = xr.Dataset({var: data[var].mean(dim='time')})
        self.climatology.attrs.update({
            'AQUA_catalog': self.catalog,
            'AQUA_model': self.model,
            'AQUA_exp': self.exp,
            'AQUA_realization': self.realization,
            'startdate': str(self.startdate),
            'enddate': str(self.enddate)
        })

        # Load data in memory for faster plot
        self.logger.debug(f"Loading climatology data in memory")
        self.climatology.load()
        self.logger.debug(f"Loaded climatology data in memory")

        if save_netcdf:
            extra_keys = {
                k: v for k, v in {
                    'var': var,
                    'plev': plev,
                }.items() if v is not None
            }
            self.savenetcdf(
                data=self.climatology,
                diagnostic_product='annual_climatology',
                create_catalog_entry=create_catalog_entry,
                extra_keys=extra_keys
            )

        if seasonal:
            stat_funcs = {'mean': 'mean', 'max': 'max', 'min': 'min', 'std': 'std'}
            if seasons_stat not in stat_funcs:
                raise ValueError("Invalid statistic. Choose one of 'mean', 'std', 'max', 'min'.")

            self.logger.info(f'Computing seasonal climatology for variable {var}.')

            season_list = ['DJF', 'MAM', 'JJA', 'SON']
            seasonal_data = []

            for season in season_list:
                season_data = select_season(data[var], season)
                season_stat = getattr(season_data, stat_funcs[seasons_stat])(dim='time')
                seasonal_data.append(season_stat.expand_dims(season=[season]))

            self.seasonal_climatology = xr.concat(seasonal_data, dim='season', coords='different').to_dataset(name=var)
            self.seasonal_climatology.attrs.update({
                'AQUA_catalog': self.catalog,
                'AQUA_model': self.model,
                'AQUA_exp': self.exp,
                'AQUA_realization': self.realization,
                'startdate': str(self.startdate),
                'enddate': str(self.enddate)
            })

            # Load data in memory for faster plot
            self.logger.debug(f"Loading seasonal climatology data in memory")
            self.seasonal_climatology.load()
            self.logger.debug(f"Loaded seasonal climatology data in memory")

            if save_netcdf:
                extra_keys = {k: v for k, v in [('var', var), ('plev', plev)] if v is not None}
                self.savenetcdf(
                    data=self.climatology,
                    diagnostic_product='seasonal_climatology',
                    create_catalog_entry=create_catalog_entry,
                    extra_keys=extra_keys
                )
                self.logger.info(f'Seasonal climatology saved to {self.outputdir}.')
