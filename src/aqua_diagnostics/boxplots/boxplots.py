import pandas as pd
import xarray as xr
from aqua.logger import log_configure
from aqua.diagnostics.core import Diagnostic
from aqua.util import select_season, to_list
from aqua.exceptions import NoDataError

class Boxplots(Diagnostic):
    """Class for computing and plotting boxplots of field means from climate model datasets.
    This class retrieves data from specified datasets, computes field means for given variables,
    and optionally saves the results to NetCDF files.
    Args:
        catalog (str): Catalog name.
        model (str): Model name.
        exp (str): Experiment name.
        source (str): Data source.
        var (str or list of str, optional): Variable(s) to retrieve. Defaults to None.
        startdate (str, optional): Start date for data retrieval. Defaults to None.
        enddate (str, optional): End date for data retrieval. Defaults to None.
        save_netcdf (bool, optional): Whether to save results as NetCDF files. Defaults to False.
        outputdir (str, optional): Directory to save output files. Defaults to './'.
        loglevel (str, optional): Logging level. Defaults to 'WARNING'.
    """
    def __init__(self,
                 catalog: str = None,
                 model: str = None,
                 exp: str = None,
                 source: str = None,
                 var: str | list[str] = None,
                 startdate: str = None,
                 enddate: str = None,
                 regrid: str = None,
                 save_netcdf: bool = False,
                 outputdir: str = './',
                 loglevel: str = 'WARNING'):

        super().__init__(catalog=catalog, model=model, exp=exp, source=source,
                         startdate=startdate, enddate=enddate, regrid=regrid,
                         loglevel=loglevel)

        self.logger = log_configure(log_level=loglevel, log_name='Boxplots')
        self.var = var
        self.save_netcdf = save_netcdf
        self.outputdir = outputdir
        self.loglevel = loglevel

    def run(self, var: None, save_netcdf=False, units: str = None) -> None:
        """
        Retrieve and preprocess dataset, selecting pressure level and/or converting units if needed.

        Args:
            var (str or list of str, optional): list of variables to retrieve. If None, uses self.var.
            units (str or list of str, optional): Target units (e.g., 'mm/day').

        Raises:
            NoDataError: If variable not found in dataset.
            KeyError: If the variable is missing from the data.
        """

        if var is not None:
            self.var = [v.lstrip('-') for v in (var if isinstance(var, list) else [var])]

        super().retrieve(var=self.var)

        if self.data is None:
            self.logger.error(f"Variable {self.var} not found in dataset {self.model}, {self.exp}, {self.source}")
            raise NoDataError("Variable not found in dataset")

        self.startdate = self.startdate or pd.to_datetime(self.data.time[0].values).strftime('%Y-%m-%d')
        self.enddate = self.enddate or pd.to_datetime(self.data.time[-1].values).strftime('%Y-%m-%d')

        self.save_netcdf = save_netcdf or self.save_netcdf

        # Unit check and conversion
        if units:
            units = to_list(units)
            if len(units) != len(self.var):
                raise ValueError(f"Length of 'units' ({len(units)}) must match number of variables ({len(self.var)})")

            for var_name, target_unit in zip(self.var, units):
                current_units = self.data[var_name].attrs.get('units')
                if current_units:
                    self.data[var_name] = super()._check_data(data=self.data[var_name], var=var_name, units=target_unit)

        # Compute field means
        fldmeans = {}
        for var_name in self.var:
            if var_name not in self.data: 
                self.logger.warning(f"Variable {var_name} not found in dataset.")
                continue
            fldmean = self.reader.fldmean(self.data[var_name])
            fldmeans[var_name] = fldmean

        self.fldmeans = xr.Dataset(fldmeans)
        self.logger.info(f"Field means computed for variables: {self.var}")

        self.fldmeans.attrs.update({
            'AQUA_catalog': self.catalog,
            'AQUA_model': self.model,
            'AQUA_exp': self.exp,
            'startdate': str(self.startdate),
            'enddate': str(self.enddate)
            })

        # Save field means to NetCDF if required
        if self.save_netcdf:
            self.logger.info(self.var)
            var_string = (
                        '_'.join(self.var) if isinstance(self.var, list)
                        else self.var if isinstance(self.var, str)
                        else None
                        )
            extra_keys = {'var': var_string} if var_string else {}
            super().save_netcdf(
                data=self.fldmeans,
                diagnostic='boxplots',
                diagnostic_product='boxplot',
                outdir=self.outputdir,
                extra_keys=extra_keys
                )
            self.logger.info(f"Field means saved to {self.outputdir}.")
