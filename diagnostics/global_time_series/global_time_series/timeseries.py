import os
import gc

import xarray as xr
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NoObservationError, NoDataError
from aqua.util import eval_formula, create_folder
from aqua.util import add_pdf_metadata, time_to_string
from aqua.graphics import plot_timeseries

from .reference_data import get_reference_timeseries

xr.set_options(keep_attrs=True)


class Timeseries():
    """
    Plot a time series of the global mean value of a given variable.
    By default monthly and annual time series are plotted,
    the annual mean is plotted as dashed line and reference data is included,
    ERA5 by default, with the option to plot the standard deviation as uncertainty quantification.
    A model, exp, source string or list of strings can be passed.
    If a list is passed, the time series will be plotted for each combination
    of model, exp and source.
    """
    def __init__(self, var=None, formula=False,
                 models=None, exps=None, sources=None,
                 monthly=True, annual=True,
                 regrid=None, plot_ref=True,
                 plot_ref_kw={'model': 'ERA5',
                              'exp': 'era5',
                              'source': 'monthly'},
                 startdate=None, enddate=None,
                 monthly_std=True, annual_std=True,
                 std_startdate=None, std_enddate=None,
                 plot_kw={'ylim': {}},
                 save=True,
                 outdir='./',
                 outfile=None,
                 loglevel='WARNING'):
        """
        Args:
            var (str): Variable name.
            formula (bool): (Optional) If True, try to derive the variable from other variables.
                            Default is False.
            models (list or str): Model IDs.
            exps (list or str): Experiment IDs.
            sources (list or str): Source IDs.
            regrid (str): Optional regrid resolution. Default is None.
            plot_ref (bool): Include reference data. Default is True.
            plot_ref_kw (dict): Keyword arguments passed to `get_reference_timeseries`.
            startdate (str): Start date. Default is None.
            enddate (str): End date. Default is None.
            annual (bool): Plot annual mean. Default is True.
            monthly_std (bool): Plot monthly standard deviation. Default is True.
            annual_std (bool): Plot annual standard deviation. Default is True.
            std_startdate (str): Start date for standard deviation. Default is "1991-01-01".
            std_enddate (str): End date for standard deviation. Default is "2020-12-31".
            plot_kw (dict): Additional keyword arguments passed to the plotting function.
            save (bool): Save the figure. Default is True.
            outdir (str): Output directory. Default is "./".
            outfile (str): Output file name. Default is None.
            loglevel (str): Log level. Default is "WARNING".
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Timeseries')

        self.var = var
        self.formula = formula

        self.models = models
        self.exps = exps
        self.sources = sources

        if self.models is None or self.exps is None:
            raise NoDataError("No model or exp provided")
        if isinstance(self.models, str):
            self.models = [self.models]
        if isinstance(self.exps, str):
            self.exps = [self.exps]
        if isinstance(self.sources, str):
            self.sources = [self.sources]

        self.monthly = monthly
        self.annual = annual
        self.regrid = regrid

        self.plot_ref = plot_ref
        self.ref_mon = None
        self.ref_mon_std = None
        self.ref_ann = None
        self.ref_ann_std = None
        self.plot_ref_kw = plot_ref_kw
        self.monthly_std = monthly_std
        self.annual_std = annual_std
        self.std_startdate = std_startdate
        self.std_enddate = std_enddate

        self.startdate = startdate
        self.enddate = enddate

        self.plot_kw = plot_kw

        self.save = save
        if self.save is False:
            self.logger.info("Figure will not be saved")
        self.outdir = outdir
        self.outfile = outfile

    def run(self):
        """Retrieve ref, retrieve data and plot"""
        self.retrieve_data()
        self.retrieve_ref()
        self.plot()
        if self.save:
            self.save_netcdf()
        self.cleanup()

    def retrieve_ref(self):
        """Retrieve reference data"""
        if self.plot_ref:
            self.logger.debug('Retrieving reference data')
            try:
                self.ref_mon, self.ref_mon_std, self.ref_ann, self.ref_ann_std =\
                    get_reference_timeseries(var=self.var,
                                             formula=self.formula,
                                             **self.plot_ref_kw,  # model,exp,source
                                             startdate=self.startdate,
                                             enddate=self.enddate,
                                             std_startdate=self.std_startdate,
                                             std_enddate=self.std_enddate,
                                             regrid=self.regrid,
                                             monthly=self.monthly,
                                             annual=self.annual,
                                             monthly_std=self.monthly_std,
                                             annual_std=self.annual_std,
                                             loglevel=self.loglevel)
            except NoObservationError:
                self.plot_ref = False
                self.logger.warning('Reference data not found, skipping reference data')
        else:
            self.logger.debug('Reference data not requested')

    def retrieve_data(self):
        """
        Retrieve data from the list and store them in a list
        of xarray.DataArray
        """
        self.logger.debug('Retrieving data')
        self.data_mon = []
        self.data_annual = []

        if self.startdate is None:
            startdate = None
        if self.enddate is None:
            enddate = None

        for i, model in enumerate(self.models):
            self.logger.info(f'Retrieving data for {model} {self.exps[i]} {self.sources[i]}')
            try:
                reader = Reader(model=model, exp=self.exps[i], source=self.sources[i],
                                startdate=self.startdate, enddate=self.enddate,
                                regrid=self.regrid, loglevel=self.loglevel)
                if self.formula:
                    data = reader.retrieve()
                    self.logger.debug(f"Evaluating formula for {self.var}")
                    data = eval_formula(self.var, data)
                else:
                    data = reader.retrieve(var=self.var)
                    data = data[self.var]
            except Exception as e:
                self.logger.debug(f"Error while retrieving: {e}")
                self.logger.warning(f"No data found for {model} {self.exps[i]} {self.sources[i]}")

            if self.startdate is None:
                if startdate is None:
                    startdate = data.time[0].values
                else:
                    startdate = min(startdate, data.time[0].values)
            if self.enddate is None:
                if enddate is None:
                    enddate = data.time[-1].values
                else:
                    enddate = max(enddate, data.time[-1].values)

            if self.monthly:
                if 'monthly' in self.sources[i] or 'mon' in self.sources[i]:
                    self.logger.debug(f"No monthly resample needed for {model} {self.exps[i]} {self.sources[i]}")
                    data_mon = data
                else:
                    data_mon = reader.timmean(data, freq='MS', exclude_incomplete=True)
                data_mon = reader.fldmean(data_mon)
                self.logger.info("Monthly data retrieved")
                self.data_mon.append(data_mon)

            if self.annual:
                data_ann = reader.timmean(data, freq='YS',
                                          exclude_incomplete=True,
                                          center_time=True)
                data_ann = reader.fldmean(data_ann)
                self.logger.info("Annual data retrieved")
                self.data_annual.append(data_ann)

            # Clean up
            del reader
            del data
            gc.collect()

        if self.startdate is None:
            self.logger.debug(f"Start date: {startdate}")
            self.startdate = startdate
        if self.enddate is None:
            self.enddate = enddate
            self.logger.debug(f"End date: {enddate}")

        if self.data_mon == [] and self.data_annual == []:
            raise NoDataError("No data found")

    def plot(self):
        """
        Call an external function using the data to plot
        """
        self.logger.info("Plotting the timeseries")

        data_labels = []
        for i, model in enumerate(self.models):
            data_labels.append(f'{model} {self.exps[i]}')

        if self.plot_ref:
            try:
                ref_label = self.plot_ref_kw['model']
            except KeyError:
                ref_label = 'Reference'
        else:
            ref_label = None

        title = f'{self.var} timeseries'

        fig, ax = plot_timeseries(monthly_data=self.data_mon,
                                  annual_data=self.data_annual,
                                  ref_monthly_data=self.ref_mon,
                                  ref_annual_data=self.ref_ann,
                                  std_monthly_data=self.ref_mon_std,
                                  std_annual_data=self.ref_ann_std,
                                  ref_label=ref_label,
                                  data_labels=data_labels,
                                  title=title)

        if self.save:
            self.save_pdf(fig, ref_label)

    def save_pdf(self, fig, ref_label):
        """
        Save the figure to a pdf file

        Args:
            fig (matplotlib.figure.Figure): Figure to save
            ref_label (str): Label for the reference data
        """
        self.logger.info("Saving figure to pdf")

        outfig = os.path.join(self.outdir, 'pdf')
        self.logger.debug(f"Saving figure to {outfig}")
        create_folder(outfig, self.loglevel)
        if self.outfile is None:
            self.outfile = f'global_time_series_timeseries_{self.var}'
            for i, model in enumerate(self.models):
                self.outfile += f'_{model}_{self.exps[i]}'
            if self.plot_ref:
                self.outfile += f'_{ref_label}'
            self.outfile += '.pdf'
        self.logger.debug(f"Outfile: {self.outfile}")
        fig.savefig(os.path.join(outfig, self.outfile))

        description = f"Time series of the global mean of {self.var}"
        description += f" from {time_to_string(self.startdate)} to {time_to_string(self.enddate)}"
        for i, model in enumerate(self.models):
            description += f" for {model} {self.exps[i]}"
        if self.plot_ref:
            description += f" with {ref_label} as reference,"
            description += f" std evaluated from {time_to_string(self.std_startdate)} to {time_to_string(self.std_enddate)}"
        self.logger.debug(f"Description: {description}")
        add_pdf_metadata(filename=os.path.join(outfig, self.outfile),
                         metadata_value=description)

    def save_netcdf(self):
        """
        Save the data to a netcdf file.
        Every model-exp-source combination is saved in a separate file.
        Reference data is saved in a separate file.
        Std data is saved in a separate file.
        """
        outdir = os.path.join(self.outdir, 'netcdf')
        create_folder(outdir, self.loglevel)

        for i, model in enumerate(self.models):
            outfile = f'global_time_series_timeseries_{self.var}_{model}_{self.exps[i]}.nc'
            self.logger.debug(f"Saving data to {outdir}/{outfile}")
            if self.monthly is True:
                self.data_mon[i].to_netcdf(os.path.join(outdir, outfile))
            if self.annual is True:
                self.data_annual[i].to_netcdf(os.path.join(outdir, outfile))

        if self.plot_ref:
            outfile = f'global_time_series_timeseries{self.var}_ref.nc'
            self.logger.debug(f"Saving reference data to {outdir}/{outfile}")
            if self.monthly_std:
                self.ref_mon.to_netcdf(os.path.join(outdir, outfile))
            if self.annual_std:
                self.logger.debug(f"Saving annual data to {outdir}/{outfile}")
                self.ref_ann.to_netcdf(os.path.join(outdir, outfile))

            outfile = f'global_time_series_timeseries{self.var}_std.nc'
            self.logger.debug(f"Saving std data to {outdir}/{outfile}")
            if self.monthly_std:
                self.ref_mon_std.to_netcdf(os.path.join(outdir, outfile))
            if self.annual_std:
                self.ref_ann_std.to_netcdf(os.path.join(outdir, outfile))

    def cleanup(self):
        """Clean up"""
        self.logger.debug("Cleaning up")
        if self.monthly:
            del self.data_mon
            if self.plot_ref:
                del self.ref_mon
                del self.ref_mon_std
        if self.annual:
            del self.data_annual
            if self.plot_ref:
                del self.ref_ann
                del self.ref_ann_std
        gc.collect()
        self.logger.debug("Cleaned up")
