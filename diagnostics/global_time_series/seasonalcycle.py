"""
Module to extract the seasonal cycle of a variable from a time series.
"""
import os
import gc

from matplotlib import pyplot as plt
from aqua.graphics import plot_seasonalcycle
from aqua.util import create_folder, add_pdf_metadata
from aqua.logger import log_configure

from .timeseries import Timeseries


class SeasonalCycle(Timeseries):
    """
    Class to extract the seasonal cycle of a variable from a time series.
    """
    def __init__(self, var=None, formula=False,
                 models=None, exps=None, sources=None,
                 regrid=None, plot_ref=True,
                 plot_ref_kw={'model': 'ERA5',
                              'exp': 'era5',
                              'source': 'monthly'},
                 startdate=None, enddate=None,
                 std_startdate=None, std_enddate=None,
                 plot_kw={'ylim': {}},
                 outdir='./',
                 outfile=None,
                 loglevel='WARNING'):
        """
        Initialize the class.

        Args:
            var: the variable to extract the seasonal cycle
            formula: if True a formula is evaluated from the variable string.
            models: the list of models to analyze
            exps: the list of experiments to analyze
            sources: the list of sources to analyze

        """
        super().__init__(var=var, formula=formula,
                         models=models, exps=exps, sources=sources,
                         monthly=True, annual=False,
                         regrid=regrid, plot_ref=plot_ref,
                         plot_ref_kw=plot_ref_kw,
                         startdate=startdate, enddate=enddate,
                         monthly_std=True, annual_std=False,
                         std_startdate=std_startdate, std_enddate=std_enddate,
                         plot_kw=plot_kw,
                         outdir=outdir,
                         outfile=outfile,
                         loglevel=loglevel)
        # Change the logger name
        self.logger = log_configure(log_level=loglevel, log_name="SeasonalCycle")
        self.logger.info("SeasonalCycle class initialized")

        self.retrieve_data = super().retrieve_data
        self.retrieve_ref = super().retrieve_ref
        self.clean_timeseries = super().cleanup

        if plot_ref:
            self.cycle_ref = None

    def run(self):
        """Run the seasonal cycle extraction."""
        self.retrieve_data()
        self.retrieve_ref()
        self.seasonal_cycle()
        self.plot()
        self.cleanup()

    def seasonal_cycle(self):
        """Extract the seasonal cycle."""
        self.logger.info("Extracting the seasonal cycle")

        self.cycle = len(self.models) * [None]

        for i, model in enumerate(self.models):
            self.logger.info(f"Processing {model} {self.exps[i]}")

            # Extract the seasonal cycle
            self.cycle[i] = self.data_mon[i].groupby('time.month').mean('time')

        self.cycle_ref = self.ref_mon.groupby('time.month').mean('time')

    def plot(self):
        """Plot the seasonal cycle."""

        labels = []
        for i, model in enumerate(self.models):
            labels.append(f"{model} {self.exps[i]}")

        ref_label = f"{self.plot_ref_kw['model']}"

        title = f"Seasonal cycle of {self.var}"

        fig, ax = plot_seasonalcycle(data=self.cycle,
                                     ref_data=self.cycle_ref,
                                     std_data=self.ref_mon_std,
                                     data_labels=labels,
                                     ref_label=ref_label,
                                     loglevel=self.loglevel,
                                     title=title)

        # Save to outdir/pdf/filename
        outfig = os.path.join(self.outdir, 'pdf')
        self.logger.debug(f"Saving figure to {outfig}")
        create_folder(outfig, self.loglevel)
        if self.outfile is None:
            self.outfile = f'global_time_series_seasonalcycle_{self.var}'
            for i, model in enumerate(self.models):
                self.outfile += f'_{model}_{self.exps[i]}'
            if self.plot_ref:
                self.outfile += f'_{ref_label}'
            self.outfile += '.pdf'
        self.logger.debug(f"Outfile: {self.outfile}")
        fig.savefig(os.path.join(outfig, self.outfile))

        description = f"Seasonal cycle of the global mean of {self.var}"
        description += f" from {self.startdate} to {self.enddate}"
        for i, model in enumerate(self.models):
            description += f" for {model} {self.exps[i]}"
        if self.plot_ref:
            description += f" with {ref_label} as reference,"
            sd_format = self.startdate.astype('datetime64[s]').astype('O')
            sd_format = sd_format.strftime('%Y-%m-%d')
            ed_format = self.enddate.astype('datetime64[s]').astype('O')
            ed_format = ed_format.strftime('%Y-%m-%d')
            description += f" std evaluated from {sd_format} to {ed_format}"
        add_pdf_metadata(filename=os.path.join(outfig, self.outfile),
                         metadata_value=description)

    def cleanup(self):
        """Clean up the data."""
        self.clean_timeseries()

        del self.cycle

        if self.plot_ref:
            del self.cycle_ref

        gc.collect()
