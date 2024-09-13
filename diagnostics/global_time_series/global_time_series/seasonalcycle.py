"""
Module to extract the seasonal cycle of a variable from a time series.
"""
import os
import gc

import xarray as xr
from aqua.graphics import plot_seasonalcycle
from aqua.util import create_folder, add_pdf_metadata, time_to_string
from aqua.logger import log_configure

from .timeseries import Timeseries

xr.set_options(keep_attrs=True)


class SeasonalCycle(Timeseries):
    """
    Class to extract the seasonal cycle of a variable from a time series.
    """
    def __init__(self, var=None, formula=False,
                 catalogs=None,
                 models=None, exps=None, sources=None,
                 regrid=None, plot_ref=True,
                 plot_ref_kw={'catalog': 'obs',
                              'model': 'ERA5',
                              'exp': 'era5',
                              'source': 'monthly'},
                 startdate=None, enddate=None,
                 std_startdate=None, std_enddate=None,
                 plot_kw={'ylim': {}},
                 save=True,
                 outdir='./',
                 outfile=None,
                 longname=None, units=None,
                 lon_limits=None, lat_limits=None,
                 loglevel='WARNING'):
        """
        Initialize the class.

        Args:
            var: the variable to extract the seasonal cycle
            formula: if True a formula is evaluated from the variable string.
            catalogs (list or str, opt): the catalogs to search for the data
            models: the list of models to analyze
            exps: the list of experiments to analyze
            sources: the list of sources to analyze
            regrid: the regridding resolution. If None or False, no regridding is performed.
            plot_ref: if True, plot the reference seasonal cycle. Default is True.
            plot_ref_kw: the keyword arguments to pass to the reference plot.
                         Default is {'model': 'ERA5', 'exp': 'era5', 'source': 'monthly'}
            startdate: the start date of the time series
            enddate: the end date of the time series
            std_startdate: the start date to evaluate the standard deviation
            std_enddate: the end date to evaluate the standard deviation
            plot_kw: the keyword arguments to pass to the plot
            save: if True, save the figure. Default is True.
            outdir: the output directory
            outfile: the output file
            longname: the long name of the variable. Override the attribute in the data file.
            units: the units of the variable. Override the attribute in the data file.
            lon_limits (list): Longitude limits of the area to evaluate. Default is None.
            lat_limits (list): Latitude limits of the area to evaluate. Default is None.
            loglevel: the logging level. Default is 'WARNING'.
        """
        super().__init__(var=var, formula=formula,
                         catalogs=catalogs,
                         models=models, exps=exps, sources=sources,
                         monthly=True, annual=False,
                         regrid=regrid, plot_ref=plot_ref,
                         plot_ref_kw=plot_ref_kw,
                         startdate=startdate, enddate=enddate,
                         monthly_std=True, annual_std=False,
                         std_startdate=std_startdate, std_enddate=std_enddate,
                         plot_kw=plot_kw,
                         save=save,
                         outdir=outdir,
                         outfile=outfile,
                         longname=longname, units=units,
                         lon_limits=lon_limits, lat_limits=lat_limits,
                         loglevel=loglevel)
        # Change the logger name
        self.logger = log_configure(log_level=loglevel, log_name="SeasonalCycle")
        self.logger.info("SeasonalCycle class initialized")

        self.retrieve_data = super().retrieve_data
        self.clean_timeseries = super().cleanup
        if plot_ref:
            self.cycle_ref = None

    def retrieve_ref(self):
        """
        Retrieve the reference data.
        Overwrite the method in the parent class.
        """
        super(SeasonalCycle, self).retrieve_ref(extend=False)
        self.logger.debug(f"Time range of the reference data: {self.ref_mon.time.values[0]} to {self.ref_mon.time.values[-1]}")
        self.ref_mon = self.ref_mon.sel(time=slice(self.std_startdate, self.std_enddate))
        self.logger.debug(f"Time range of the reference data after slicing: {self.ref_mon.time.values[0]} to {self.ref_mon.time.values[-1]}")  # noqa: E501

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
        description_timerange = []

        for i, model in enumerate(self.models):
            self.logger.info(f"Processing {model} {self.exps[i]}")

            # We save here the time range of the data before compressing it to monthly means
            description_timerange.append(f" from {time_to_string(self.data_mon[i].time.values[0])} to {time_to_string(self.data_mon[i].time.values[-1])}")  # noqa: E501

            # Extract the seasonal cycle
            self.cycle[i] = self.data_mon[i].groupby('time.month').mean('time')

        self.cycle_ref = self.ref_mon.groupby('time.month').mean('time')
        self.description_timerange = description_timerange

    def plot(self):
        """Plot the seasonal cycle."""

        labels = []
        for i, model in enumerate(self.models):
            labels.append(f"{model} {self.exps[i]}")

        ref_label = f"{self.plot_ref_kw['model']}"

        try:
            title = self.data_mon[0].attrs['long_name'] + ' (' + self.data_mon[0].attrs['units'] + ') seasonal cycle'
        except KeyError:
            title = f'{self.var} seasonal cycle'

        if self.lon_limits is not None or self.lat_limits is not None:
            title += ' for region'
            if self.lon_limits is not None:
                title += f' lon: {self.lon_limits}'
            if self.lat_limits is not None:
                title += f' lat: {self.lat_limits}'

        fig, _ = plot_seasonalcycle(data=self.cycle,
                                    ref_data=self.cycle_ref,
                                    std_data=self.ref_mon_std,
                                    data_labels=labels,
                                    ref_label=ref_label,
                                    loglevel=self.loglevel,
                                    title=title)

        if self.save:
            self.save_seasonal_pdf(fig, ref_label)
            self.save_seasonal_netcdf()

    def save_seasonal_pdf(self, fig, ref_label):
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
            self.outfile = f'global_time_series_seasonalcycle_{self.var}'
            for i, model in enumerate(self.models):
                if self.catalogs[i] is not None:
                    self.outfile += f'_{self.catalogs[i]}'
                self.outfile += f'_{model}_{self.exps[i]}'
            if self.lon_limits is not None:
                self.outfile += f'_lon{self.lon_limits[0]}_{self.lon_limits[1]}'
            if self.lat_limits is not None:
                self.outfile += f'_lat{self.lat_limits[0]}_{self.lat_limits[1]}'
            if self.plot_ref:
                self.outfile += f'_{ref_label}'
            self.outfile += '.pdf'
        self.logger.debug(f"Outfile: {self.outfile}")
        fig.savefig(os.path.join(outfig, self.outfile))

        description = f"Seasonal cycle of the global mean of {self.var}"
        for i, model in enumerate(self.models):
            description += f" for {model} {self.exps[i]}"
            description += self.description_timerange[i]
        if self.plot_ref:
            description += f" with {ref_label} as reference,"
            try:
                description += f" std evaluated from {time_to_string(self.std_startdate)} to {time_to_string(self.std_enddate)}"  # noqa: E501
            except ValueError:
                description += f" std evaluated from {time_to_string(self.ref_mon.time.values[0])} to {time_to_string(self.ref_mon.time.values[-1])}"  # noqa: E501
        description += "."
        if self.lon_limits is not None or self.lat_limits is not None:
            description += " The data have been averaged over a region defined by"
            if self.lon_limits is not None:
                description += f" longitude limits {self.lon_limits}"
            if self.lat_limits is not None:
                description += f" latitude limits {self.lat_limits}"
            description += "."
        self.logger.debug(f"Description: {description}")
        add_pdf_metadata(filename=os.path.join(outfig, self.outfile),
                         metadata_value=description)

    def save_seasonal_netcdf(self):
        """
        Save the seasonal cycle to a netcdf file
        """
        self.logger.info("Saving seasonal cycle to netcdf")
        outdir = os.path.join(self.outdir, 'netcdf')
        create_folder(outdir, self.loglevel)

        for i, model in enumerate(self.models):
            outfile = f'global_time_series_seasonalcycle_{self.var}'
            if self.catalogs[i] is not None:
                outfile += f'_{self.catalogs[i]}'
            outfile += f'_{model}_{self.exps[i]}.nc'
            try:
                self.cycle[i].to_netcdf(os.path.join(outdir, outfile))
            except Exception as e:
                self.logger.error(f"Error while saving netcdf {outdir}/{outfile}: {e}")

        if self.plot_ref:
            outfile = f'global_time_series_seasonalcycle_{self.var}_{self.plot_ref_kw["model"]}.nc'
            try:
                self.cycle_ref.to_netcdf(os.path.join(outdir, outfile))
            except Exception as e:
                self.logger.error(f"Error while saving netcdf {outdir}/{outfile}: {e}")

    def cleanup(self):
        """Clean up the data."""
        self.clean_timeseries()

        del self.cycle

        if self.plot_ref:
            del self.cycle_ref

        gc.collect()
