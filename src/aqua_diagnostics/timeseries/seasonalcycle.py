"""
Module to extract the seasonal cycle of a variable from a time series.
"""
import os
import gc

import xarray as xr
from aqua.graphics import plot_seasonalcycle
from aqua.util import time_to_string, OutputSaver
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
                 save_netcdf=True,
                 outdir='./',
                 longname=None, units=None,
                 lon_limits=None, lat_limits=None,
                 loglevel='WARNING', rebuild=None, filename_keys=None,
                 save_pdf=True, save_png=True, dpi=300):
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
            save_netcdf: if True, save the NetCDF. Default is True.
            outdir: the output directory
            longname: the long name of the variable. Override the attribute in the data file.
            units: the units of the variable. Override the attribute in the data file.
            lon_limits (list): Longitude limits of the area to evaluate. Default is None.
            lat_limits (list): Latitude limits of the area to evaluate. Default is None.
            loglevel: the logging level. Default is 'WARNING'.
            rebuild (bool, optional): If True, overwrite the existing files. If False, do not overwrite. Default is True.
            filename_keys (list, optional): List of keys to keep in the filename.
                                            Default is None, which includes all keys (see OutputNamer class).
            save_pdf (bool): If True, save the figure as a PDF. Default is True.
            save_png (bool): If True, save the figure as a PNG. Default is True.
            dpi (int, optional): Dots per inch (DPI) for saving figures. Default is 300.
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
                         save_netcdf=save_netcdf,
                         outdir=outdir,
                         longname=longname, units=units,
                         lon_limits=lon_limits, lat_limits=lat_limits,
                         loglevel=loglevel,
                         rebuild=rebuild, filename_keys=filename_keys,
                         save_pdf=save_pdf, save_png=save_png, dpi=dpi)
        # Change the logger name
        self.logger = log_configure(log_level=loglevel, log_name="SeasonalCycle")
        self.logger.info("SeasonalCycle class initialized")

        self.retrieve_data = super().retrieve_data
        self.clean_timeseries = super().cleanup
        if plot_ref:
            self.cycle_ref = None

        self.diagnostic_product = 'seasonalcycle'
        self.diagnostic = 'timeseries'


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

        if self.save_pdf or self.save_png:
            self.save_seasonal_image(fig, ref_label)
        if self.save_netcdf:
            self.save_seasonal_netcdf()

    def save_seasonal_image(self, fig, ref_label):
        """
        Save the figure to an image file (PDF/PNG).

        Args:
            fig (matplotlib.figure.Figure): Figure to save
            ref_label (str): Label for the reference data
        """
        output_saver = self._get_output_saver(catalog=self.catalogs[0], model=self.models[0], exp=self.exps[0])
        common_save_args = self._construct_save_args()

        description = self._construct_description(ref_label)
        self.logger.debug(f"Description: {description}")

        metadata = {"Description": description}

        if self.save_pdf:
            output_saver.save_pdf(fig, metadata=metadata, **common_save_args)
        if self.save_png:
            output_saver.save_png(fig, metadata=metadata, **common_save_args)

    def save_seasonal_netcdf(self):
        """Save the seasonal cycle to a NetCDF file."""
        for i, model in enumerate(self.models):
            output_saver = self._get_output_saver(catalog=self.catalogs[i], model=model, exp=self.exps[i])
            common_save_args = self._construct_save_args()
            # Pop the 'dpi' key from the dictionary
            common_save_args.pop('dpi', None)

            output_saver.save_netcdf(self.cycle[i], **common_save_args)

        if self.plot_ref:
            output_saver_ref = self._get_output_saver(model=self.plot_ref_kw['model'], exp=self.plot_ref_kw['exp'])
            output_saver_ref.save_netcdf(self.cycle_ref, **common_save_args)

    def _get_output_saver(self, catalog=None, model=None, exp=None):
        """
        Create and return an OutputSaver instance.

        Args:
            catalog (str): Catalog to use.
            model (str): Model identifier.
            exp (str): Experiment identifier.

        Returns:
            OutputSaver: An instance of the OutputSaver class.
        """
        # TODO: CHeck if this can be done by using the similar method from the parent class
        return OutputSaver(diagnostic=self.diagnostic, catalog=catalog, model=model, exp=exp,
                           loglevel=self.loglevel, default_path=self.outdir, rebuild=self.rebuild,
                           filename_keys=self.filename_keys)

    def _construct_save_args(self):
        """
        Construct common save arguments for image and NetCDF files.

        Returns:
            dict: Dictionary containing the common save arguments.
        """
        common_save_args = {'diagnostic_product': self.diagnostic_product, 'var': self.var, 'dpi': self.dpi}

        if self.plot_ref:
            common_save_args.update({'model_2': self.plot_ref_kw['model'], 'exp_2': self.plot_ref_kw['exp']})
        if self.lon_limits is not None:
            lon_limits = f'_lon{self.lon_limits[0]}_{self.lon_limits[1]}'
            common_save_args.update({'lon_limits': lon_limits})
        if self.lat_limits is not None:
            lat_limits = f'_lat{self.lat_limits[0]}_{self.lat_limits[1]}'
            common_save_args.update({'lat_limits': lat_limits})

        return common_save_args

    def _construct_description(self, ref_label):
        """
        Construct a description string for the metadata.

        Args:
            ref_label (str): Label for the reference data.

        Returns:
            str: Constructed description.
        """
        description = f"Seasonal cycle of the global mean of {self.var}"

        for i, model in enumerate(self.models):
            description += f" for {model} {self.exps[i]}"
            description += self.description_timerange[i]
        if self.plot_ref:
            description += f" with {ref_label} as reference,"
            try:
                description += f" std evaluated from {time_to_string(self.std_startdate)} to {time_to_string(self.std_enddate)}"
            except ValueError:
                description += f" std evaluated from {time_to_string(self.ref_mon.time.values[0])} to {time_to_string(self.ref_mon.time.values[-1])}"
        description += "."
        if self.lon_limits is not None or self.lat_limits is not None:
            description += " The data have been averaged over a region defined by"
            if self.lon_limits is not None:
                description += f" longitude limits {self.lon_limits}"
            if self.lat_limits is not None:
                description += f" latitude limits {self.lat_limits}"
            description += "."

        return description

    def cleanup(self):
        """Clean up the data."""
        self.clean_timeseries()

        del self.cycle

        if self.plot_ref:
            del self.cycle_ref

        gc.collect()
