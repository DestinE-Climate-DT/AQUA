"""Gregory plot module."""
import os
import gc

import matplotlib.pyplot as plt
import xarray as xr
from aqua import Reader
from aqua.logger import log_configure
from aqua.util import OutputSaver
from aqua.util import time_to_string, evaluate_colorbar_limits
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from .reference_data import get_reference_ts_gregory, get_reference_toa_gregory

xr.set_options(keep_attrs=True)  # Keep attributes after operations


class GregoryPlot():
    """
    Gregory plot class.
    Retrieve data, obs reference and plot the Gregory plot.
    Can work with a list of models, experiments and sources.
    """
    def __init__(self, catalogs=None,
                 models=None, exps=None, sources=None,
                 monthly=True, annual=True,
                 regrid=None,
                 ts_name='2t', toa_name=['mtnlwrf', 'mtnswrf'],
                 ts_std_start='1980-01-01', ts_std_end='2010-12-31',
                 toa_std_start='2001-01-01', toa_std_end='2020-12-31',
                 ref=True, save_netcdf=True,
                 outdir='./',
                 loglevel='WARNING',
                 rebuild=True, filename_keys=None,
                 save_pdf=True, save_png=True, dpi=300):
        """
        Args:
            catalogs (list, opt): List of catalogs to search for the data.
            models (list): List of model IDs.
            exps (list): List of experiment IDs.
            sources (list): List of source IDs.
            monthly (bool): If True, plot monthly data.
                            Default is True.
            annual (bool): If True, plot annual data.
                           Default is True.
            regrid (str): Optional regrid resolution.
                          Default is None.
            ts (str): variable name for 2m temperature, default is '2t'.
            toa (list): list of variable names for net radiation at TOA,
                        default is ['mtnlwrf', 'mtnswrf'].
            ts_std_start (str): Start date for standard deviation calculation for 2m temperature.
                                Default is '1980-01-01'.
            ts_std_end (str): End date for standard deviation calculation for 2m temperature.
                                Default is '2010-12-31'.
            toa_std_start (str): Start date for standard deviation calculation for net radiation at TOA.
                                Default is '2001-01-01'.
            toa_std_end (str): End date for standard deviation calculation for net radiation at TOA.
                                Default is '2020-12-31'.
            ref (bool): If True, reference data is plotted.
                        Default is True. Reference data are ERA5 for 2m temperature
                        and CERES for net radiation at TOA.
            save_netcdf (bool): If True, save the NetCDF file. Default is True.
            outdir (str): Output directory. Default is './'.
            loglevel (str): Logging level. Default is WARNING.
            rebuild (bool, optional): If True, overwrite the existing files. If False, do not overwrite. Default is True.
            filename_keys (list, optional): List of keys to keep in the filename.
                                            Default is None, which includes all keys (see OutputNamer class).
            save_pdf (bool): If True, save the figure as a PDF. Default is True.
            save_png (bool): If True, save the figure as a PNG. Default is True.
            dpi (int, optional): Dots per inch (DPI) for saving figures. Default is 300.

        """
        self.loglevel = loglevel
        self.logger = log_configure(loglevel, 'Gregory plot')

        self.models = models
        self.exps = exps
        self.sources = sources
        self._catalogs(catalogs=catalogs)  # Define self.catalogs

        if self.models is None or self.exps is None or self.sources is None:
            raise NoDataError("No models, experiments or sources given. No plot will be drawn.")
        if isinstance(self.models, str):
            self.models = [self.models]
        if isinstance(self.exps, str):
            self.exps = [self.exps]
        if isinstance(self.sources, str):
            self.sources = [self.sources]
        if isinstance(self.catalogs, str):
            self.catalogs = [self.catalogs]

        self.monthly = monthly
        self.annual = annual
        self.regrid = regrid

        self.ref = ref
        self.ts_name = ts_name
        self.toa_name = toa_name
        self.retrieve_list = [self.ts_name] + self.toa_name
        self.ts_std_start = ts_std_start
        self.ts_std_end = ts_std_end
        self.toa_std_start = toa_std_start
        self.toa_std_end = toa_std_end
        self.logger.debug(f"Retrieving {self.retrieve_list}, for standard deviation calculation: "
                          f"2m temperature from {time_to_string(self.ts_std_start)} to {time_to_string(self.ts_std_end)}, "
                          f"net radiation at TOA from {time_to_string(self.toa_std_start)} to {time_to_string(self.toa_std_end)}") # noqa

        self.save_netcdf = save_netcdf
        if self.save_netcdf is False:
            self.logger.info("No NetCDF file will be saved.")
        self.outdir = outdir

        self.diagnostic_product = 'gregory_plot'
        self.diagnostic = 'timeseries'
        self.rebuild = rebuild
        self.filename_keys = filename_keys
        self.save_pdf = save_pdf
        self.save_png = save_png
        self.dpi = dpi

    def run(self):
        """
        Retrieve ref, retrieve data and plot
        """
        self.retrieve_data()
        self.retrieve_ref()
        self.plot()
        if self.save_netcdf:
            self._save_netcdf()
        self.cleanup()

    def retrieve_data(self):
        """Retrieve data from the given models, experiments and sources."""
        self.logger.debug("Retrieving data")
        self.data_ts_mon = len(self.models) * [None]
        self.data_toa_mon = len(self.models) * [None]
        self.data_ts_annual = len(self.models) * [None]
        self.data_toa_annual = len(self.models) * [None]

        for i, model in enumerate(self.models):
            self.logger.info(f'Retrieving data for {self.catalogs[i]} {model} {self.exps[i]} {self.sources[i]}')
            try:
                reader = Reader(catalog=self.catalogs[i], model=model,
                                exp=self.exps[i], source=self.sources[i],
                                regrid=self.regrid, loglevel=self.loglevel)
                data = reader.retrieve(var=self.retrieve_list)
                # We're assuming the ts is in K and we want it in C
                ts = reader.fldmean(data[self.ts_name]) - 273.15
                toa = reader.fldmean(data[self.toa_name[0]] + data[self.toa_name[1]])
            except Exception as e:
                self.logger.error(f"Error: {e}")
                raise NoDataError(f"Could not retrieve data for {model} {self.exps[i]}. No plot will be drawn.") from e

            if self.monthly:
                if 'monthly' in self.sources[i] or 'mon' in self.sources[i]:
                    self.logger.debug(f"No monthly resample needed for {model} {self.exps[i]} {self.sources[i]}")
                    data_ts_mon = ts
                    data_toa_mon = toa
                else:
                    self.logger.debug("Resampling data to monthly")
                    data_ts_mon = reader.timmean(data=ts, freq='MS',
                                                 exclude_incomplete=True)
                    data_toa_mon = reader.timmean(data=toa, freq='MS',
                                                  exclude_incomplete=True)
                if len(data_ts_mon) < 2 or len(data_toa_mon) < 2:
                    self.logger.warning(f"Not enough data for {model} {self.exps[i]}. No monthly data will be plotted.")
                    self.data_ts_mon[i] = None
                    self.data_toa_mon[i] = None
                else:
                    self.data_ts_mon[i] = data_ts_mon
                    self.data_toa_mon[i] = data_toa_mon

            if self.annual:
                self.logger.debug("Resampling data to annual")
                data_ts_annual = reader.timmean(data=ts, freq='YS',
                                                exclude_incomplete=True)
                data_toa_annual = reader.timmean(data=toa, freq='YS',
                                                 exclude_incomplete=True)
                if len(data_ts_annual) < 2 or len(data_toa_annual) < 2:
                    self.logger.warning(f"Not enough data for {model} {self.exps[i]}. No annual data will be plotted.")
                    self.data_ts_annual[i] = None
                    self.data_toa_annual[i] = None
                else:
                    self.data_ts_annual[i] = data_ts_annual
                    self.data_toa_annual[i] = data_toa_annual

            # Clean up
            del data, ts, toa, reader
            if self.monthly:
                del data_ts_mon, data_toa_mon
            if self.annual:
                del data_ts_annual, data_toa_annual
            gc.collect()

        # Check at least one dataset has been retrieved
        if all([d is None for d in self.data_ts_mon]) and all([d is None for d in self.data_ts_annual]):
            raise NotEnoughDataError("Not enough data available. No plot will be drawn.")
        elif all([d is None for d in self.data_ts_mon]):
            self.monthly = False
            self.logger.warning("No monthly data available. Monthly plot will not be drawn.")
        elif all([d is None for d in self.data_ts_annual]):
            self.annual = False
            self.logger.warning("No annual data available. Annual plot will not be drawn.")

    def retrieve_ref(self):
        """Retrieve reference data."""
        if self.ref:
            self.logger.debug("Retrieving reference data")
            try:
                ref_ts_mean, ref_ts_std = get_reference_ts_gregory(ts_name=self.ts_name,
                                                                   startdate=self.ts_std_start,
                                                                   enddate=self.ts_std_end,
                                                                   loglevel=self.loglevel)
                ref_toa_mean, ref_toa_std = get_reference_toa_gregory(toa_name=self.toa_name,
                                                                      startdate=self.toa_std_start,
                                                                      enddate=self.toa_std_end,
                                                                      loglevel=self.loglevel)
            except NoObservationError as e:
                self.logger.debug(f"Error: {e}")
                self.logger.error("No reference data available. No reference plot will be drawn.")
                self.ref = False
            # We're assuming the ts is in K and we want it in C
            self.ref_ts_mean = ref_ts_mean - 273.15
            self.ref_ts_std = ref_ts_std
            self.ref_toa_mean = ref_toa_mean
            self.ref_toa_std = ref_toa_std

    def plot(self):
        """Plot the Gregory plot."""
        self.logger.debug("Plotting")
        ax1 = None
        ax2 = None

        if self.monthly and self.annual:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 6))
        elif self.monthly:
            fig, ax1 = plt.subplots(1, 1, figsize=(6, 6))
        elif self.annual:
            fig, ax2 = plt.subplots(1, 1, figsize=(6, 6))

        color_list = ["#1898e0", "#8bcd45", "#f89e13", "#d24493",
                      "#00b2ed", "#dbe622", "#fb4c27", "#8f57bf",
                      "#00bb62", "#f9c410", "#fb4865", "#645ccc"]

        if self.monthly:
            ax1.axhline(0, color="k")
            ax1.set_xlabel("2m temperature [C]")
            ax1.set_ylabel(r"Net radiation TOA [$\rm Wm^{-2}$]")
            ax1.grid(True)
            ax1.set_title("Monthly Mean")

            toa_min, toa_max = evaluate_colorbar_limits(self.data_toa_mon, sym=False)
            toa_min = min(toa_min, -12.)
            toa_max = max(toa_max, 12.)
            ax1.set_ylim(toa_min, toa_max)
            ts_min, ts_max = evaluate_colorbar_limits(self.data_ts_mon, sym=False)
            ts_min = min(ts_min, 10.)
            ts_max = max(ts_max, 16.5)
            ax1.set_xlim(ts_min, ts_max)
            self.logger.debug(f"Monthly x-axis limits: {ts_min} to {ts_max}")
            self.logger.debug(f"Monthly y-axis limits: {toa_min} to {toa_max}")

            for i, model in enumerate(self.models):
                if self.data_ts_mon[i] is not None and self.data_toa_mon[i] is not None:
                    ax1.plot(self.data_ts_mon[i], self.data_toa_mon[i], marker=".",
                             label=f"{model} {self.exps[i]}", color=color_list[i])

            for i, model in enumerate(self.models):  # Last so that legend is at the end
                if self.data_ts_mon[i] is not None and self.data_toa_mon[i] is not None:
                    if i == 0:
                        label_b = "First Time-step"
                        label_e = "Last Time-step"
                    else:
                        label_b = None
                        label_e = None
                    ax1.plot(self.data_ts_mon[i][0], self.data_toa_mon[i][0], marker=">",
                             color="tab:blue", label=label_b)  # Blue to be colorblind friendly
                    ax1.plot(self.data_ts_mon[i][-1], self.data_toa_mon[i][-1], marker="<",
                             color="tab:red", label=label_e)

            ax1.legend()

        if self.annual:
            ax2.axhline(0, color="k", lw=0.7)
            ax2.set_xlabel("2m temperature [C]")
            if ax1 is None:
                ax2.set_ylabel(r"Net radiation TOA [$\rm Wm^{-2}$]")
            ax2.grid(True)
            ax2.set_title("Annual Mean")

            toa_min, toa_max = evaluate_colorbar_limits(self.data_toa_annual, sym=False)
            toa_min = min(toa_min, -2.)
            toa_max = max(toa_max, 2.)
            ts_min, ts_max = evaluate_colorbar_limits(self.data_ts_annual, sym=False)
            ts_min = min(ts_min, 13.5)
            ts_max = max(ts_max, 15.)
            ax2.set_xlim(ts_min, ts_max)
            ax2.set_ylim(toa_min, toa_max)
            self.logger.debug(f"Annual x-axis limits: {ts_min} to {ts_max}")
            self.logger.debug(f"Annual y-axis limits: {toa_min} to {toa_max}")

            for i, model in enumerate(self.models):
                if self.data_ts_annual[i] is not None and self.data_toa_annual[i] is not None:
                    ax2.plot(self.data_ts_annual[i], self.data_toa_annual[i], marker=".",
                             label=f"{model} {self.exps[i]}", color=color_list[i])
                    if i == 0 and ax1 is None:
                        label_b = "First Time-step"
                        label_e = "Last Time-step"
                    else:
                        label_b = None
                        label_e = None
                    ax2.plot(self.data_ts_annual[i][0], self.data_toa_annual[i][0], marker=">",
                             color="tab:blue", label=label_b)
                    ax2.plot(self.data_ts_annual[i][-1], self.data_toa_annual[i][-1], marker="<",
                             color="tab:red", label=label_e)
                    ax2.text(self.data_ts_annual[i][0], self.data_toa_annual[i][0],
                             str(self.data_ts_annual[i].time.dt.year[0].values),
                             fontsize=8, ha='right')
                    ax2.text(self.data_ts_annual[i][-1], self.data_toa_annual[i][-1],
                             str(self.data_ts_annual[i].time.dt.year[-1].values),
                             fontsize=8, ha='right')

            if self.ref:
                ax2.axhspan(self.ref_toa_mean - self.ref_toa_std, self.ref_toa_mean + self.ref_toa_std,
                            color="lightgreen", alpha=0.3, label=r"$\sigma$ band")
                ax2.axvspan(self.ref_ts_mean - self.ref_ts_std, self.ref_ts_mean + self.ref_ts_std,
                            color="lightgreen", alpha=0.3)

            ax2.legend()

        if self.save_pdf or self.save_png:
            self.save_image(fig)

    def save_image(self, fig):
        """Save the figure to an image file (PDF/PNG).
        
        Args:
            fig (matplotlib.figure.Figure): Figure to save.
        """
    
        # Get OutputSaver instance for the first model
        output_saver = self._get_output_saver(catalog=self.catalogs[0], model=self.models[0], exp=self.exps[0])
        common_save_args = {'diagnostic_product': self.diagnostic_product, 'dpi': self.dpi}

        # Use the helper function to generate the description
        description = self._construct_description(plot_type="Gregory plot", ref_label="ERA5 and CERES")
        self.logger.debug(f"Description: {description}")

        metadata = {"Description": description}

        # Save the figure as PDF/PNG as per user preferences
        if self.save_pdf:
            output_saver.save_pdf(fig, metadata=metadata, **common_save_args)
        if self.save_png:
            output_saver.save_png(fig, metadata=metadata, **common_save_args)

    def _save_netcdf(self):
        """Save the data to a netCDF file."""

        # Loop through the models and save their corresponding data
        for i, model in enumerate(self.models):
            output_saver = self._get_output_saver(catalog=self.catalogs[i], model=model, exp=self.exps[i])
            common_save_args = {'diagnostic_product': self.diagnostic_product}

            if self.monthly:
                self._save_frequency_data(output_saver, frequency='monthly', data_ts=self.data_ts_mon[i], data_toa=self.data_toa_mon[i], **common_save_args)
            if self.annual:
                self._save_frequency_data(output_saver, frequency='annual', data_ts=self.data_ts_annual[i], data_toa=self.data_toa_annual[i], **common_save_args)

        # Save the reference data if required
        if self.ref:
            output_saver_ref = self._get_output_saver(model='ERA5', exp='CERES')
            ref_dataset = xr.Dataset({'ts_mean': self.ref_ts_mean, 'ts_std': self.ref_ts_std,
                                      'toa_mean': self.ref_toa_mean, 'toa_std': self.ref_toa_std})
            output_saver_ref.save_netcdf(ref_dataset, diagnostic_product=self.diagnostic_product)

    def _construct_description(self, plot_type: str = "Gregory plot", ref_label: str = None) -> str:
        """
        Construct a descriptive string for the output files.

        Args:
            plot_type (str): Type of plot (e.g., "Gregory plot").
            ref_label (str, optional): Label for the reference data.

        Returns:
            str: A description of the figure or NetCDF dataset.
        """
        description = f"{plot_type}"

        # Add model and experiment details
        models_info = ' '.join(f"{model} {exp}" for model, exp in zip(self.models, self.exps))
        description += f" {models_info}"

        # Add reference data information if available
        # TODO: Make the reference data source more generic
        if self.ref:
            description += (
                f" with reference data ERA5 for 2m temperature from {self.ts_std_start} to {self.ts_std_end}"
                f" and CERES for net radiation at TOA from {self.toa_std_start} to {self.toa_std_end}."
            )
            if ref_label:
                description += f" with {ref_label} as reference."

        return description

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
        return OutputSaver(diagnostic=self.diagnostic, catalog=catalog, model=model, exp=exp,
                           loglevel=self.loglevel, default_path=self.outdir, rebuild=self.rebuild,
                           filename_keys=self.filename_keys)

    def _save_frequency_data(self, output_saver, frequency, data_ts, data_toa, **common_save_args):
        """Helper function to save data for a specific frequency.
        
        Args:
            output_saver (OutputSaver): OutputSaver instance.
            frequency (str): Frequency of the data (e.g., 'monthly', 'annual').
            data_ts (xarray.DataArray): 2m temperature data.
            data_toa (xarray.DataArray): Net radiation at TOA data.
            common_save_args (dict): Common arguments for saving the data.
        """
        output_saver.save_netcdf(data_ts, frequency=frequency, mode='w', **common_save_args)
        output_saver.save_netcdf(data_toa, frequency=frequency, mode='a', **common_save_args)

    def _catalogs(self, catalogs=None):
        """
        Fill in the missing catalogs. If catalogs is None, creates a list of None
        with the same length as models, exps and sources.

        Args:
            catalogs (list): List of catalogs to search for the data.
        """
        if catalogs is None:
            self.catalogs = [None] * len(self.models)
        else:
            self.catalogs = catalogs

    def cleanup(self):
        """Clean up"""
        self.logger.debug("Cleaning up")
        gc.collect()
        self.logger.debug("Cleaned up")
