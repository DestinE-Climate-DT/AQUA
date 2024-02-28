"""Gregory plot module."""
import os
import gc

import matplotlib.pyplot as plt
import xarray as xr
from aqua import Reader
from aqua.logger import log_configure
from aqua.util import create_folder, add_pdf_metadata, time_to_string
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from .reference_data import get_reference_ts_gregory, get_reference_toa_gregory

xr.set_options(keep_attrs=True)  # Keep attributes after operations


class GregoryPlot():
    """
    Gregory plot class.
    Retrieve data, obs reference and plot the Gregory plot.
    Can work with a list of models, experiments and sources.
    """
    def __init__(self, models=None, exps=None, sources=None,
                 monthly=True, annual=True,
                 regrid=None,
                 ts_name='2t', toa_name=['mtnlwrf', 'mtnswrf'],
                 ts_std_start='1980-01-01', ts_std_end='2010-12-31',
                 toa_std_start='2001-01-01', toa_std_end='2020-12-31',
                 ref=True, save=True,
                 outdir='./', outfile=None,
                 loglevel='WARNING'):
        """
        Args:
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
            save (bool): If True, save the figure. Default is True.
            outdir (str): Output directory. Default is './'.
            outfile (str): Output file name. Default is None.
            loglevel (str): Logging level. Default is WARNING.
        """
        self.loglevel = loglevel
        self.logger = log_configure(loglevel, 'Gregory plot')

        self.models = models
        self.exps = exps
        self.sources = sources

        if self.models is None or self.exps is None:
            raise NoDataError("No models or experiments given. No plot will be drawn.")
        if isinstance(self.models, str):
            self.models = [self.models]
        if isinstance(self.exps, str):
            self.exps = [self.exps]
        if isinstance(self.sources, str):
            self.sources = [self.sources]

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
                          f"net radiation at TOA from {time_to_string(self.toa_std_start)} to {time_to_string(self.toa_std_end)}")

        self.save = save
        if self.save is False:
            self.logger.info("No output file will be saved.")
        self.outdir = outdir
        self.outfile = outfile

    def run(self):
        """
        Retrieve ref, retrieve data and plot
        """
        self.retrieve_data()
        self.retrieve_ref()
        self.plot()
        if self.save:
            self.save_netcdf()
        self.cleanup()

    def retrieve_data(self):
        """Retrieve data from the given models, experiments and sources."""
        self.logger.debug("Retrieving data")
        self.data_ts_mon = len(self.models) * [None]
        self.data_toa_mon = len(self.models) * [None]
        self.data_ts_annual = len(self.models) * [None]
        self.data_toa_annual = len(self.models) * [None]

        for i, model in enumerate(self.models):
            self.logger.info(f'Retrieving data for {model} {self.exps[i]} {self.sources[i]}')
            try:
                reader = Reader(model=model, exp=self.exps[i], source=self.sources[i],
                                regrid=self.regrid, loglevel=self.loglevel)
                data = reader.retrieve(var=self.retrieve_list)
                ts = reader.fldmean(data[self.ts_name]) - 273.15
                toa = reader.fldmean(data[self.toa_name[0]] + data[self.toa_name[1]])
            except Exception as e:
                self.logger.error(f"Error: {e}")
                raise NoDataError(f"Could not retrieve data for {model}-{self.exps[i]}. No plot will be drawn.") from e

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
            ax1.set_ylim(-12., 12.)
            ax1.set_xlabel("2m temperature [C]")
            ax1.set_ylabel(r"Net radiation TOA [$\rm Wm^{-2}$]")
            ax1.grid(True)
            ax1.set_title("Monthly Mean")

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
                             color="tab:green", label=label_b)
                    ax1.plot(self.data_ts_mon[i][-1], self.data_toa_mon[i][-1], marker="<",
                             color="tab:red", label=label_e)

            ax1.legend()

        if self.annual:
            ax2.set_ylim(-2, 2)
            ax2.axhline(0, color="k", lw=0.7)
            ax2.set_xlabel("2m temperature [C]")
            if ax1 is None:
                ax2.set_ylabel(r"Net radiation TOA [$\rm Wm^{-2}$]")
            ax2.grid(True)
            ax2.set_title("Annual Mean")

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
                             color="tab:green", label=label_b)
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

        if self.save:
            self.save_pdf(fig)

    def save_pdf(self, fig):
        """Save the figure to a pdf file."""
        self.logger.info("Saving figure to pdf")
        # Save to outdir/pdf/filename
        outfig = os.path.join(self.outdir, 'pdf')
        self.logger.debug(f"Saving figure to {outfig}")
        create_folder(outfig, self.loglevel)
        if self.outfile is None:
            self.outfile = 'global_time_series_gregory_plot'
            for i, model in enumerate(self.models):
                self.outfile += f"_{model}_{self.exps[i]}"
            if self.ref:
                self.outfile += "_ref_ERA5_CERES"
            self.outfile += '.pdf'
        self.logger.debug(f"Output file: {self.outfile}")
        fig.savefig(os.path.join(outfig, self.outfile))

        description = "Gregory plot"
        for i, model in enumerate(self.models):
            description += f" {model} {self.exps[i]}"
        if self.ref:
            description += f" with reference data ERA5 for 2m temperature from {self.ts_std_start} to {self.ts_std_end}"
            description += f"and CERES for net radiation at TOA from {self.toa_std_start} to {self.toa_std_end}"
        self.logger.debug(f"Description: {description}")
        add_pdf_metadata(filename=os.path.join(outfig, self.outfile),
                         metadata_value=description)

    def save_netcdf(self):
        pass

    def cleanup(self):
        """Clean up"""
        self.logger.debug("Cleaning up")
        gc.collect()
        self.logger.debug("Cleaned up")
