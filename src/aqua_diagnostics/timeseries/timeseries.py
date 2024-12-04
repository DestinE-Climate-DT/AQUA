import gc  # Garbage collector

import xarray as xr
from aqua import Reader
from aqua.logger import log_configure
from aqua.exceptions import NoObservationError, NoDataError
from aqua.util import eval_formula, OutputSaver, time_to_string
from aqua.graphics import plot_timeseries

from .reference_data import get_reference_timeseries
from .util import loop_seasonalcycle

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
                 catalogs=None,
                 models=None, exps=None, sources=None,
                 monthly=True, annual=True,
                 regrid=None, plot_ref=True,
                 plot_ref_kw={'catalog': 'obs',
                              'model': 'ERA5',
                              'exp': 'era5',
                              'source': 'monthly'},
                 startdate=None, enddate=None,
                 monthly_std=True, annual_std=True,
                 std_startdate=None, std_enddate=None,
                 plot_kw={'ylim': {}}, longname=None,
                 units=None, extend=True,
                 lon_limits=None, lat_limits=None,
                 save_netcdf=True,
                 outdir='./',
                 loglevel='WARNING',
                 rebuild=None, filename_keys=None,
                 save_pdf=True, save_png=True, dpi=300):
        """
        Args:
            var (str): Variable name.
            formula (bool): (Optional) If True, try to derive the variable from other variables.
                            Default is False.
            catalogs (list or str): Catalog IDs.
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
            longname (str): Long name of the variable. Default is None and logname attribute is used.
            units (str): Units of the variable. Default is None and units attribute is used.
            extend (bool): Extend the reference range. Default is True.
            lon_limits (list): Longitude limits of the area to evaluate. Default is None.
            lat_limits (list): Latitude limits of the area to evaluate. Default is None.
            save_netcdf (bool): Save the NetCDF. Default is True.
            outdir (str): Output directory. Default is "./".
            loglevel (str): Log level. Default is "WARNING".
            rebuild (bool, optional): If True, overwrite the existing files. If False, do not overwrite. Default is True.
            filename_keys (list, optional): List of keys to keep in the filename.
                                            Default is None, which includes all keys (see OutputNamer class).
            save_pdf (bool): If True, save the figure as a PDF. Default is True.
            save_png (bool): If True, save the figure as a PNG. Default is True.
            dpi (int, optional): Dots per inch (DPI) for saving figures. Default is 300.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name='Timeseries')

        self.var = var
        self.formula = formula

        self.models = models
        self.exps = exps
        self.sources = sources
        self._catalogs(catalogs=catalogs)  # It defines self.catalogs

        if self.models is None or self.exps is None or self.sources is None:
            raise NoDataError("No models, exps or sources provided")

        if isinstance(self.catalogs, str):
            self.catalogs = [self.catalogs]
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
        self.monthly_std = monthly_std if monthly else False
        self.annual_std = annual_std if annual else False
        self.std_startdate = std_startdate
        self.std_enddate = std_enddate
        self.extend = extend
        self.extending_ref_range = False

        self.startdate = startdate
        self.enddate = enddate

        self.plot_kw = plot_kw
        self.longname = longname
        self.units = units
        self.lon_limits = lon_limits
        self.lat_limits = lat_limits

        self.save_netcdf = save_netcdf
        if self.save_netcdf is False:
            self.logger.info("NetCDF will not be saved")
        self.outdir = outdir

        self.diagnostic_product = 'timeseries'
        self.diagnostic = 'timeseries'
        self.rebuild = rebuild
        self.filename_keys = filename_keys
        self.save_pdf = save_pdf
        self.save_png = save_png
        if not self.save_png and not self.save_pdf:
            self.logger.info("Figure will not be saved")
        self.dpi = dpi

    def run(self):
        """Retrieve ref, retrieve data and plot"""
        self.retrieve_data()
        self.retrieve_ref(extend=self.extend)
        self.plot()
        if self.save_netcdf:
            self._save_netcdf()
        self.cleanup()

    def retrieve_ref(self, extend=True):
        """Retrieve reference data
        If the reference data don't cover the same range as the model data,
        a seasonal cycle or a band of the reference data is added to the plot.

        Args:
            extend (bool): Extend the reference range. Default is True.
        """
        if self.plot_ref:
            self.logger.debug('Retrieving reference data')
            try:
                self.ref_mon, self.ref_mon_std, self.ref_ann, self.ref_ann_std =\
                    get_reference_timeseries(var=self.var,
                                             formula=self.formula,
                                             **self.plot_ref_kw,  # catalog,model,exp,source
                                             startdate=self.startdate,
                                             enddate=self.enddate,
                                             std_startdate=self.std_startdate,
                                             std_enddate=self.std_enddate,
                                             regrid=self.regrid,
                                             monthly=self.monthly,
                                             annual=self.annual,
                                             monthly_std=self.monthly_std,
                                             annual_std=self.annual_std,
                                             loglevel=self.loglevel,
                                             lon_limits=self.lon_limits,
                                             lat_limits=self.lat_limits)
                if extend:  # We introduce the possibility to avoid this for seasonal cycle
                    self.check_ref_range()
                if self.longname is not None:
                    if self.ref_mon is not None:
                        self.ref_mon.attrs['long_name'] = self.longname
                    if self.ref_ann is not None:
                        self.ref_ann.attrs['long_name'] = self.longname
                if self.units is not None:
                    if self.ref_mon is not None:
                        self.ref_mon.attrs['units'] = self.units
                    if self.ref_ann is not None:
                        self.ref_ann.attrs['units'] = self.units
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
            self.logger.info(f'Retrieving data for {self.catalogs[i]} {model} {self.exps[i]} {self.sources[i]}')
            try:
                reader = Reader(catalog=self.catalogs[i], model=model,
                                exp=self.exps[i], source=self.sources[i],
                                startdate=self.startdate, enddate=self.enddate,
                                regrid=self.regrid, loglevel=self.loglevel)
                if self.formula:
                    data = reader.retrieve()
                    self.logger.debug(f"Evaluating formula for {self.var}")
                    data = eval_formula(self.var, data)
                    if data is None:
                        self.logger.error(f"Formula evaluation failed for {self.catalogs[i]} {model} {self.exps[i]} {self.sources[i]}") # noqa
                else:
                    data = reader.retrieve(var=self.var)
                    data = data[self.var]
            except Exception as e:
                self.logger.debug(f"Error while retrieving: {e}")
                self.logger.warning(f"No data found for {self.catalogs[i]} {model} {self.exps[i]} {self.sources[i]}")

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
                    self.logger.debug(f"No monthly resample needed for {self.catalogs[i]} {model} {self.exps[i]} {self.sources[i]}") # noqa
                    data_mon = data
                else:
                    data_mon = reader.timmean(data, freq='MS', exclude_incomplete=True)
                data_mon = reader.fldmean(data_mon, lon_limits=self.lon_limits, lat_limits=self.lat_limits)
                self.logger.info("Monthly data retrieved")
                if data_mon is not None:
                    if self.longname is not None:
                        data_mon.attrs['long_name'] = self.longname
                        self.logger.debug(f"Long name updated to: {self.longname}")
                    if self.units is not None:
                        data_mon.attrs['units'] = self.units
                        self.logger.debug(f"Units updated to: {self.units}")
                    self.data_mon.append(data_mon)
                else:
                    self.logger.warning(f"No monthly data found for {self.catalogs[i]} {model} {self.exps[i]} {self.sources[i]}") # noqa

            if self.annual:
                data_ann = reader.timmean(data, freq='YS',
                                          exclude_incomplete=True,
                                          center_time=True)
                data_ann = reader.fldmean(data_ann, lon_limits=self.lon_limits, lat_limits=self.lat_limits)
                self.logger.info("Annual data retrieved")
                if data_ann is not None:
                    if self.longname is not None:
                        data_ann.attrs['long_name'] = self.longname
                        self.logger.debug(f"Long name updated to: {self.longname}")
                    if self.units is not None:
                        data_ann.attrs['units'] = self.units
                        self.logger.debug(f"Units updated to: {self.units}")
                    self.data_annual.append(data_ann)
                else:
                    self.logger.warning(f"No annual data found for {self.catalogs[i]} {model} {self.exps[i]} {self.sources[i]}") # noqa

            # Clean up
            del reader
            del data
            gc.collect()

        if self.startdate is None:
            startdate = time_to_string(startdate)
            self.startdate = startdate
            self.logger.debug(f"Start date: {self.startdate}")
        if self.enddate is None:
            enddate = time_to_string(enddate)
            self.enddate = enddate
            self.logger.debug(f"End date: {self.enddate}")

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

        try:
            if self.monthly:
                title = self.data_mon[0].attrs['long_name'] + ' (' + self.data_mon[0].attrs['units'] + ') timeseries'
            elif self.annual:
                title = self.data_annual[0].attrs['long_name'] + ' (' + self.data_annual[0].attrs['units'] + ') timeseries'
            else:
                title = self.var + ' timeseries'
        except KeyError:
            title = f'{self.var} timeseries'

        if self.lon_limits is not None or self.lat_limits is not None:
            title += ' for region'
            if self.lon_limits is not None:
                title += f' lon: {self.lon_limits}'
            if self.lat_limits is not None:
                title += f' lat: {self.lat_limits}'

        fig, _ = plot_timeseries(monthly_data=self.data_mon,
                                 annual_data=self.data_annual,
                                 ref_monthly_data=self.ref_mon,
                                 ref_annual_data=self.ref_ann,
                                 std_monthly_data=self.ref_mon_std,
                                 std_annual_data=self.ref_ann_std,
                                 ref_label=ref_label,
                                 data_labels=data_labels,
                                 title=title)

        if self.save_pdf or self.save_png:
            self.save_image(fig, ref_label)

    def save_image(self, fig, ref_label):
        """
        Save the figure to image files (PDF/PNG).
        Args:
            fig (matplotlib.figure.Figure): Figure to save.
            ref_label (str): Label for the reference data.
        """
        output_saver = self._get_output_saver(catalog=self.catalogs[0], model=self.models[0], exp=self.exps[0])

        # Get common save arguments
        common_save_args = self._get_common_save_args()

        description = self._construct_description(ref_label)
        self.logger.debug(f"Description: {description}")

        metadata = {"Description": description}

        if self.save_pdf:
            output_saver.save_pdf(fig, metadata=metadata, **common_save_args)
        if self.save_png:
            output_saver.save_png(fig, metadata=metadata, **common_save_args)

    def _save_netcdf(self):
        """
        Save the data to a netcdf file.
        Every model-exp-source combination is saved in a separate file.
        Reference data is saved in a separate file.
        Std data is saved in a separate file.
        """
        for i, model in enumerate(self.models):
            output_saver = self._get_output_saver(catalog=self.catalogs[i], model=model, exp=self.exps[i])
            common_save_args = self._get_common_save_args()
            common_save_args.pop('dpi', None)

            if self.monthly:
                output_saver.save_netcdf(self.data_mon[i], frequency='monthly', **common_save_args)
            if self.annual:
                output_saver.save_netcdf(self.data_annual[i], frequency='annual', **common_save_args)

        if self.plot_ref:
            output_saver_ref = self._get_output_saver(model=self.plot_ref_kw['model'], exp=self.plot_ref_kw['exp'])
            common_save_args = self._get_common_save_args()
            common_save_args.pop('dpi', None)

            # Save reference data
            if self.monthly:
                output_saver_ref.save_netcdf(self.ref_mon, frequency='monthly', **common_save_args)
            if self.annual:
                output_saver_ref.save_netcdf(self.ref_ann, frequency='annual', **common_save_args)

            # Save standard deviation data
            common_save_args.update({'stat': 'std'})
            if self.monthly_std:
                output_saver_ref.save_netcdf(self.ref_mon_std, frequency='monthly', **common_save_args)
            if self.annual_std:
                output_saver_ref.save_netcdf(self.ref_ann_std, frequency='annual', **common_save_args)

    # Helper functions
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

    def _get_common_save_args(self):
        """
        Create and return a dictionary of common arguments for saving files.
        Returns:
            dict: Common arguments for saving files.
        """
        common_save_args = {'diagnostic_product': self.diagnostic_product, 'var': self.var, 'dpi': self.dpi}
        if self.lon_limits is not None:
            lon_limits = f'_lon{self.lon_limits[0]}_{self.lon_limits[1]}'
            common_save_args.update({'lon_limits': lon_limits})
        if self.lat_limits is not None:
            lat_limits = f'_lat{self.lat_limits[0]}_{self.lat_limits[1]}'
            common_save_args.update({'lat_limits': lat_limits})
        return common_save_args

    def _construct_description(self, ref_label):
        """
        Construct the description for the metadata.
        Args:
            ref_label (str): Label for the reference data.
        Returns:
            str: A description string.
        """
        description = "Time series of the global mean of"
        if self.monthly:
            description += f" {self.data_annual[0].attrs['long_name']}"
        elif self.annual:
            description += f" {self.data_annual[0].attrs['long_name']}"
        else:
            description += f" {self.var}"
        description += f" from {time_to_string(self.startdate)} to {time_to_string(self.enddate)}"
        for i, model in enumerate(self.models):
            description += f" for {model} {self.exps[i]}"
        if self.plot_ref:
            description += f" with {ref_label} as reference,"
            if self.std_startdate is not None and self.std_enddate is not None:
                description += f" std evaluated from {time_to_string(self.std_startdate)} to {time_to_string(self.std_enddate)}" # noqa
            else:
                description += " std evaluated from the full time range."
        if self.extending_ref_range:
            description += " The reference range has been extended with a seasonal cycle or a band to match the model data."
        if self.lon_limits is not None or self.lat_limits is not None:
            description += " The data have been averaged over a region defined by"
            if self.lon_limits is not None:
                description += f" longitude limits {self.lon_limits}"
            if self.lat_limits is not None:
                description += f" latitude limits {self.lat_limits}"
            description += "."
        return description

    def check_ref_range(self):
        """
        If the reference data don't cover the same range as the model data,
        a seasonal cycle or a band of the reference data is added to the plot.
        """
        self.logger.debug("Checking reference range")

        if self.monthly:
            exp_startdate, exp_enddate = self._extend_ref_range(freq='monthly')
            self.logger.info(f"Monthly reference std time range for expansion evaluation: {exp_startdate} to {exp_enddate}")

            startdate, enddate = self._extend_ref_range(freq='monthly', range_eval=True)
            self.logger.info(f"Monthly reference data time available {startdate} to {enddate}")

            if startdate > self.startdate or enddate < self.enddate:
                self.logger.info("Extending reference range with a seasonal cycle")
                self.extending_ref_range = True

                # TODO: startdate has to be rounded to the first of the month
                # if startdate > self.startdate:
                #     self.logger.debug("Adding a seasonal cycle to the start of the reference data")
                #     ref_mon_loop = loop_seasonalcycle(data=self.ref_mon,
                #                                       startdate=self.startdate,
                #                                       enddate=startdate,
                #                                       freq='MS')
                #     self.ref_mon = xr.concat([ref_mon_loop, self.ref_mon], dim='time')

                if enddate < self.enddate:
                    self.logger.debug("Adding a seasonal cycle to the end of the reference data")
                    ref_mon_loop = loop_seasonalcycle(data=self.ref_mon,
                                                      startdate=enddate,
                                                      enddate=self.enddate,
                                                      freq='MS', loglevel=self.loglevel)
                    self.ref_mon = xr.concat([self.ref_mon, ref_mon_loop], dim='time')
                    self.ref_mon = self.ref_mon.sortby('time')

            self.ref_mon = self.ref_mon.sel(time=slice(self.startdate, self.enddate))

        if self.annual:
            exp_startdate, exp_enddate = self._extend_ref_range(freq='annual')
            self.logger.info(f"Annual reference std time range for expansion evaluation: {exp_startdate} to {exp_enddate}")

            startdate, enddate = self._extend_ref_range(freq='annual', range_eval=True)
            self.logger.info(f"Annual reference data time available {startdate} to {enddate}")

            if startdate > self.startdate or enddate < self.enddate:
                self.logger.info("Extending reference range with a band of the reference data")
                self.extending_ref_range = True

                # TODO: startdate has to be rounded to the center of the year (month=7)
                # if startdate > self.startdate:
                #     self.logger.debug("Adding a band to the start of the reference data")
                #     ref_ann_loop = loop_seasonalcycle(data=self.ref_ann,
                #                                       startdate=self.startdate,
                #                                       enddate=startdate,
                #                                       freq='YS')
                #     self.ref_ann = xr.concat([ref_ann_loop, self.ref_ann], dim='time')

                if enddate < self.enddate:
                    self.logger.debug("Adding a band to the end of the reference data")
                    ref_ann_loop = loop_seasonalcycle(data=self.ref_ann,
                                                      startdate=enddate,
                                                      enddate=self.enddate,
                                                      freq='YS', loglevel=self.loglevel)
                    self.ref_ann = xr.concat([self.ref_ann, ref_ann_loop], dim='time')
                    self.ref_ann = self.ref_ann.sortby('time')

            self.ref_ann = self.ref_ann.sel(time=slice(self.startdate, self.enddate))

    def _extend_ref_range(self, freq='monthly', range_eval=False):
        """Evaluate range for statistics to extend the reference range

        Args:
            freq (str): Frequency of the data. Default is 'monthly'.
            range_eval (bool): Evaluate the range also if std is provided. Default is False.

        Returns:
            startdate and enddate (str): Start and end date of the reference range.
        """
        startdate = self.std_startdate
        enddate = self.std_enddate

        if startdate is None or enddate is None or range_eval:
            self.logger.debug("No std reference range provided, using data retrieved range")
            if freq == 'monthly':
                startdate = self.ref_mon.time[0].values
                startdate = time_to_string(startdate)
                enddate = self.ref_mon.time[-1].values
                enddate = time_to_string(enddate)
            elif freq == 'annual':
                if self.annual:
                    startdate = self.ref_ann.time[0].values
                    startdate = time_to_string(startdate)
                    enddate = self.ref_ann.time[-1].values
                    enddate = time_to_string(enddate)
            else:
                raise ValueError(f"Unknown frequency: {freq}")

        return startdate, enddate

    def _catalogs(self, catalogs=None):
        """
        Fill in the missing catalogs. If catalogs is None, creates a list of None
        with the same length as models, exps and sources.
        """
        if catalogs is None:
            self.catalogs = [None] * len(self.models)
        else:
            self.catalogs = catalogs

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
