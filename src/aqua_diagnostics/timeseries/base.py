import os
import xarray as xr
from aqua.fixer import EvaluateFormula
from aqua.logger import log_configure
from aqua.util import frequency_string_to_pandas, pandas_freq_to_string
from aqua.util import time_to_string, strlist_to_phrase
from aqua.diagnostics.core import Diagnostic, start_end_dates, OutputSaver

xr.set_options(keep_attrs=True)


class BaseMixin(Diagnostic):
    """The BaseMixin class is used for the Timeseries and the SeasonalCycles classes."""
    def __init__(self, diagnostic_name: str = 'timeseries',
                 catalog: str = None, model: str = None,
                 exp: str = None, source: str = None,
                 regrid: str = None,
                 startdate: str = None, enddate: str = None,
                 std_startdate: str = None, std_enddate: str = None,
                 region: str = None, lon_limits: list = None, lat_limits: list = None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the Base class.

        Args:
            diagnostic_name (str): The name of the diagnostic. Default is 'timeseries'.
                                   This will be used to configure the logger and the output files.
            catalog (str): The catalog to be used. If None, the catalog will be determined by the Reader.
            model (str): The model to be used.
            exp (str): The experiment to be used.
            source (str): The source to be used.
            regrid (str): The target grid to be used for regridding. If None, no regridding will be done.
            startdate (str): The start date of the data to be retrieved.
                             If None, all available data will be retrieved.
            enddate (str): The end date of the data to be retrieved.
                           If None, all available data will be retrieved.
            std_startdate (str): The start date of the standard period. Ignored if std is False.
            std_enddate (str): The end date of the standard period. Ignored if std is False.
            region (str): The region to select. This will define the lon and lat limits.
            lon_limits (list): The longitude limits to be used. Overriden by region.
            lat_limits (list): The latitude limits to be used. Overriden by region.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """
        super().__init__(catalog=catalog, model=model, exp=exp, source=source, regrid=regrid,
                         loglevel=loglevel)
        
        # Log name is the diagnostic name with the first letter capitalized
        self.logger = log_configure(log_level=loglevel, log_name=diagnostic_name.capitalize())
        self.diagnostic_name = diagnostic_name

        # We want to make sure we retrieve the required amount of data with a single Reader instance
        self.startdate, self.enddate = start_end_dates(startdate=startdate, enddate=enddate,
                                                       start_std=std_startdate, end_std=std_enddate)
        # They need to be stored to evaluate the std on the correct period
        self.std_startdate = self.startdate if std_startdate is None else std_startdate
        self.std_enddate = self.enddate if std_enddate is None else std_enddate
        # Finally we need to set the start and end dates of the data
        self.plt_startdate = startdate
        self.plt_enddate = enddate
        self.logger.debug(f"Retrieve start date: {self.startdate}, End date: {self.enddate}")
        self.logger.debug(f"Plot start date: {self.plt_startdate}, End date: {self.plt_enddate}")
        self.logger.debug(f"Std start date: {self.std_startdate}, Std end date: {self.std_enddate}")

        # Set the region based on the region name or the lon and lat limits
        self.region, self.lon_limits, self.lat_limits = self._set_region(region=region, diagnostic='timeseries',
                                                                         lon_limits=lon_limits, lat_limits=lat_limits)
        self.logger.debug(f"Region: {self.region}, Lon limits: {self.lon_limits}, Lat limits: {self.lat_limits}")

        # Initialize the possible results
        self.hourly = None
        self.daily = None
        self.monthly = None
        self.annual = None
        self.std_hourly = None
        self.std_daily = None
        self.std_monthly = None
        self.std_annual = None

    def retrieve(self, var: str, formula: bool = False, long_name: str = None,
                 units: str = None, short_name: str = None, reader_kwargs: dict = {}):
        """
        Retrieve the data for the given variable.

        Args:
            var (str): The variable to be retrieved.
            formula (bool): If True, the variable is a formula.
            long_name (str): The long name of the variable, if different from the variable name.
            units (str): The units of the variable, if different from the original units.
            short_name (str): The short name of the variable, if different from the variable name.
            reader_kwargs (dict): Additional keyword arguments for the Reader. Default is an empty dictionary.
        """
        # If the user requires a formula the evaluation requires the retrieval
        # of all the variables
        if formula:
            super().retrieve(reader_kwargs=reader_kwargs, months_required=2)
            self.logger.debug("Evaluating formula %s", var)
            self.data = EvaluateFormula(data=self.data, formula=var, long_name=long_name,
                                        short_name=short_name, units=units,
                                        loglevel=self.loglevel).evaluate()
            if self.data is None:
                raise ValueError(f'Error evaluating formula {var}. '
                                 'Check the variable names and the formula syntax.')
        else:
            super().retrieve(var=var, reader_kwargs=reader_kwargs, months_required=2)
            if self.data is None:
                raise ValueError(f'Variable {var} not found in the data. '
                                 'Check the variable name and the data source.')
            # Get the xr.DataArray to be aligned with the formula code
            self.data = self.data[var]

        if self.plt_startdate is None:
            self.plt_startdate = self.data.time.min().values
            self.logger.debug('Plot start date set to %s', self.plt_startdate)
        if self.plt_enddate is None:
            self.plt_enddate = self.data.time.max().values
            self.logger.debug('Plot end date set to %s', self.plt_enddate)

        # Customization of the data, expecially needed for formula
        if units is not None:
            self._check_data(var, units)
        if long_name is not None:
            self.data.attrs['long_name'] = long_name
        # We want to be sure that a long_name is always defined for description setup
        elif self.data.attrs.get('long_name') is None:
            self.data.attrs['long_name'] = var
        # We use the short_name as the name of the variable
        # to be always used in plots
        if short_name is not None:
            self.data.attrs['short_name'] = short_name
            self.data.name = short_name
        else:
            self.data.attrs['short_name'] = var

    def compute_std(self, freq: str, exclude_incomplete: bool = True, center_time: bool = True,
                    box_brd: bool = True):
        """
        Compute the standard deviation of the data. Support for monthly and annual frequencies.

        Args:
            freq (str): The frequency to be used for the resampling.
            exclude_incomplete (bool): If True, exclude incomplete periods.
            center_time (bool): If True, the time will be centered.
            box_brd (bool,opt): choose if coordinates are comprised or not in area selection.
                                Default is True
        """
        if freq is None:
            self.logger.error('Frequency not provided')
            raise ValueError('Frequency not provided')

        freq = frequency_string_to_pandas(freq)
        str_freq = pandas_freq_to_string(freq)
        self.logger.info('Computing %s standard deviation', str_freq)

        freq_dict = {'hourly': {'data': self.hourly, 'groupdby': 'time.hour'},
                     'daily': {'data': self.daily, 'groupdby': 'time.dayofyear'},
                     'monthly': {'data': self.monthly, 'groupdby': 'time.month'},
                     'annual': {'data': self.annual, 'groupdby': None}}

        data = self.data
        data = self.reader.fldmean(data, box_brd=box_brd,
                                   lon_limits=self.lon_limits, lat_limits=self.lat_limits)
        data = self.reader.timmean(data, freq=freq, exclude_incomplete=exclude_incomplete,
                                   center_time=center_time)
        data = data.sel(time=slice(self.std_startdate, self.std_enddate))
        if self.std_startdate is None or self.std_enddate is None:
            self.std_startdate = data.time.min().values
            self.std_enddate = data.time.max().values
        if freq_dict[str_freq]['groupdby'] is not None:
            data = data.groupby(freq_dict[str_freq]['groupdby']).std('time')
        else:  # For annual data, we compute the std over all years
            data = data.std('time')

        if self.region is not None:
            data.attrs['AQUA_region'] = self.region

        # Store start and end dates for the standard deviation.
        # pd.Timestamp cannot be used as attribute, so we convert to a string
        data.attrs['std_startdate'] = time_to_string(self.std_startdate)
        data.attrs['std_enddate'] = time_to_string(self.std_enddate)

        # Load data in memory for faster plot
        self.logger.debug(f"Loading std data for frequency {str_freq} in memory")
        data.load()
        self.logger.debug(f"Loaded std data for frequency {str_freq} in memory")

        # Assign the data to the correct attribute based on frequency
        if str_freq == 'hourly':
            self.std_hourly = data
        elif str_freq == 'daily':
            self.std_daily = data
        elif str_freq == 'monthly':
            self.std_monthly = data
        elif str_freq == 'annual':
            self.std_annual = data

    def save_netcdf(self, diagnostic_product: str, freq: str,
                    outputdir: str = './', rebuild: bool = True,
                    create_catalog_entry: bool = False,
                    dict_catalog_entry: dict = {'jinjalist': ['freq', 'realization', 'region'],
                                                'wildcardlist': ['var']}):
        """
        Save the data to a netcdf file.

        Args:
            diagnostic_product (str): The product name to be used in the filename (e.g., 'timeseries or 'seasonalcycles').
            freq (str): The frequency of the data.
            outputdir (str): The directory to save the data.
            rebuild (bool): If True, rebuild the data from the original files.
            create_catalog_entry (bool): If True, create a catalog entry for the data. Default is False.
            dict_catalog_entry (dict): A dictionary with catalog entry information. Default is {'jinjalist': ['freq', 'region', 'realization'], 'wildcardlist': ['var']}.
        """
        str_freq = pandas_freq_to_string(freq)

        if str_freq == 'hourly':
            data = self.hourly if self.hourly is not None else self.logger.error('No hourly data available')
            data_std = self.std_hourly if self.std_hourly is not None else None
        elif str_freq == 'daily':
            data = self.daily if self.daily is not None else self.logger.error('No daily data available')
            data_std = self.std_daily if self.std_daily is not None else None
        elif str_freq == 'monthly':
            data = self.monthly if self.monthly is not None else self.logger.error('No monthly data available')
            data_std = self.std_monthly if self.std_monthly is not None else None
        elif str_freq == 'annual':
            data = self.annual if self.annual is not None else self.logger.error('No annual data available')
            data_std = self.std_annual if self.std_annual is not None else None

        var = getattr(data, 'short_name', None)
        extra_keys = {'var': var, 'freq': str_freq}
        
        if data.name is None:
            data.name = var

        # In order to have a catalog entry we want to have a key region even in the global case
        region = self.region if self.region is not None else 'global'
        extra_keys.update({'region': region})

        self.logger.info('Saving %s data for %s to netcdf in %s', str_freq, diagnostic_product, outputdir)

        # Loading data in memory before saving to netcdf
        super().save_netcdf(data=data, diagnostic=self.diagnostic_name, diagnostic_product=diagnostic_product,
                            outputdir=outputdir, rebuild=rebuild, extra_keys=extra_keys,
                            create_catalog_entry=create_catalog_entry, dict_catalog_entry=dict_catalog_entry)
        if data_std is not None:
            extra_keys.update({'std': 'std'})
            self.logger.info('Saving %s data for %s to netcdf in %s', str_freq, diagnostic_product, outputdir)
            # TODO: Check if the catalog entry generation is required for the std values
            super().save_netcdf(data=data_std, diagnostic=self.diagnostic_name, diagnostic_product=diagnostic_product,
                                outputdir=outputdir, rebuild=rebuild, extra_keys=extra_keys)

    def _check_data(self, var: str, units: str):
        """
        Make sure that the data is in the correct units.

        Args:
            var (str): The variable to be checked.
            units (str): The units to be checked.
        """
        self.data = super()._check_data(data=self.data, var=var, units=units)


class PlotBaseMixin():
    """PlotBaseMixin class is used for the PlotTimeseries and the PlotSeasonalcycles classes."""
    def __init__(self, diagnostic_name: str = 'timeseries', loglevel: str = 'WARNING'):
        """
        Initialize the PlotBaseMixin class.

        Args:
            diagnostic_name (str): The name of the diagnostic. Default is 'timeseries'.
                                   This will be used to configure the logger and the output files.
            loglevel (str): The log level to be used. Default is 'WARNING'.
        """
        # Data info initalized as empty
        self.loglevel = loglevel
        self.diagnostic_name = diagnostic_name
        log_name = 'Plot' + diagnostic_name.capitalize()
        self.logger = log_configure(log_level=loglevel, log_name=log_name)
        # All these will be filled by the get_data_info() methods
        self.catalogs = None
        self.models = None
        self.exps = None
        self.realizations = None
        self.ref_catalogs = None
        self.ref_models = None
        self.ref_exps = None
        self.std_startdate = None
        self.std_enddate = None
        self.region = None
        self.short_name = None
        self.long_name = None
        self.units = None

    def set_data_labels(self):
        """
        Set the data labels for the plot.
        The labels are extracted from the data arrays attributes.

        Returns:
            data_labels (list): List of data labels for the plot.
        """
        data_labels = []
        for i in range(self.len_data):
            label = f'{self.models[i]} {self.exps[i]}'
            data_labels.append(label)

        self.logger.debug('Data labels: %s', data_labels)
        return data_labels

    def set_ref_label(self):
        """
        Set the reference label for the plot.
        The label is extracted from the reference data arrays attributes.

        Returns:
            ref_label (str): Reference label for the plot.
        """
        ref_label = []
        for i in range(self.len_ref):
            label = f'{self.ref_models[i] if isinstance(self.ref_models, list) else self.ref_models}'
            label += f' {self.ref_exps[i] if isinstance(self.ref_exps, list) else self.ref_exps}'
            ref_label.append(label)
        self.logger.debug('Reference labels: %s', ref_label)

        # Convert to string if only one reference data
        if len(ref_label) == 1:
            ref_label = ref_label[0]

        self.logger.debug('Reference label: %s', ref_label)
        return ref_label

    def set_title(self, diagnostic: str = None):
        """
        Set the title for the plot. Uses short_name, long_name, and units attributes.

        Args:
            diagnostic (str): Diagnostic name to be used in the title.

        Returns:
            title (str): Title for the plot.
        """
        title = f'{diagnostic} '
        if self.long_name is not None:
            title += f'of {self.long_name} '

        if self.units is not None:
            title += f'[{self.units}] '

        if self.region is not None:
            title += f'[{self.region}] '

        if self.len_data == 1:
            title += f'for {self.catalogs[0]} {self.models[0]} {self.exps[0]} '

        self.logger.debug('Title: %s', title)
        return title

    def set_description(self, diagnostic: str = None):
        """
        Set the caption for the plot.
        The caption is extracted from the data arrays attributes and the
        reference data arrays attributes.
        The caption is stored as 'Description' in the metadata dictionary.

        Args:
            diagnostic (str): Diagnostic name to be used in the caption.

        Returns:
            description (str): Caption for the plot.
        """

        description = f'{diagnostic} '

        description += f'of {self.long_name} '
        if self.units is not None:
          units = self.units.replace("**", r"\*\*")
          description += f'[{units}] '
        if self.short_name is not None:
          description += f'({self.short_name}) '

        if self.region is not None:
            description += f'for region {self.region} '

        description += 'for '
        description += strlist_to_phrase(items=[f'{self.catalogs[i]} {self.models[i]} {self.exps[i]}' for i in range(self.len_data)])

        if self.len_ref > 0:
            description += f' with reference'
            for i in range(self.len_ref):
                if self.ref_models[i] == 'ERA5' or self.ref_models == 'ERA5':
                    description += f' ERA5 '
                elif isinstance(self.ref_models, list):
                    description += f' {self.ref_models[i]} {self.ref_exps[i]} '
                else:
                    description += f' {self.ref_models} {self.ref_exps} '
        elif self.len_ref == 0:
            description += '.'

        if self.std_startdate is not None and self.std_enddate is not None:
            description += f'with standard deviation from {time_to_string(self.std_startdate)} to {time_to_string(self.std_enddate)}.'
            description += ' The shaded area represents 2 standard deviations.'

        self.logger.debug('Description: %s', description)
        return description

    def save_plot(self, fig, description: str = None, rebuild: bool = True,
                  outputdir: str = './', dpi: int = 300, format: str = 'png', diagnostic_product: str = None):
        """
        Save the plot to a file.

        Args:
            fig (matplotlib.figure.Figure): Figure object.
            description (str): Description of the plot.
            rebuild (bool): If True, rebuild the plot even if it already exists.
            outputdir (str): Output directory to save the plot.
            dpi (int): Dots per inch for the plot.
            format (str): Format of the plot ('png' or 'pdf'). Default is 'png'.
            diagnostic_product (str): Diagnostic product to be used in the filename as diagnostic_product.
        """
        outputsaver = OutputSaver(diagnostic=self.diagnostic_name,
                                  catalog=self.catalogs,
                                  model=self.models,
                                  exp=self.exps,
                                  realization=self.realizations,
                                  # This is needed for the Gregory diagnostic, which save the reference models and experiments
                                  # as dictionaries to build correct labels and descriptions
                                  catalog_ref=list(self.ref_catalogs.values()) if isinstance(self.ref_catalogs, dict) else self.ref_catalogs,
                                  model_ref=list(self.ref_models.values()) if isinstance(self.ref_models, dict) else self.ref_models,
                                  exp_ref=list(self.ref_exps.values()) if isinstance(self.ref_exps, dict) else self.ref_exps,
                                  outputdir=outputdir,
                                  loglevel=self.loglevel)

        metadata = {"Description": description, "dpi": dpi }
        extra_keys = {'diagnostic_product': diagnostic_product}

        if self.short_name is not None:
            extra_keys.update({'var': self.short_name})
        if self.region is not None:
            extra_keys.update({'region': self.region})

        outputsaver.save_figure(fig, diagnostic_product,
                                extra_keys=extra_keys, metadata=metadata,
                                save_pdf=format in ['pdf', 'both'], 
                                save_png=format in ['png', 'both'],
                                rebuild=rebuild, dpi=dpi)
