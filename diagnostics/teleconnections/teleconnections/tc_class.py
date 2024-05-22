"""Module for teleconnection class.

This module contains the teleconnection class, which is used to
evaluate teleconnection indices and regressions.
The teleconnection class is initialized with one model, so that
it can evaluate indices and regressions for a single model at a time.
Multiples models can be evaluated by initializing multiple teleconnection
objects.
Different teleconnections can be evaluated for the same model.

Available teleconnections:
    - NAO
    - ENSO
"""
import os
import xarray as xr
import pandas as pd

from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure
from aqua.reader import Reader, inspect_catalogue
from aqua.util import ConfigPath, create_folder
from teleconnections.index import station_based_index, regional_mean_anomalies
from teleconnections.plots import index_plot
from teleconnections.statistics import reg_evaluation, cor_evaluation
from teleconnections.tools import TeleconnectionsConfig, set_filename

xr.set_options(keep_attrs=True)


class Teleconnection():
    """Class for teleconnection objects."""

    def __init__(self, model: str, exp: str, source: str,
                 telecname: str,
                 configdir=None, aquaconfigdir=None,
                 interface='teleconnections-destine',
                 regrid=None, freq=None,
                 savefig=False, outputfig=None,
                 savefile=False, outputdir=None,
                 filename=None,
                 startdate=None, enddate=None,
                 months_window: int = 3, loglevel: str = 'WARNING'):
        """
        Args:
            model (str):                    Model name.
            exp (str):                      Experiment name.
            source (str):                   Source name.
            telecname (str):                Teleconnection name.
                                            See documentation for available teleconnections.
            configdir (str, optional):      Path to diagnostics configuration folder.
            aquaconfigdir (str, optional):  Path to AQUA configuration folder.
            interface (str, optional):      Interface filename. Defaults to 'teleconnections-destine'.
            regrid (str, optional):         Regridding resolution. Defaults to None.
            freq (str, optional):           Frequency of the data. Defaults to None.
            savefig (bool, optional):       Save figures if True. Defaults to False.
            outputfig (str, optional):      Output directory for figures.
                                            If None, the current directory is used.
            savefile (bool, optional):      Save files if True. Defaults to False.
            outputdir (str, optional):      Output directory for files.
                                            If None, the current directory is used.
            filename (str, optional):       Output filename.
            startdate (str, optional):     Start date for the data.
                                            Format: YYYY-MM-DD. Defaults to None.
            enddate (str, optional):        End date for the data.
                                            Format: YYYY-MM-DD. Defaults to None.
            months_window (int, optional):  Months window for teleconnection
                                            index. Defaults to 3.
            loglevel (str, optional):       Log level. Defaults to 'WARNING'.

        Raises:
            NoDataError: If the data is not available.
            ValueError: If telecname is not one of the available teleconnections.
        """

        # Configure logger
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Teleconnection')

        # Reader variables
        self.model = model
        self.exp = exp
        self.source = source

        self.startdate = startdate
        self.enddate = enddate

        # Load AQUA config and check that the data is available
        self.machine = None
        self.aquaconfigdir = aquaconfigdir
        self._aqua_config()

        self.regrid = regrid
        if self.regrid is None:
            self.logger.info('No regrid will be performed, be sure that the data is '
                                'already at low resolution')
        self.logger.debug("Regrid resolution: %s", self.regrid)

        self.freq = freq
        if self.freq is None:
            self.logger.info('No time aggregation will be performed, be sure that the data is '
                             'already at the desired frequency')
        self.logger.debug("Frequency: %s", self.freq)

        # Teleconnection variables
        avail_telec = ['NAO', 'ENSO', 'MJO']
        if telecname in avail_telec:
            self.telecname = telecname
            if self.telecname == 'MJO':
                raise NotImplementedError('MJO teleconnection not implemented yet')
        else:
            raise ValueError('telecname must be one of {}'.format(avail_telec))

        self._load_namelist(configdir=configdir, interface=interface)

        # Variable to be used for teleconnection
        self.var = self.namelist[self.telecname]['field']
        self.logger.debug("Teleconnection variable: %s", self.var)

        # The teleconnection type is used to select the correct function
        self.telec_type = self.namelist[self.telecname]['telec_type']

        # At the moment it is used by all teleconnections
        self.months_window = months_window

        # Output variables
        self.outputfig = None
        self.outputdir = None

        self._load_figs_options(savefig, outputfig)
        self._load_data_options(savefile, outputdir)
        if self.savefile or self.savefig:
            self._filename(filename)
            if self.savefile:
                self.logger.info("Saving file to %s/%s", outputdir, self.filename)
            if self.savefig:
                self.logger.info("Saving figures to %s/%s", outputfig, self.filename)

        # Data empty at the beginning
        self.data = None
        self.index = None

        # Initialize the Reader class
        # Notice that reader is a private method
        # but **kwargs are passed to it so that it can be used to pass
        # arguments to the reader if needed
        self._reader(startdate=self.startdate, enddate=self.enddate)

    def retrieve(self, var=None, **kwargs):
        """Retrieve teleconnection data with the AQUA reader.
        The data is saved as teleconnection attribute and can be accessed
        with self.data.
        If var is not None, the data is not saved as teleconnection attribute
        and can be accessed with the returned value.

        Args:
            var (str, optional): Variable to be retrieved.
                                 If None, the variable specified in the namelist
            **kwargs: Keyword arguments to be passed to the reader.

        Returns:
            xarray.DataArray: Data retrieved if a variable is specified.

        Raises:
            NoDataError: If the data is not available.
        """
        if var is None:
            try:
                self.data = self.reader.retrieve(var=self.var, **kwargs)
            except (ValueError, KeyError) as e:
                self.logger.debug(f"Error: {e}")
                raise NoDataError("Variable {} not found".format(self.var)) from e
        else:
            try:
                data = self.reader.retrieve(var=var, **kwargs)
            except (ValueError, KeyError) as e:
                self.logger.debug(f"Error: {e}")
                raise NoDataError('Variable {} not found'.format(var)) from e
        self.logger.info('Data retrieved')

        if self.regrid:
            self.data = self.reader.regrid(self.data)
            self.logger.info('Data regridded to %s', self.regrid)

        if self.freq:
            if self.freq == 'monthly':
                self.data = self.reader.timmean(data=self.data, freq=self.freq)
                self.logger.info('Time aggregated to %s', self.freq)
            else:
                self.logger.warning('Time aggregation %s not implemented for teleconnections', self.freq)

        if var:
            self.logger.info("Returning data as xarray.DataArray")
            return data

    def evaluate_index(self, rebuild=False, **kwargs):
        """Evaluate teleconnection index.
        The index is saved as teleconnection attribute and can be accessed
        with self.index.

        Args:
            rebuild (bool, optional): If True, the index is recalculated.
                                      Default is False.
            **kwargs: Keyword arguments to be passed to the index function.

        Raises:
            ValueError: If the index is not calculated correctly.
        """
        if self.data is None and self.index is None:
            self.logger.info('No retrieve has been performed, trying to retrieve')
            self.retrieve()

        if self.index is not None and not rebuild:
            self.logger.warning('Index already calculated, skipping')
            return
        elif self.index is None and not rebuild and self.savefile:
            self._check_index_file()

        if rebuild and self.index is not None:
            self.logger.info('Rebuilding index')
            self.index = None

        if self.index is not None:
            return

        # Check that data have at least 2 years:
        if len(self.data[self.var].time) < 24:
            raise NotEnoughDataError('Data have less than 24 months')

        if self.telec_type == 'station':
            self.index = station_based_index(field=self.data[self.var],
                                             namelist=self.namelist,
                                             telecname=self.telecname,
                                             months_window=self.months_window,
                                             loglevel=self.loglevel, **kwargs)
        elif self.telec_type == 'region':
            self.index = regional_mean_anomalies(field=self.data[self.var],
                                                 namelist=self.namelist,
                                                 telecname=self.telecname,
                                                 months_window=self.months_window,
                                                 loglevel=self.loglevel, **kwargs)

        self.logger.info('Index evaluated')
        if self.index is None:
            raise ValueError('It was not possible to calculate the index')

        if self.savefile:
            filename = set_filename(self.filename, 'index')
            file = self.outputdir + '/' + filename + '.nc'
            self.index.to_netcdf(file)
            self.logger.info('Index saved to %s', file)

    def evaluate_regression(self, data=None, var=None,
                            dim: str = 'time',
                            season=None):
        """
        Evaluate teleconnection regression.
        If var is None, the regression is calculated between the teleconnection
        index and the teleconnection variable.
        If var is not None, the regression is calculated between the teleconnection
        index and the specified variable.
        The regression is returned as xarray.DataArray.

        Args:
            data (xarray.DataArray, optional): Data to be used for regression.
                                               If None, the data used for the index is used.
            var (str, optional):               Variable to be used for regression.
                                               If None, the variable used for the index is used.
            dim (str, optional):               Dimension to be used for regression.
                                               Default is 'time'.
            season (str, optional):            Season to be selected. Default is None.

        Returns:
            xarray.DataArray: Regression map
        """
        # We prepare the data for the regression, season selection is done
        # inside the function reg_evaluation, because it can be used
        # also as a standalone function
        data, dim = self._prepare_corr_reg(var=var, data=data, dim=dim)

        reg = reg_evaluation(indx=self.index, data=data, dim=dim, season=season)

        if self.savefile:
            if var:
                add = 'regression'
                if season:
                    add += '_' + season
                add += '_' + var
                filename = set_filename(self.filename, add)
                file = self.outputdir + '/' + filename
                file += '.nc'
            else:
                add = 'regression'
                if season:
                    add += '_' + season
                filename = set_filename(self.filename, add)
                file = self.outputdir + '/' + filename
                file += '.nc'
            reg.to_netcdf(file)
            self.logger.info("Regression saved to %s", file)

        return reg

    def evaluate_correlation(self, data=None, var=None,
                             dim: str = 'time',
                             season=None):
        """
        Evaluate teleconnection correlation.
        If var is None, the correlation is calculated between the teleconnection
        index and the teleconnection variable.
        If var is not None, the correlation is calculated between the teleconnection
        index and the specified variable.
        The correlation is returned as xarray.DataArray.

        Args:
            data (xarray.DataArray, optional): Data to be used for correlation.
                                               If None, the data used for the index is used.
            var (str, optional):               Variable to be used for correlation.
                                               If None, the variable used for the index is used.
            dim (str, optional):               Dimension to be used for correlation.
                                               Default is 'time'.
            season (str, optional):            Season to be selected. Default is None.

        Returns:
            xarray.DataArray: Correlation map
        """
        # We prepare the data for the regression, season selection is done
        # inside the function reg_evaluation, because it can be used
        # also as a standalone function
        data, dim = self._prepare_corr_reg(var=var, data=data, dim=dim)

        cor = cor_evaluation(indx=self.index, data=data, dim=dim, season=season)

        if self.savefile:
            add = 'correlation'
            if var:
                add += '_' + var
                if season:
                    add += '_' + season
                filename = set_filename(self.filename, add)
                file = self.outputdir + '/' + filename
                file += '.nc'
            else:
                add = 'correlation'
                if season:
                    add += '_' + season
                filename = set_filename(self.filename, add)
                file = self.outputdir + '/' + filename
                file += '.nc'
            cor.to_netcdf(file)
            self.logger.info("Correlation saved to %s", file)

        return cor

    def plot_index(self, step=False, **kwargs):
        """
        Plot teleconnection index.

        Args:
            step (bool, optional): If True, plot the index with a step function (experimental)
            **kwargs: Keyword arguments to be passed to the index_plot function.
        """

        if self.index is None:
            self.logger.warning('No index has been calculated, trying to calculate')
            self.evaluate_index()

        title = kwargs.get('title', None)
        if title is None:
            title = 'Index' + ' ' + self.telecname + ' ' + self.model + ' ' + self.exp

        ylabel = self.telecname + ' index'

        if self.savefig:
            filename = set_filename(self.filename, 'index')
            filename += '.pdf'

            index_plot(indx=self.index, save=self.savefig,
                       outputdir=self.outputfig, filename=filename,
                       loglevel=self.loglevel, step=step,
                       ylabel=ylabel, **kwargs)
            self.logger.info("Index plot saved to %s/%s", self.outputfig, filename)
        else:
            index_plot(indx=self.index, save=self.savefig,
                       loglevel=self.loglevel, step=step,
                       ylabel=ylabel, **kwargs)

    def _load_namelist(self, configdir=None, interface=None):
        """Load namelist.

        Args:
            configdir (str, optional): Path to diagnostics configuration folder.
                                       If None, the default diagnostics folder is used.
        """
        config = TeleconnectionsConfig(configdir=configdir, interface=interface)

        self.namelist = config.load_namelist()
        self.logger.info('Namelist loaded')

    def _aqua_config(self):
        """Load AQUA configuration.

        Raises:
            NoDataError: If the data is not available.
        """
        aqua_config = ConfigPath(configdir=self.aquaconfigdir)
        self.machine = aqua_config.machine
        self.logger.debug("Machine: %s", self.machine)

        # Check that the data is available in the catalogue
        if inspect_catalogue(model=self.model, exp=self.exp,
                             source=self.source,
                             verbose=False) is False:
            raise NoDataError('Data not available')

    def _load_figs_options(self, savefig=False, outputfig=None):
        """Load the figure options.
        Args:
            savefig (bool): whether to save the figures.
                            Default is False.
            outputfig (str): path to the figure output directory.
                             Default is None.
                             See init for the class default value.
        """
        self.savefig = savefig

        if self.savefig:
            self.logger.debug('Figures will be saved')
            self._load_folder_info(outputfig, 'figure')

    def _load_data_options(self, savefile=False, outputdir=None):
        """Load the data options.
        Args:
            savefile (bool): whether to save the data.
                             Default is False.
            outputdir (str): path to the data output directory.
                             Default is None.
                             See init for the class default value.
        """
        self.savefile = savefile

        if self.savefile:
            self.logger.debug('Data will be saved')
            self._load_folder_info(outputdir, 'data')

    def _filename(self, filename=None):
        """Generate the output file name.
        Args:
            filename (str): name of the output file.
                            Default is None.
        """
        if filename is None:
            self.logger.info('No filename specified, using the default name')
            filename = 'teleconnections_' + self.telecname + '_' + self.model + '_' + self.exp + '_'\
                       + self.source
        self.filename = filename
        self.logger.debug("Output filename: %s", self.filename)

    def _load_folder_info(self, folder=None, folder_type=None):
        """Load the folder information.
        Args:
            folder (str): path to the folder.
                          Default is None.
            folder_type (str): type of the folder.
                               Default is None.

        Raises:
            KeyError: if the folder_type is not recognised.
            TypeError: if the folder_type is not a string.
        """
        if folder_type not in ['figure', 'data']:
            raise KeyError('The folder_type must be either figure or data')

        if not folder:
            self.logger.warning("No %s folder specified, using the current directory", folder_type)
            folder = os.getcwd()
        else:
            if not isinstance(folder, str):
                raise TypeError('The folder must be a string')
            create_folder(folder, self.loglevel)

        # Store the folder in the class
        if folder_type == 'figure':
            self.outputfig = folder
            self.logger.debug("Figure output folder: %s", self.outputfig)
        elif folder_type == 'data':
            self.outputdir = folder
            self.logger.debug("Data output folder: %s", self.outputdir)

    def _reader(self, **kwargs):
        """Initialize AQUA reader.

        Args:
            **kwargs: Keyword arguments to be passed to the reader.
        """

        self.reader = Reader(model=self.model, exp=self.exp, source=self.source,
                             regrid=self.regrid, loglevel=self.loglevel, **kwargs)
        self.logger.info('Reader initialized')

    def _prepare_corr_reg(self, data=None, var=None,
                          dim='time'):
        """Prepare data for the correlation or regression evaluation.

        Args:
            data (xarray.DataArray, optional): Data to be used for the regression.
                                               If None, the teleconnection data is used.
            var (str, optional): Variable to be used for the regression.
                                 If None, the teleconnection variable is used.
            dim (str, optional): Dimension to be used for the regression.
                                 Default is 'time'.

        Returns:
            data (xarray.DataArray): Data to be used for the regression or correlation.
            dim (str): Dimension to be used for the regression or correlation.
        """
        if self.index is None:
            self.logger.warning('No index has been calculated, trying to calculate')
            self.evaluate_index()

        if var is None:  # Use the teleconnection variable
            self.logger.debug('No variable specified, using teleconnection variable')
            var = self.var

        if data is None and var == self.var:  # Use the teleconnection data
            self.logger.debug('No data specified, using teleconnection data')
            if self.data is None:
                self.logger.warning('No data has been loaded, trying to retrieve it')
                self.retrieve()  # this will load the data in self.data
            data = self.data
            data = data[var]

            return data, dim

        if var != self.var:
            self.logger.debug("Variable %s is different from teleconnection variable %s",
                              var, self.var)
            self.logger.info("The result won't be saved as teleconnection attribute, but returned")

            if data is not None:
                try:
                    data = data[var]
                except KeyError:
                    return data, dim

                return data, dim
            else:  # data is None
                self.logger.debug('No data specified, trying to retrieve it')
                data = self.retrieve(var=var)
                data = data[var]

                return data, dim

        return data, dim

    def _check_index_file(self):
        """Check if the index file is already present."""
        self.logger.debug("Checking if index has been calculated in a previous session")
        filename = set_filename(self.filename, 'index')
        file = self.outputdir + '/' + filename + '.nc'
        if os.path.isfile(file):
            self.logger.info('Index found in %s', file)
            self.index = xr.open_mfdataset(file)
            if isinstance(self.index, xr.Dataset):
                try:
                    self.index = self.index['index']
                except KeyError:
                    self.logger.warning("Index not found in the file, rebuilding")
                    self.index = None
                    return
            else:
                self.logger.warning("Index is not a Dataset, skipping")
                self.index = None
                return

            # Checking if the index has the correct time span
            self._check_index_time()

    def _check_index_time(self):
        """Check if the index has the correct time span."""
        index_start = self.index.time[0].values
        index_end = self.index.time[-1].values

        self.logger.debug(f"Index has time span {index_start} - {index_end}")

        data_start = self.startdate if self.startdate is not None else self.data[self.var].time[0].values
        data_end = self.enddate if self.enddate is not None else self.data[self.var].time[-1].values

        self.logger.debug(f"Selected time span: {data_start} - {data_end}")

        # Adapt the data time span since the first and last month are dropped in the index evaluation
        # formula adapted to a generic months_window
        data_start = data_start + pd.DateOffset(months=(self.months_window-1)/2)
        data_end = data_end - pd.DateOffset(months=(self.months_window-1)/2)

        if index_start == data_start and index_end == data_end:
            self.logger.info("Index has the correct time span, skipping the evaluation")
            return
        else:
            self.logger.debug("Index has a different time span, rebuilding the index")
            self.index = None
            return
