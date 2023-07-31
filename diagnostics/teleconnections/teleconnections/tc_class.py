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

from aqua.logger import log_configure
from aqua.reader import Reader
from teleconnections.index import station_based_index, regional_mean_anomalies
from teleconnections.plots import index_plot
from teleconnections.statistics import reg_evaluation, cor_evaluation
from teleconnections.tools import load_namelist


class Teleconnection():
    """Class for teleconnection objects."""

    def __init__(self, model: str, exp: str, source: str,
                 telecname: str, configdir=None,
                 regrid=None, freq=None,
                 zoom=None,
                 savefig=False, outputfig=None,
                 savefile=False, outputdir=None,
                 filename=None,
                 months_window: int = 3, loglevel: str = 'WARNING'):
        """
        Args:
            model (str):                    Model name.
            exp (str):                      Experiment name.
            source (str):                   Source name.
            telecname (str):                Teleconnection name.
                                            See documentation for available teleconnections.
            configdir (str, optional):      Path to diagnostics configuration folder.
            regrid (str, optional):         Regridding resolution. Defaults to None.
            freq (str, optional):           Frequency of the data. Defaults to None.
            zoom (str, optional):           Zoom for ICON data. Defaults to None.
            savefig (bool, optional):       Save figures if True. Defaults to False.
            outputfig (str, optional):      Output directory for figures.
                                            If None, the current directory is used.
            savefile (bool, optional):      Save files if True. Defaults to False.
            outputdir (str, optional):      Output directory for files.
                                            If None, the current directory is used.
            filename (str, optional):       Output filename.
            months_window (int, optional):  Months window for teleconnection
                                            index. Defaults to 3.
            loglevel (str, optional):       Log level. Defaults to 'WARNING'.

        Raises:
            ValueError: If telecname is not one of the available teleconnections.
        """

        # Configure logger
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Teleconnection')

        # Reader variables
        self.model = model
        self.exp = exp
        self.source = source
        self.logger.debug('Open dataset: {}/{}/{}'.format(self.model, self.exp,
                                                          self.source))

        self.regrid = regrid
        if self.regrid is None:
            self.logger.warning('No regridding will be performed')
            self.logger.info('Be sure that the data is already regridded')
        self.logger.debug('Regridding resolution: {}'.format(self.regrid))

        self.freq = freq
        if self.freq is None:
            self.logger.warning('No time aggregation will be performed')
            self.logger.info('Be sure that the data is already monthly aggregated')
        self.logger.debug('Frequency: {}'.format(self.freq))

        self.zoom = zoom
        if self.zoom is not None:
            self.logger.debug('Zoom: {}'.format(self.zoom))

        # Teleconnection variables
        avail_telec = ['NAO', 'ENSO']
        if telecname in avail_telec:
            self.telecname = telecname
        else:
            raise ValueError('telecname must be one of {}'.format(avail_telec))

        self._load_namelist(configdir=configdir)

        # Variable to be used for teleconnection
        self.var = self.namelist[self.telecname]['field']
        self.logger.debug('Teleconnection variable: {}'.format(self.var))

        # The teleconnection type is used to select the correct function
        self.telec_type = self.namelist[self.telecname]['telec_type']
        self.logger.debug('Teleconnection type: {}'.format(self.telec_type))

        # At the moment it is used by all teleconnections
        if self.telecname == 'NAO' or self.telecname == 'ENSO':
            self.months_window = months_window

        # Output variables
        self._load_figs_options(savefig, outputfig)
        self._load_data_options(savefile, outputdir)
        if self.savefile or self.savefig:
            self._filename(filename)

        # Data empty at the beginning
        self.data = None
        self.index = None
        self.regression = None
        self.correlation = None

        # Initialize the Reader class
        # Notice that reader is a private method
        # but **kwargs are passed to it so that it can be used to pass
        # arguments to the reader if needed

        if self.zoom:
            self._reader(zoom=self.zoom)
        else:
            self._reader()

    def _load_namelist(self, configdir=None):
        """Load namelist.

        Args:
            configdir (str, optional): Path to diagnostics configuration folder.
                                       If None, the default diagnostics folder is used.
        """

        self.namelist = load_namelist(diagname='teleconnections',
                                      configdir=configdir)
        self.logger.info('Namelist loaded')

    def _reader(self, **kwargs):
        """Initialize AQUA reader.

        Args:
            **kwargs: Keyword arguments to be passed to the reader.
        """

        self.reader = Reader(model=self.model, exp=self.exp, source=self.source,
                             regrid=self.regrid, freq=self.freq,
                             loglevel=self.loglevel, **kwargs)
        self.logger.info('Reader initialized')

    def run(self):
        """Run teleconnection analysis.

        The analysis consists of:

        - Retrieving the data

        - Evaluating the teleconnection index

        - Evaluating the regression

        - Evaluating the correlation

        These methods can be also run separately.
        """

        self.logger.debug('Running teleconnection analysis for data: {}/{}/{}'
                          .format(self.model, self.exp, self.source))

        self.retrieve()
        self.evaluate_index()
        self.evaluate_regression()
        self.evaluate_correlation()

        self.logger.info('Teleconnection analysis completed')

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
        """
        if var is None:
            try:
                self.data = self.reader.retrieve(var=self.var, **kwargs)
            except ValueError:
                self.logger.warning('Variable {} not found'.format(self.var))
                self.logger.warning('Trying to retrieve without fixing and **kwargs')
                self.data = self.reader.retrieve(var=self.var, fix=False)
            self.logger.info('Data retrieved')

            if self.regrid:
                self.data = self.reader.regrid(self.data)
                self.logger.info('Data regridded')

            if self.freq:
                if self.freq == 'monthly':
                    self.data = self.reader.timmean(self.data)
                    self.logger.info('Time aggregated to {}'.format(self.freq))
        else:
            try:
                data = self.reader.retrieve(var=var, **kwargs)
            except ValueError:
                self.logger.warning('Variable {} not found'.format(var))
                self.logger.warning('Trying to retrieve without fixing and **kwargs')
                data = self.reader.retrieve(var=var, fix=False)
            self.logger.info('Data retrieved')

            if self.regrid:
                data = self.reader.regrid(data)
                self.logger.info('Data regridded')

            if self.freq:
                if self.freq == 'monthly':
                    data = self.reader.timmean(data)
                    self.logger.info('Time aggregated to {}'.format(self.freq))

            return data

    def evaluate_index(self, rebuild=False, **kwargs):
        """Evaluate teleconnection index.
        The index is saved as teleconnection attribute and can be accessed
        with self.index.

        Args:
            rebuild (bool, optional): If True, the index is recalculated.
                                      Default is False.
            **kwargs: Keyword arguments to be passed to the index function.
        """

        if self.index is not None and not rebuild:
            self.logger.warning('Index already calculated, skipping')
            return

        if self.data is None:
            self.logger.warning('No retrieve has been performed, trying to retrieve')
            self.retrieve()

        if self.telec_type == 'station':
            self.logger.debug('Calculating {} index'.format(self.telecname))
            self.index = station_based_index(field=self.data[self.var],
                                             namelist=self.namelist,
                                             telecname=self.telecname,
                                             months_window=self.months_window,
                                             loglevel=self.loglevel, **kwargs)
        elif self.telec_type == 'region':
            self.logger.debug('Calculating {} index'.format(self.telecname))
            self.index = regional_mean_anomalies(field=self.data[self.var],
                                                 namelist=self.namelist,
                                                 telecname=self.telecname,
                                                 months_window=self.months_window,
                                                 loglevel=self.loglevel, **kwargs)

        # HACK: ICON has a depth_full dimension that is not used
        #       but it is not removed by the index evaluation
        if self.model == 'ICON':
            try:
                self.index = self.index.isel(depth_full=0)
            except ValueError:
                self.index = self.index

        self.logger.debug(self.telecname + ' index calculated')
        if self.index is None:
            raise ValueError('Index not calculated')

        if self.savefile:
            file = self.outputdir + '/' + self.filename + '_index.nc'
            self.index.to_netcdf(file)
            self.logger.info('Index saved to {}'.format(file))

    def evaluate_regression(self, data=None, var=None, dim='time',
                            rebuild=False):
        """Evaluate teleconnection regression.
        If var is None, the regression is calculated between the teleconnection
        index and the teleconnection variable. The regression is saved as
        teleconnection attribute and can be accessed with self.regression.
        If var is not None, the regression is calculated between the teleconnection
        index and the specified variable. The regression is not saved as
        teleconnection attribute and can be accessed with the returned value.

        Args:
            data (xarray.DataArray, optional): Data to be used for regression.
                                               If None, the data used for the index is used.
            var (str, optional):               Variable to be used for regression.
                                               If None, the variable used for the index is used.
            dim (str, optional):               Dimension to be used for regression.
                                               Default is 'time'.
            rebuild (bool, optional):          If True, the regression is recalculated.
                                               Default is False.

        Returns:
            xarray.DataArray: Regression map if var is not None.
        """
        if self.regression is not None and var is None and not rebuild:
            self.logger.warning('Regression already calculated, skipping')
            return

        data, dim = self._prepare_corr_reg(var=var, data=data, dim=dim)

        if var is None:
            self.regression = reg_evaluation(indx=self.index, data=data,
                                             dim=dim)
            # HACK: ICON has a depth_full dimension that is not used
            #       but it is not removed by the regression evaluation
            if self.model == 'ICON':
                try:
                    self.regression = self.regression.isel(depth_full=0)
                except ValueError:
                    self.regression = self.regression
        else:
            reg = reg_evaluation(indx=self.index, data=data, dim=dim)
            # HACK: ICON has a depth_full dimension that is not used
            #       but it is not removed by the regression evaluation
            if self.model == 'ICON':
                try:
                    reg = reg.isel(depth_full=0)
                except ValueError:
                    reg = reg

        if self.savefile and var is None:
            file = self.outputdir + '/' + self.filename + '_regression.nc'
            self.regression.to_netcdf(file)
            self.logger.info('Regression saved to {}'.format(file))
        elif self.savefile and var is not None:
            file = self.outputdir + '/' + self.filename + '_regression_{}.nc'.format(var)
            reg.to_netcdf(file)
            self.logger.info('Regression saved to {}'.format(file))

        if var is None:
            return
        else:
            return reg

    def evaluate_correlation(self, data=None, var=None, dim='time',
                             rebuild=False):
        """Evaluate teleconnection correlation.
        If var is None, the correlation is calculated between the teleconnection
        index and the teleconnection variable. The correlation is saved as
        teleconnection attribute and can be accessed with self.correlation.
        If var is not None, the correlation is calculated between the teleconnection
        index and the specified variable. The correlation is not saved as
        teleconnection attribute and can be accessed with the returned value.

        Args:
            data (xarray.DataArray, optional): Data to be used for correlation.
                                               If None, the data used for the index is used.
            var (str, optional):               Variable to be used for correlation.
                                               If None, the variable used for the index is used.
            dim (str, optional):               Dimension to be used for correlation.
                                               Default is 'time'.
            rebuild (bool, optional):          If True, the correlation is recalculated.
                                               Default is False.

        Returns:
            xarray.DataArray: Correlation map if var is not None.
        """
        if self.correlation is not None and var is None and not rebuild:
            self.logger.warning('Correlation already calculated, skipping')
            return

        data, dim = self._prepare_corr_reg(var=var, data=data, dim=dim)

        if var is None:
            self.correlation = cor_evaluation(indx=self.index, data=data,
                                              dim=dim)
        else:
            cor = cor_evaluation(indx=self.index, data=data, dim=dim)

        if self.savefile and var is None:
            file = self.outputdir + '/' + self.filename + '_correlation.nc'
            self.correlation.to_netcdf(file)
            self.logger.info('Correlation saved to {}'.format(file))
        elif self.savefile and var is not None:
            file = self.outputdir + '/' + self.filename + '_correlation_{}.nc'.format(var)
            cor.to_netcdf(file)
            self.logger.info('Correlation saved to {}'.format(file))

        if var is None:
            return
        else:
            return cor

    def plot_index(self, step=False, **kwargs):
        """Plot teleconnection index.

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
            # Set the filename
            filename = self.filename + '_index.pdf'
            self.logger.info('Index plot saved to {}/{}'.format(self.outputfig,
                                                                filename))
            index_plot(indx=self.index, save=self.savefig,
                       outputdir=self.outputfig, filename=filename,
                       loglevel=self.loglevel, step=step, title=title,
                       ylabel=ylabel, **kwargs)
        else:
            index_plot(indx=self.index, save=self.savefig,
                       loglevel=self.loglevel, step=step, title=title,
                       ylabel=ylabel, **kwargs)

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
            self.logger.info('Figures will be saved')
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
            self.logger.info('Data will be saved')
            self._load_folder_info(outputdir, 'data')

    def _filename(self, filename=None):
        """Generate the output file name.
        Args:
            filename (str): name of the output file.
                            Default is None.
        """
        if filename is None:
            self.logger.info('No filename specified, using the default name')
            filename = 'teleconnections_' + self.model + '_' + self.exp + '_' + self.source + '_' + self.telecname
        self.filename = filename
        self.logger.debug('Output filename: {}'.format(self.filename))

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
            self.logger.warning('No {} folder specified, using the current directory'.format(folder_type))
            folder = os.getcwd()
        else:
            if not isinstance(folder, str):
                raise TypeError('The folder must be a string')
            if not os.path.isdir(folder):
                self.logger.warning('The folder {} does not exist, creating it'.format(folder))
                os.makedirs(folder)

        # Store the folder in the class
        if folder_type == 'figure':
            self.outputfig = folder
            self.logger.debug('Figure output folder: {}'.format(self.outputfig))
        elif folder_type == 'data':
            self.outputdir = folder
            self.logger.debug('Data output folder: {}'.format(self.outputdir))

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
            self.logger.debug('Variable: {}'.format(var))

        if data is None and var == self.var:  # Use the teleconnection data
            self.logger.debug('No data specified, using teleconnection data')
            if self.data is None:
                self.logger.warning('No data has been loaded, trying to retrieve it')
                self.retrieve()  # this will load the data in self.data
            data = self.data
            data = data[var]

            return data, dim

        if var != self.var:
            self.logger.debug('Variable {} is different from teleconnection variable {}'.format(var, self.var))
            self.logger.warning("The result won't be saved as teleconnection attribute")

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
