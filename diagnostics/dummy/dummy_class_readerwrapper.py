"""Dummy diagnostic for testing purposes"""
import os

from aqua.logger import log_configure
from aqua.util import load_yaml
from aqua.reader import Reader

# Please import only functions that are used in the class
# and not the entire module
from .dummy_func import dummy_func


class DummyDiagnosticWrapper():
    """Dummy diagnostic class

    This is a dummy diagnostic class for testing purposes.

    Methods (only public):
        retrieve: retrieve data from the data Reader class.
        fldmean: compute the field mean of the data.
        multiplication: multiply the data by a constant.
    """
    def __init__(self, model=None, exp=None, source=None,
                 custom_diagvar="I'm a custom variable",
                 diagconfigdir=None, regrid=None,
                 freq=None, var=None,
                 savefig=False, outputfig=None,
                 savefile=False, outputdir=None,
                 filename=None,
                 loglevel='WARNING'):
        """
        The DummyDiagnostic constructor.
        The init method is used to initialize the class.
        It is called when the class is instantiated.
        You can pass arguments to the class when you instantiate it.
        Please adapt the arguments to your needs.
        Take care of the default values. They should be the default that
        you want to use in your diagnostic.
        User should be able to run the default diagnostic setup having only
        to specify the model, experiment and source plus some directory
        or name specifications about where to store the outputs.
        Feel free to add more arguments if you need them.
        Feel freecas well to remove arguments if you do not need them,
        for instance if you do not need to regrid or to compute the time mean.
        Please keep the loglevel argument. It is used to configure the logger
        and it's preferable to have it in the init method rather that using
        print statements.

        Args:
            model (str):          the model name to be used by the Reader class.
                                  Default is None.
            exp (str):            the experiment name to be used by the Reader class.
                                  Default is None.
            source (str):         the source name to be used by the Reader class.
                                  Default is None.
            diagconfigdir (str, opt): the path to the directory containing the config files.
                                  Default is None.
                                  Please change it to the path of your config files,
                                  if you have any or if there is a clear config directory.
            regrid (str, opt):    whether to regrid the data or not.
                                  Default is None.
                                  Please change it to the default value that you want to use,
                                  considering that it will be the one used by the Reader class
                                  if the user does not specify it.
            freq (str, opt):      the frequency of the data temporal aggregation.
                                  Default is None.
                                  If None is passed, no temporal aggregation will be performed.
                                  Consider that time mean can be a very long operation.
                                  If you want to compute the time mean, please consider
                                  making use of the parallelization capabilities of xarray.
                                  You can find more examples in the lra or gribber framework code.
                                  Please change it to the default value that you want to use,
                                  considering that it will be the one used by the Reader class
                                  if the user does not specify it.
            var (str, opt):       the variable name to be used by the Reader class in the retrieve method.
                                  Default is None.
                                  Please change it to the default value that you want to use,
                                  considering that it will be the one used by the Reader class
                                  if the user does not specify it.
                                  It may be a list as well, since the Reader class can handle lists,
                                  in this example it is a string for simplicity.
            custom_diagvar (str): the variable name to be used by the Reader class in the compute method.
                                  Default is "I'm a custom variable".
                                  Please change it to the default value that you want to use,
                                  add more of them if your diagnostic needs more than one variable.
            savefig (bool, opt):  whether to save the figures or not.
                                  Default is False to not produce figures files in the notebooks.
                                  Consider that it may be suggested to have it True by default
                                  when running the diagnostic from the command line.
            outputfig (str, opt): the path to the output figure directory.
                                  Default is None. If None is passed, the figures will be saved
                                  in the current directory.
            savefile (bool, opt): whether to save the data or not.
                                  Default is False to not produce data files in the notebooks.
                                  Consider that it may be suggested to have it True by default
                                  when running the diagnostic from the command line.
            outputdir (str, opt): the path to the output directory.
                                  Default is None. If None is passed, the data will be saved
                                  in the current directory.
            filename (str, opt):  the custon name of the output file.
                                  Default is None.
                                  Please consider changing it to the default value that you want to use.
            loglevel (str):       the log level.
                                  Default is 'WARNING'.
                                  Please do not change it. It is used with the same default value
                                  in the other diagnostics and in the framework.
                                  Possible values are 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL',
                                  with decreasing order of verbosity and increasing importance.

        Returns:
            A DummyDiagnostic class instance.

        Raises:
            ValueError: if model, exp or source are not specified.
            ValueError: if var is not specified or it is not a string.
        """

        # Configure logger
        self.loglevel = loglevel  # Set the log level to the one passed to the class
        self.logger = log_configure(self.loglevel, 'Dummy')  # Please change the name of the logger to the name of your diagnostic

        # Reader variables
        # This is a block that initializes the variables that will be used by the Reader class.
        # Please adapt it to your needs.
        # Consider that this example is instantiating only one Reader class.
        # If you need to instantiate more than one Reader class, please adapt it to your needs.
        # You may instantiate more Reader classes here or you instead instantiate them
        # on multiple instances of the diagnostic class.
        # It will depend on your internal methods.
        if model:  # here model, exp, source are mandatory, check if it is the case for your diagnostic.
            self.model = model
        else:
            raise ValueError('model must be specified')

        if exp:
            self.exp = exp
        else:
            raise ValueError('exp must be specified')

        if source:
            self.source = source
        else:
            raise ValueError('source must be specified')

        self.regrid = regrid  # adapt or remove if you do not need it
        if self.regrid is None:
            self.logger.warning('No regridding will be performed')
        self.logger.info('Regridding resolution: %s', self.regrid)

        self.freq = freq  # adapt or remove if you do not need it
        if self.freq:
            self.logger.warning('Time mean will be computed')
            self.logger.info('Time mean frequency: %s', self.freq)
        # Consider that the Reader has more arguments that can be passed to it.
        # Please adapt the code to your needs.
        # You can find more information about the Reader class in the documentation.

        # Diagnostic variables
        # This is a block that initializes the variables that will be used by the diagnostic.
        # Please adapt it to your needs.
        self.var = var  # adapt or remove if you do not need it
        if self.var is None:
            raise ValueError('var must be specified')  # you may have (and probably should) a default different from None
        if not isinstance(self.var, str):  # it is a string in this example, but it may be a list as well
            raise ValueError('var must be a string')
        # If you want to retrieve all data consider removing the var argument from the class __init__.

        # Here we first load a configuration file and then we load the diagnostic variables.
        # If no variable is specified in the configuration file, the default value is used.
        self._load_config(diagconfigdir)  # adapt or remove if you do not need it, check the method
        self._load_diagvar(custom_diagvar)  # adapt or remove if you do not need it, check the method

        # Output variables
        # This is a block that initializes the variables that will be used to save the data.
        # Please adapt it to your needs.
        # Here we set two folders, one for the figures and one for the data.
        self._load_figs_options(savefig, outputfig)  # adapt or remove if you do not need it, check the method
        self._load_data_options(savefile, outputdir, filename)  # adapt or remove if you do not need it, check the method

        # Initialize the Reader class
        self._reader()  # self has not to be passed to the method as it is contained in the class

        self.fieldmean = None
        self.product = None
        self.outputdir = None
        self.outputfig = None
        self.data = None

        # Plese if some more initialization is needed for the diagnostic, do it in this functions.

    def _load_config(self, diagconfigdir=None):
        """Load the config file, if one is present.

        Args:
            diagconfigdir (str): path to the config directory.
                             Default is None.
        """

        self.diagconfigdir = diagconfigdir  # adapt or remove if you do not need it
        if self.diagconfigdir is None:
            self.logger.warning('No config directory specified')
        else:
            self.logger.info('Config directory: %s', self.diagconfigdir)

        # Here you can load the config files if you have any.
        try:
            configname = 'diagconfig.yaml'  # adapt or remove if you do not need it
            configpath = os.path.join(self.diagconfigdir, configname)  # adapt or remove if you do not need it
            self.config = load_yaml(configpath)  # customise the name of the config file if you need it
        except (FileNotFoundError, TypeError):
            self.logger.warning('Config file not found')
            self.logger.warning('Please be sure all the settings are passed as arguments')
            self.config = None

    def _load_diagvar(self, custom_diagvar):
        """Load the diagnostic variables from the config file, if one is present.

        Args:
            custom_diagvar (srt):  example of diagnostic variables.
                                   See init for the class default value.
        """
        try:
            self.logger.info('Loading diagnostic variables from config file')
            self.custom_diagvar = self.config['custom_diagvar']
        except (KeyError, TypeError):
            self.logger.warning('No diagnostic variables specified, using the default ones')
            self.custom_diagvar = custom_diagvar
        self.logger.debug('Diagnostic variables: %s', self.custom_diagvar)

    def _load_figs_options(self, savefig=False, outputfig=None):
        """Load the figure options.

        Args:
            savefig (bool): whether to save the figures.
                            Default is False.
            outputfig (str): path to the figure output directory.
                             Default is None.
                             See init for the class default value.
        """
        self.savefig = savefig  # adapt or remove if you do not need it

        if self.savefig:
            self.logger.info('Figures will be saved')
            self._load_folder_info(outputfig, 'figure')

    def _load_data_options(self, savefile=False, outputdir=None, filename=None):
        """Load the data options.

        Args:
            savefile (bool): whether to save the data.
                             Default is False.
            outputdir (str): path to the data output directory.
                             Default is None.
                             See init for the class default value.
            filename (str): name of the output file.
                            Default is None.
                            See init for the class default value.
        """
        self.savefile = savefile  # adapt or remove if you do not need it

        if self.savefile:
            self.logger.info('Data will be saved')
            self._load_folder_info(outputdir, 'data')
            if filename is None:
                self.logger.info('No filename specified, using the config file setting')
                try:
                    filename = self.config['filename']
                except (KeyError, TypeError):
                    self.logger.warning('No filename specified, using the default one')
                    filename = 'dummy.nc'
            self.logger.debug('Output filename: %s', filename)
            self.filename = filename

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
        if not folder:
            self.logger.info('No %s folder specified, trying to use the config file setting', folder_type)
            try:
                if folder_type == 'figure':
                    folder = self.config['outputfig']
                elif folder_type == 'data':
                    folder = self.config['outputdir']
                # Check if the path exists
                if not os.path.exists(folder):
                    self.logger.warning('%s folder does not exist, creating it', folder_type)
                    os.makedirs(folder)
            except (KeyError, TypeError):
                self.logger.warning('No %s folder specified, using the current directory', folder_type)
                folder = os.getcwd()

        # Store the folder in the class
        if folder_type == 'figure':
            self.outputfig = folder
            self.logger.debug('Figure output folder: %s', self.outputfig)
        elif folder_type == 'data':
            self.outputdir = folder
            self.logger.debug('Data output folder: %s', self.outputdir)

    def _reader(self):
        """The reader method.
        This method initializes the Reader class.
        """

        self.reader = Reader(model=self.model, exp=self.exp, source=self.source, regrid=self.regrid,
                             freq=self.freq, loglevel=self.loglevel)
        self.logger.debug('Reader class initialized')

    def retrieve(self):  # Notice that self should be contained in every method
        """The retrieve method.
        This method retrieves the data from the Reader class.
        It is an explicit method, so no "_" at the beginning.
        If your methods are not explicit, add "_" at the beginning of the method name.

        Raises:
            ValueError: if the variable is not found
        """

        try:
            self.data = self.reader.retrieve(var=self.var)
            if self.freq:
                self.logger.debug('Temporal aggregation of data')
                self.data = self.reader.timmean(self.data)
            if self.regrid:
                self.logger.debug('Regridding data')
                self.data = self.reader.regrid(self.data)
        except ValueError:
            try:
                self.logger.warning('Variable %s not found', self.var)
                self.logger.warning('Trying to retrieve without fixing')
                self.data = self.reader.retrieve(var=self.var, fix=False)
            except ValueError:
                raise ValueError(f'Variable {self.var} not found')
        self.logger.info('Data retrieved')

    def fldmean(self):
        """The fldmean method.
        This method computes the field mean of the retrieved data.
        It is an example of method that does not use of external functions of the module

        Raises:
            TypeError: if the variable is not a string
        """
        self.logger.info('Field mean computed')
        self.fieldmean = self.reader.fldmean(self.data)

    def multiplication(self):
        """The multiplication method.
        This method computes the some product by a constant of the retrieved data.
        It is an example of method that uses of external functions of the module
        """

        self.logger.info('Multiplication computed')
        self.product = dummy_func(self.data)  # dummy_func is a function defined in the module

    def _private_method(self):
        """The private method.
        This method is a private one, so it has "_" at the beginning.
        If your methods are explicit, remove "_" at the beginning of the method name.
        """
        self.logger.debug('This is a private method, should not be accessed from outside the class')
        self.logger.debug('Congratulations, you found it!')

# Notice that more methods can be added to the class, and they can be explicit or not.
# Consider adding or not plot methods, depending on your needs.
