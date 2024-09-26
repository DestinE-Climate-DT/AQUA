"""Utility functions for getting the configuration files"""
import os
import platform
import intake
from aqua.logger import log_configure
from .yaml import load_yaml
from .util import to_list


class ConfigPath():
    """
    A class to manage the configuration path and directory robustly, including
    handling and browsing across multiple catalogs.

    Attributes:
        filename (str): The name of the configuration file. Defaults to 'config-aqua.yaml'.
        configdir (str): The directory where the configuration file is located.
                         If not provided, it is determined by the `get_config_dir` method.
        catalog (str): The first catalog from the list of available catalogs, or None if
                       no catalogs are available.
        loglevel (str): The logging level. Default is 'WARNING'.
    """

    def __init__(self, configdir=None, filename='config-aqua.yaml',
                 catalog=None, loglevel='WARNING'):

        # set up logger
        self.logger = log_configure(log_level=loglevel, log_name='ConfigPath')

        # get the configuration directory and its file
        self.filename = filename
        if configdir is None:
            self.configdir = self.get_config_dir()
        else:
            self.configdir = configdir
        self.config_file = os.path.join(self.configdir, self.filename)
        self.logger.debug('Configuration file found in %s', self.config_file)

        # get all the available installed catalogs
        if catalog is None:
            self.catalog_available = to_list(self.get_catalog())
        else:
            self.catalog_available = to_list(catalog)
        self.logger.debug('Available catalogs are %s', self.catalog_available)

        # set the catalog as the first available and get all configurations
        if self.catalog_available is None:
            self.logger.warning('No available catalogs found')
            self.catalog = None
            self.base_available = None
            self.catalog_file = None
            self.machine_file = None
        else:
            self.catalog = self.catalog_available[0]
            self.base_available = self.get_base()
            self.logger.debug('Default catalog will be %s', self.catalog)
            self.catalog_file, self.machine_file = self.get_catalog_filenames(self.catalog)

        # get also info on machine on init
        self.machine = self.get_machine()

    def get_config_dir(self):
        """
        Return the path to the configuration directory,
        searching in a list of pre-defined directories.

        Generalized to work for config files with different names.

        Returns:
            configdir (str): the dir of the catalog file and other config files

        Raises:
            FileNotFoundError: if no config file is found in the predefined folders
        """

        configdirs = []

        # Check first if AQUA_CONFIG is defined
        aquaconfigdir = os.environ.get('AQUA_CONFIG')
        if aquaconfigdir:
            configdirs.append(aquaconfigdir)

        # Then if the home is defined
        homedir = os.environ.get('HOME')
        if homedir:
            configdirs.append(os.path.join(homedir, '.aqua'))

        # Autosearch for the config folder
        for configdir in configdirs:
            if os.path.exists(os.path.join(configdir, self.filename)):
                self.logger.debug('AQUA installation found in %s', configdir)
                return configdir

        raise FileNotFoundError(f"No config file {self.filename} found in {configdirs}")

    def get_catalog(self):
        """
        Extract the name of the catalog from the configuration file

        Returns:
            The name of the catalog read from the configuration file
        """

        if os.path.exists(self.config_file):
            base = load_yaml(self.config_file)
            if 'catalog' not in base:
                raise KeyError(f'Cannot find catalog information in {self.config_file}')

            # particular case of an empty list
            if not base['catalog']:
                return None

            self.logger.debug('Catalog found in %s file are %s', self.config_file, base['catalog'])
            return base['catalog']

        raise FileNotFoundError(f'Cannot find the basic configuration file {self.config_file}!')

    def browse_catalogs(self, model: str, exp: str, source: str):
        """
        Given a triplet of model-exp-source, browse all catalog installed catalogs

        Returns
            a list of catalogs where the triplet is found
            a dictionary with information on wrong triplet
        """

        success = []
        fail = {}

        if self.catalog_available is None:
            return success, fail

        if all(v is not None for v in [model, exp, source]):

            for catalog in self.catalog_available:
                self.logger.debug('Browsing catalog %s ...', catalog)
                catalog_file, _ = self.get_catalog_filenames(catalog)
                cat = intake.open_catalog(catalog_file)
                check, level, avail = scan_catalog(cat, model=model,
                                                   exp=exp, source=source)
                if check is True:
                    self.logger.info('%s_%s_%s triplet found in in %s!', model, exp, source, catalog)
                    success.append(catalog)
                else:
                    fail[catalog] = f'In catalog {catalog} when looking for {model}_{exp}_{source} triplet I could not find the {level}. Available alternatives are {avail}' # noqa
            return success, fail

        raise KeyError('Need to defined the triplet model, exp and source')

    def deliver_intake_catalog(self, model, exp, source, catalog=None):
        """
        Given a triplet of model-exp-source (and possibly a specific catalog), browse the catalog
        and check if the triplet can be found

        Returns:
          The intake catalog and the associated catalog and machine file

        """
        matched, failed = self.browse_catalogs(model=model, exp=exp, source=source)
        if not matched:
            for _, value in failed.items():
                self.logger.error(value)
            raise KeyError('Cannot find the triplet in any catalog. Check logger error for hints on possible typos')

        if catalog is not None:
            self.catalog = catalog
        else:
            if len(matched) > 1:
                self.logger.warning('Multiple triplets found in %s, setting %s as the default', matched, matched[0])
            self.catalog = matched[0]

        self.logger.debug('Final catalog to be used is %s', self.catalog)
        self.catalog_file, self.machine_file = self.get_catalog_filenames(self.catalog)
        return intake.open_catalog(self.catalog_file), self.catalog_file, self.machine_file

    def get_machine_info(self):
        """
        This extract the information related to the machine from the catalog-dependent machine file

        Returns:
            machine_paths: the dictionary with the paths
            intake_vars: the dictionary for the intake catalog variables
        """
        # loading the grid defintion file
        machine_file = load_yaml(self.machine_file)

        # get informtion on paths
        if self.machine in machine_file:
            machine_paths = machine_file[self.machine]
        else:
            if 'default' in machine_file:
                machine_paths = machine_file['default']
            else:
                raise KeyError(f'Cannot find machine paths for {self.machine}, regridding and areas feature will not work')

        # extract potential intake variables
        if 'intake' in machine_paths:
            intake_vars = machine_paths['intake']
        else:
            intake_vars = {}

        return machine_paths, intake_vars

    def get_base(self):
        """
        Get all the possible base configurations available
        """

        base = {}
        if os.path.exists(self.config_file):
            for catalog in self.catalog_available:
                definitions = {'catalog': catalog, 'configdir': self.configdir}
                base[catalog] = load_yaml(infile=self.config_file, definitions=definitions, jinja=True)
            return base

        raise FileNotFoundError(f'Cannot find the basic configuration file {self.config_file}!')

    def get_machine(self):
        """
        Extract the name of the machine from the configuration file

        Returns:
            The name of the machine read from the configuration file
        """

        if os.path.exists(self.config_file):
            base = load_yaml(self.config_file)
            # if we do not know the machine we assume is "unknown"
            self.machine = 'unknown'
            # if the configuration file has a machine entry, use it
            if 'machine' in base:
                self.machine = base['machine']
                self.logger.debug('Machine found in configuration file, set to %s', self.machine)
            # if the entry is auto, or the machine unknown, try autodetection
            if self.machine in ['auto', 'unknown']:
                self.logger.debug('Machine is %s, trying to self detect', self.machine)
                self.machine = self._auto_detect_machine()
            return self.machine

        raise FileNotFoundError(f'Cannot find the basic configuration file {self.config_file}!')

    def _auto_detect_machine(self):
        """Tentative method to identify the machine from the hostname"""

        platform_name = platform.node()

        if os.getenv('GITHUB_ACTIONS'):
            self.logger.debug('GitHub machine identified!')
            return 'github'

        platform_dict = {
            'uan': 'lumi',
            'levante': 'levante',
        }

        # Search for the dictionary key in the key_string
        for key, value in platform_dict.items():
            if key in platform_name:
                self.logger.debug('%s machine identified!', value)
                return value

        self.logger.debug('No machine identified, still unknown and set to None!')
        return None

    def get_catalog_filenames(self, catalog=None):
        """
        Extract the filenames

        Returns:
            Two strings for the path of the fixer, regrid and config files
        """

        if self.catalog is None:
            raise KeyError('No AQUA catalog is installed. Please run "aqua add CATALOG_NAME"')

        if catalog is None:
            catalog = self.catalog

        catalog_file = self.base_available[catalog]['reader']['catalog']
        self.logger.debug('Catalog file is %s', catalog_file)
        if not os.path.exists(catalog_file):
            raise FileNotFoundError(f'Cannot find catalog file in {catalog_file}. Did you install it with "aqua add {catalog}"?') # noqa
        machine_file = self.base_available[catalog]['reader']['machine']
        self.logger.debug('Machine file is %s', machine_file)
        if not os.path.exists(machine_file):
            raise FileNotFoundError(f'Cannot find machine file for {catalog} in {machine_file}')

        return catalog_file, machine_file

    def get_reader_filenames(self, catalog=None):
        """
        Extract the filenames for the reader for catalog, regrid and fixer

        Returns:
            Three strings for the path of the fixer, regrid and config files
        """
        if catalog is None:
            catalog = self.catalog

        fixer_folder = self.base_available[catalog]['reader']['fixer']
        if not os.path.exists(fixer_folder):
            raise FileNotFoundError(f'Cannot find the fixer folder in {fixer_folder}')
        grids_folder = self.base_available[catalog]['reader']['regrid']
        if not os.path.exists(grids_folder):
            raise FileNotFoundError(f'Cannot find the regrid folder in {grids_folder}')

        return fixer_folder, grids_folder


def scan_catalog(cat, model=None, exp=None, source=None):
    """
    Check if the model, experiment and source are in the catalog.

    Returns:
        status (bool): True if the triplet is found
        level (str): The level at which the triplet is failing
        info (str): The available catalog entries at the level of the triplet
    """

    status = False
    avail = None
    level = None
    if model in cat:
        if exp in cat[model]:
            if source in cat[model][exp]:
                status = True
            else:
                level = 'source'
                avail = list(cat[model][exp].keys())
        else:
            level = 'exp'
            avail = list(cat[model].keys())
    else:
        level = 'model'
        avail = list(cat.keys())

    return status, level, avail
