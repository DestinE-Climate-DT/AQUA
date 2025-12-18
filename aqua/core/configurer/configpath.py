"""Catalog configuration helpers for AQUA."""
import os
#import platform
import intake

from aqua.core.util.util import to_list
from aqua.core.util.yaml import load_yaml
from aqua.core.logger import log_configure
from .locator import ConfigLocator


class ConfigPath():
    """
    A class to manage the configuration path and directory robustly, including
    handling and browsing across multiple catalogs.
    """

    def __init__(self, configdir=None, filename='config-aqua.yaml',
                 catalog=None, loglevel='warning', locator=None):
        """
        Initialize the ConfigPath instance.
        Args:
            configdir (str | None): The directory where the configuration file is located.
                                        If None, it is determined by the `get_config_dir` method.
            filename (str): The name of the configuration file. Defaults to 'config-aqua.yaml'.
            catalog (str | list | None): Specific catalog(s) to use. If None,
                                        all available catalogs are considered.
            loglevel (str): The logging level. Defaults to 'warning'.
            locator (ConfigLocator | None): An optional ConfigLocator instance.
        """

        # set up logger
        self.logger = log_configure(log_level=loglevel, log_name='ConfigPath')

        # get the configuration directory and its file
        self.filename = filename
        if locator is None:
            locator = ConfigLocator(filename=filename, configdir=configdir, logger=self.logger)
        self.locator = locator
        self.configdir = self.locator.configdir
        self.config_file = self.locator.config_file
        self.logger.debug('Configuration file found in %s', self.config_file)
        self.config_dict = load_yaml(self.config_file)

        # if no catalog are provided, get all available
        if catalog is None:
            catalog = self.get_catalog()
        self.catalog_available = to_list(catalog)
        self.logger.debug('Available catalogs are %s', self.catalog_available)

        # set the catalog as the first available and get all configurations
        if not self.catalog_available:
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

        # Initialize data model attribute
        self.data_model = None

    def get_config_dir(self):
        """
        Return the path to the configuration directory.

        Notes:
            This method delegates to `ConfigLocator` and is kept for backward
            compatibility.
        """
        return self.locator.configdir

    def get_catalog(self):
        """
        Extract the name of the catalog from the configuration file

        Returns:
            list[str] | None: the catalog names from the main config file or
            None when the `catalog` entry is present but empty.
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

        if not all(v is not None for v in [model, exp, source]):
            raise KeyError('Need to defined the triplet model, exp and source')

        for catalog in self.catalog_available:
            self.logger.debug('Browsing catalog %s ...', catalog)
            catalog_file, _ = self.get_catalog_filenames(catalog)
            cat = intake.open_catalog(catalog_file)
            check, level, avail = self.scan_catalog(cat, model=model, exp=exp, source=source)
            if check:
                self.logger.info('%s_%s_%s triplet found in in %s!', model, exp, source, catalog)
                success.append(catalog)
            else:
                fail[catalog] = (f'In catalog {catalog} when looking for {model}_{exp}_{source} '
                                 f'triplet I could not find the {level}. Available alternatives are {avail}')
        return success, fail

    def deliver_intake_catalog(self, model, exp, source, catalog=None):
        """
        Given a triplet of model-exp-source (and possibly a specific catalog), browse the catalog
        and check if the triplet can be found

        Returns:
            intake.catalog.Catalog: The intake catalog
            str: The path to the catalog file
            str: The path to the machine file
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
        Extract the information related to the machine from the catalog-dependent machine file

        Returns:
            machine_paths (dict): the machine_paths filesystem locations
            intake_vars (dict): the intake catalog variables
        """
        # loading the grid defintion file
        machine_file = load_yaml(self.machine_file)
        machine_paths = {}

        # get information on paths
        if self.machine in machine_file:
            machine_paths = machine_file[self.machine]
        else:
            if 'default' in machine_file:
                machine_paths = machine_file['default']

        # The main config file has priority
        if 'paths' in self.config_dict:
            for path in ['areas', 'weights', 'grids']:
                if path in self.config_dict['paths']:
                    if 'paths' not in machine_paths:
                        machine_paths['paths'] = {}
                    machine_paths['paths'][path] = self.config_dict['paths'][path]
        else:
            self.logger.debug('No paths found in the main configuration file %s', self.base_available)
        if machine_paths == {}:
            self.logger.error('Cannot find machine paths for %s, regridding and areas feature will not work', self.machine)

        # extract potential intake variables
        intake_vars = machine_paths.get('intake', {})
        return machine_paths, intake_vars

    def get_base(self):
        """
        Get all the possible base configurations available

        Returns:
            dict[str, dict]: map of catalog name to rendered configuration.
        """
        if os.path.exists(self.config_file):
            base = {}
            for catalog in self.catalog_available:
                definitions = {'catalog': catalog, 'configdir': self.configdir}
                base[catalog] = load_yaml(infile=self.config_file, definitions=definitions, jinja=True)
            return base
        raise FileNotFoundError(f'Cannot find the basic configuration file {self.config_file}!')

    def get_machine(self):
        """
        Extract the name of the machine from the configuration file

        Returns:
            str | None: resolved machine name from the configuration file, or None when detection fails.
        """
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f'Cannot find the basic configuration file {self.config_file}!')

        base = load_yaml(self.config_file)
        # if we do not know the machine we assume is "unknown"
        machine = 'unknown'
        # if the configuration file has a machine entry, use it
        if 'machine' in base:
            self.logger.debug('Machine found in configuration file, set to %s', machine)
            return base['machine']

        # warning for unknown machine
        self.logger.warning('No machine entry found in configuration file, set to %s', machine)
        return machine

        # if the entry is auto, or the machine unknown, try autodetection
        # if self.machine in ['auto', 'unknown']:
        #     self.logger.debug('Machine is %s, trying to self detect', self.machine)
        #     self.machine = self._auto_detect_machine()

    # def _auto_detect_machine(self):
    #     """Tentative method to identify the machine from the hostname"""

    #     platform_name = platform.node()

    #     if os.getenv('GITHUB_ACTIONS'):
    #         self.logger.debug('GitHub machine identified!')
    #         return 'github'

    #     platform_dict = {
    #         'uan': 'lumi',
    #         'levante': 'levante',
    #     }

    #     # Search for the dictionary key in the key_string
    #     for key, value in platform_dict.items():
    #         if key in platform_name:
    #             self.logger.debug('%s machine identified!', value)
    #             return value

    #     self.logger.debug('No machine identified, still unknown and set to None!')
    #     return None

    def get_catalog_filenames(self, catalog=None):
        """
        Extract the catalog and machine file paths for the selected catalog.

        Args:
            catalog (str | None): override catalog to inspect; defaults to the
                current `self.catalog`.

        Returns:
            catalog_file (str): the path to the catalog file
            machine_file (str): the path to the machine file
        """
        if self.catalog is None:
            raise KeyError('No AQUA catalog is installed. Please run "aqua add CATALOG_NAME"')

        if catalog is None:
            catalog = self.catalog

        catalog_file = self.base_available[catalog]['reader']['catalog']
        self.logger.debug('Catalog file is %s', catalog_file)
        if not os.path.exists(catalog_file):
            raise FileNotFoundError(f'Cannot find catalog file in {catalog_file}. Did you install it with "aqua add {catalog}"?')

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

    def scan_catalog(self, cat, model=None, exp=None, source=None):
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

    def show_catalog_content(self, catalog=None, model=None, exp=None, source=None, verbose=True):
        """
        Scan catalog(s) by reading YAML files directly and display the model/exp/source structure.
        Uses intake to handle path resolution automatically.

        Args:
            catalog (str | list | None): Specific catalog(s) to scan. If None, loops over all available catalogs.
            model (str | None): Optional model filter.
            exp (str | None): Optional experiment filter.
            source (str | None): Optional source filter.
            verbose (bool): If True, prints the formatted catalog structure. Defaults to True.

        Returns:
            dict: Dictionary with catalog names as keys and nested dict structure as values.
        """

        self.logger = log_configure(log_level='info', log_name='ShowCatalog')

        results = {}
        catalogs_to_scan = to_list(catalog) if catalog else self.catalog_available

        if not catalogs_to_scan:
            self.logger.warning('No catalogs available to scan')
            return results

        self.logger.debug("Catalogs to show: %s", catalogs_to_scan)

        for cat_name in catalogs_to_scan:
            # Open catalog (skip if fails)
            try:
                catalog_file, _ = self.get_catalog_filenames(catalog=cat_name)
            except (KeyError, FileNotFoundError) as e:
                self.logger.warning('Cannot scan catalog: %s', e)
                continue

            # Use intake to open the catalog
            try:
                cat = intake.open_catalog(catalog_file)
            except (KeyError, FileNotFoundError, Exception) as e:
                self.logger.warning('Cannot open catalog %s: %s', cat_name, e)
                continue

            catalog_structure = {}
            models = [model] if model else list(cat.keys())

            for model_name in models:
                if model_name not in cat:
                    self.logger.warning('Model %s not found in catalog %s', model_name, cat_name)
                    self.logger.warning('Available models are: %s', list(cat.keys()))
                    continue

                model_cat = cat[model_name]
                experiments = [exp] if exp else list(model_cat.keys())

                for exp_name in experiments:
                    if exp_name not in model_cat:
                        self.logger.warning('Experiment %s not found in model %s', exp_name, model_name)
                        self.logger.warning('Available experiments are: %s', list(model_cat.keys()))
                        continue

                    exp_cat = model_cat[exp_name]
                    sources = list(exp_cat.keys())

                    # Apply source filter if provided
                    if source:
                        sources = [s for s in sources if s == source]

                    if sources:
                        if model_name not in catalog_structure:
                            catalog_structure[model_name] = {}
                        catalog_structure[model_name][exp_name] = sources

            if catalog_structure:
                results[cat_name] = catalog_structure
                formatted_output = self.format_catalog_structure(catalog_structure, cat_name)
                if verbose:
                    print(formatted_output)

        return results

    @staticmethod
    def format_catalog_structure(structure, catalog_name):
        """Format catalog structure as a nicely aligned tree."""
        lines = [f"\n{'='*80}", f"📁 Catalog: {catalog_name}", f"{'='*80}"]

        for model_name, experiments in sorted(structure.items()):
            lines.append(f"\n   Model: {model_name}")

            for exp_name, sources in sorted(experiments.items()):
                lines.append(f"     └─ Experiment: {exp_name}")

                # Format sources in columns for better readability
                if sources:
                    sorted_sources = sorted(sources)
                    # Group sources in rows of 3 for compact display
                    for i in range(0, len(sorted_sources), 3):
                        source_group = sorted_sources[i:i+3]
                        formatted_sources = "  ".join(f"{s:<25}" for s in source_group)
                        lines.append(f"        ├─ {formatted_sources}")

        lines.append(f"{'='*80}\n")
        return "\n".join(lines)

    def get_data_model(self, data_model: str = 'aqua'):
        """
        Extract the data model information from the data_model folder.
        Populate the `self.data_model` attribute with the loaded data model dictionary.

        Args:
            data_model (str): the data model to be used. Defaults to 'aqua'.
        """
        data_model_folder = os.path.join(self.configdir, 'data_model')
        filename = f'{data_model}.yaml'

        if os.path.exists(data_model_folder):
            data_model_dict = load_yaml(os.path.join(data_model_folder, filename))
            self.logger.debug(f'Found data model {data_model} version {data_model_dict.get("version", "unknown")} in {data_model_folder}')
            self.data_model = data_model_dict
        else:
            raise FileNotFoundError(f'Cannot find the data model {data_model} in {data_model_folder}!')

    def get_coordinate_name(self, coordinate: str):
        """
        Given a coordinate name, return its definition from the data model.

        Args:
            coordinate (str): the coordinate name to be retrieved.

        Returns:
            str: the coordinate definition from the data model.
        """
        if self.data_model is None:
            self.get_data_model()

        print(self.data_model['data_model'])
        if coordinate not in self.data_model['data_model']:
            raise KeyError(f'No coordinate {coordinate} found in the data model!')
        else:
            return self.data_model['data_model'][coordinate]['name']
