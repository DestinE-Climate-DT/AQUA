"""Configuration path helpers for AQUA.

This module resolves configuration directories, catalog file locations,
and machine information from the AQUA configuration file(s). It has no
knowledge of intake catalogs themselves - it only deals with paths and
yaml-based configuration. For actually opening/browsing intake catalogs,
see `catalog_browser.CatalogBrowser`.
"""

import os

from aqua.core.logger import log_configure
from aqua.core.util.util import to_list
from aqua.core.util.yaml import load_yaml

from .locator import ConfigLocator


class ConfigPaths:
    """
    A class to manage the configuration path and directory robustly, including
    handling and browsing across multiple catalogs.

    This class deals exclusively with paths and yaml-derived configuration.
    It does not import or use `intake` - that responsibility lives in
    `CatalogBrowser`, which is typically built on top of a `ConfigPaths`
    instance.
    """

    def __init__(self, configdir=None, filename="config-aqua.yaml", catalog=None, loglevel="warning", locator=None):
        """
        Initialize the ConfigPaths instance.

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
        self.logger = log_configure(log_level=loglevel, log_name="ConfigPaths")

        # get the configuration directory and its file
        self.filename = filename
        if locator is None:
            locator = ConfigLocator(filename=filename, configdir=configdir, logger=self.logger)
        self.locator = locator
        self.configdir = self.locator.configdir
        self.config_file = self.locator.config_file
        self.logger.debug("Configuration file found in %s", self.config_file)
        self.config_dict = load_yaml(self.config_file)

        # if no catalog are provided, get all available
        if catalog is None:
            catalog = self.get_catalog()
        self.catalog_available = to_list(catalog)
        self.logger.debug("Available catalogs are %s", self.catalog_available)

        # set the catalog as the first available and get all configurations
        if not self.catalog_available:
            self.logger.warning("No available catalogs found")
            self.catalog = None
            self.base_available = None
            self.catalog_file = None
            self.machine_file = None
        else:
            self.catalog = self.catalog_available[0]
            self.base_available = self.get_base()
            self.logger.debug("Default catalog will be %s", self.catalog)
            self.catalog_file, self.machine_file = self.get_catalog_filenames(self.catalog)

        # get also info on machine on init
        self.machine = self.get_machine()

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
            if "catalog" not in base:
                raise KeyError(f"Cannot find catalog information in {self.config_file}")

            # particular case of an empty list
            if not base["catalog"]:
                return None

            self.logger.debug("Catalog found in %s file are %s", self.config_file, base["catalog"])
            return base["catalog"]

        raise FileNotFoundError(f"Cannot find the basic configuration file {self.config_file}!")

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
            if "default" in machine_file:
                machine_paths = machine_file["default"]

        # The main config file has priority
        if "paths" in self.config_dict:
            for path in ["areas", "weights", "grids"]:
                if path in self.config_dict["paths"]:
                    if "paths" not in machine_paths:
                        machine_paths["paths"] = {}
                    machine_paths["paths"][path] = self.config_dict["paths"][path]
        else:
            self.logger.debug("No paths found in the main configuration file %s", self.base_available)
        if machine_paths == {}:
            self.logger.error("Cannot find machine paths for %s, regridding and areas feature will not work", self.machine)

        # extract potential intake variables
        intake_vars = machine_paths.get("intake", {})
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
                definitions = {"catalog": catalog, "configdir": self.configdir}
                base[catalog] = load_yaml(infile=self.config_file, definitions=definitions, jinja=True)
            return base
        raise FileNotFoundError(f"Cannot find the basic configuration file {self.config_file}!")

    def get_machine(self):
        """
        Extract the name of the machine from the configuration file

        Returns:
            str | None: resolved machine name from the configuration file, or None when detection fails.
        """
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Cannot find the basic configuration file {self.config_file}!")

        base = load_yaml(self.config_file)
        # if we do not know the machine we assume is "unknown"
        machine = "unknown"
        # if the configuration file has a machine entry, use it
        if "machine" in base:
            self.logger.debug("Machine found in configuration file, set to %s", machine)
            return base["machine"]

        # warning for unknown machine
        self.logger.warning("No machine entry found in configuration file, set to %s", machine)
        return machine

        # if the entry is auto, or the machine unknown, try autodetection
        # if self.machine in ['auto', 'unknown']:
        #     self.logger.debug('Machine is %s, trying to self detect', self.machine)
        #     self.machine = self._auto_detect_machine()

    # def _auto_detect_machine(self):
    #     """Tentative method to identify the machine from the hostname"""
    #
    #     platform_name = platform.node()
    #
    #     if os.getenv('GITHUB_ACTIONS'):
    #         self.logger.debug('GitHub machine identified!')
    #         return 'github'
    #
    #     platform_dict = {
    #         'uan': 'lumi',
    #         'levante': 'levante',
    #     }
    #
    #     # Search for the dictionary key in the key_string
    #     for key, value in platform_dict.items():
    #         if key in platform_name:
    #             self.logger.debug('%s machine identified!', value)
    #             return value
    #
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

        catalog_file = self.base_available[catalog]["reader"]["catalog"]
        self.logger.debug("Catalog file is %s", catalog_file)
        if not os.path.exists(catalog_file):
            raise FileNotFoundError(
                f'Cannot find catalog file in {catalog_file}. Did you install it with "aqua add {catalog}"?'
            )

        machine_file = self.base_available[catalog]["reader"]["machine"]
        self.logger.debug("Machine file is %s", machine_file)
        if not os.path.exists(machine_file):
            raise FileNotFoundError(f"Cannot find machine file for {catalog} in {machine_file}")

        return catalog_file, machine_file

    def get_reader_filenames(self, catalog=None):
        """
        Extract the filenames for the reader for catalog, regrid and fixer

        Returns:
            Three strings for the path of the fixer, regrid and config files
        """
        if catalog is None:
            catalog = self.catalog

        fixer_folder = self.base_available[catalog]["reader"]["fixer"]
        if not os.path.exists(fixer_folder):
            raise FileNotFoundError(f"Cannot find the fixer folder in {fixer_folder}")
        grids_folder = self.base_available[catalog]["reader"]["regrid"]
        if not os.path.exists(grids_folder):
            raise FileNotFoundError(f"Cannot find the regrid folder in {grids_folder}")

        return fixer_folder, grids_folder
