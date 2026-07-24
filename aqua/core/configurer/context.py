"""Configuration path helpers for AQUA.

This module resolves the configuration directory/file and the machine name.
It knows nothing about catalogs - that responsibility lives entirely in
`config_catalog.ConfigCatalog`, since "catalog" is an intake concept in AQUA.
"""

import os

from jinja2 import Template

from aqua.core.logger import log_configure
from aqua.core.util.yaml import load_yaml

from .locator import ConfigLocator


class ConfigContext:
    """
    Resolves the AQUA configuration directory/file and the machine name.
    Has no knowledge of catalogs or intake.
    """

    def __init__(self, configdir: str | None, filename: str = "config-aqua.yaml", loglevel: str = "warning", locator=None):
        """
        Initialize the ConfigContext instance.

        Args:
            configdir (str | None): The directory where the configuration file is located.
                                        If None, it is determined by the `get_config_dir` method.
            filename (str): The name of the configuration file. Defaults to 'config-aqua.yaml'.
            loglevel (str): The logging level. Defaults to 'warning'.
            locator (ConfigLocator | None): An optional ConfigLocator instance.
        """

        # set up logger
        self.logger = log_configure(log_level=loglevel, log_name="ConfigContext")

        # get the configuration directory and its file
        self.filename = filename
        if locator is None:
            locator = ConfigLocator(filename=filename, configdir=configdir, logger=self.logger)
        self.locator = locator
        self.configdir = self.locator.configdir
        self.config_file = self.locator.config_file
        self.logger.debug("Configuration file found in %s", self.config_file)
        self.config_dict = load_yaml(self.config_file)

        # get info on machine on init
        self.machine = self.get_machine()

    def get_config_dir(self):
        """
        Return the path to the configuration directory.

        Notes:
            This method delegates to `ConfigLocator` and is kept for backward
            compatibility.
        """
        return self.locator.configdir

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

    def get_reader_folders(self):
        """
        Extract the filenames for the reader for regrid and fixer

        Returns:
            Two strings for the path of the fixer and regrid folders
        """

        fixer_folder = self.config_dict["reader"]["fixer"]
        fixer_folder = Template(fixer_folder).render(configdir=self.configdir)
        if not os.path.exists(fixer_folder):
            raise FileNotFoundError(f"Cannot find the fixer folder in {fixer_folder}")
        grids_folder = self.config_dict["reader"]["regrid"]
        grids_folder = Template(grids_folder).render(configdir=self.configdir)
        if not os.path.exists(grids_folder):
            raise FileNotFoundError(f"Cannot find the regrid folder in {grids_folder}")

        return fixer_folder, grids_folder

    def get_folder(self, name: str):
        """
        Extract the filenames for the configuration folders

        Args:
            folder_name (str): name of the folder to be extracted
        """
        config_folder = os.path.join(self.configdir, name)
        if not os.path.exists(config_folder):
            raise FileNotFoundError(f"Cannot find the {name} folder in {self.configdir}")
        return config_folder
