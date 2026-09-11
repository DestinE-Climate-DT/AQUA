"""Configuration path helpers for AQUA.

This module resolves the configuration directory/file and the machine name.
It knows nothing about catalogs - that responsibility lives entirely in
`config_catalog.ConfigCatalog`, since "catalog" is an intake concept in AQUA.
"""

import os
from functools import cached_property

from jinja2 import Template

from aqua.core.logger import log_configure
from aqua.core.util.yaml import load_yaml

from .locator import ConfigLocator
from .packages import ConfigPackages


class ConfigContext:
    """
    Resolves the AQUA configuration directory/file and the machine name.
    Provides access to configuration folders for AQUA utilities (e.g., reader, fixer, regrid).
    This class is intended to be used by all AQUA utilities that need to know the configuration context
    """

    def __init__(
        self,
        configdir: str | None = None,
        filename: str = "config-aqua.yaml",
        loglevel: str = "warning",
        locator: ConfigLocator | None = None,
        packages: ConfigPackages | None = None,
    ):
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
        self.locator = locator or ConfigLocator(filename=filename, configdir=configdir, logger=self.logger)
        self.packages = packages or ConfigPackages(logger=self.logger)
        self.configdir = self.locator.configdir
        self.config_file = self.locator.config_file
        self.logger.debug("Configuration file found in %s", self.config_file)
        self.config_dict = load_yaml(self.config_file)

    def get_config_dir(self) -> str:
        """
        Return the path to the configuration directory.
        """
        return self.configdir

    @cached_property
    def machine(self) -> str:
        """
        Extract the name of the machine from the configuration file.
        Cached automatically on first access.

        Returns:
            str: Resolved machine name, or "unknown" when detection fails.
        """
        machine_name = self.config_dict.get("machine")

        if machine_name:
            self.logger.debug("Machine found in configuration file, set to %s", machine_name)
            return machine_name

        self.logger.warning("No machine entry found in configuration file, set to unknown")
        return "unknown"

    def get_machine(self) -> str:
        """
        Convenience getter for machine name to maintain backwards compatibility.

        Returns:
            str: Resolved machine name or "unknown".
        """
        return self.machine

    def _get_reader_folder(self, folder_name: str) -> str:
        """
        Extract the filenames for the reader for regrid and fixer

        Args:
            folder_name (str): name of the folder to be extracted

        Returns:
            str: path of the folder
        """
        folder_path = self.config_dict["reader"].get(folder_name)
        if folder_path is None:
            raise KeyError(f"Folder name '{folder_name}' not found in reader configuration.")
        folder_path = Template(folder_path).render(configdir=self.configdir)
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Cannot find the {folder_name} folder in {folder_path}")
        return folder_path

    def get_reader_folders(self) -> tuple[str, str]:
        """
        Extract the filenames for the reader for regrid and fixer

        Returns:
            Two strings for the path of the fixer and regrid folders
        """

        return self._get_reader_folder("fixer"), self._get_reader_folder("regrid")

    def get_folder(self, name: str) -> str:
        """
        Extract the filenames for the configuration folders

        Args:
            name (str): name of the folder to be extracted

        Returns:
            str: path of the folder
        """
        config_folder = os.path.join(self.configdir, name)
        if not os.path.exists(config_folder):
            raise FileNotFoundError(f"Cannot find the {name} folder in {self.configdir}")
        return config_folder
