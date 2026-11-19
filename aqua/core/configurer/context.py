"""Configuration path helpers for AQUA.

This module resolves the configuration directory/file, the machine name and the
active project. It knows nothing about catalogs - that responsibility lives entirely in
`config_catalog.ConfigCatalog`, since "catalog" is an intake concept in AQUA.

Example:
    >>> from aqua.core.configurer import ConfigContext
    >>> context = ConfigContext()
    >>> context.project  # e.g. "diagnostics", or None for an installation without project
    >>> context.get_project_folder("analysis")  # ~/.aqua/analysis/<project>
"""

import os
from functools import cached_property

from jinja2 import Template

from aqua.core.logger import log_configure
from aqua.core.util.yaml import load_yaml

from .locator import ConfigLocator
from .packages import ConfigPackages

PROJECT_KEY = "project"
"""Key of ``config-aqua.yaml`` holding the active project."""
PROJECT_ENV = "AQUA_PROJECT"
"""Environment variable overriding the active project."""
PROJECT_FOLDERS = ("analysis", "collections", "tools")
"""Configuration folders with one subfolder per project (catgen is not project-scoped)."""
PRE_PROJECT_MESSAGE = (
    f"No {PROJECT_KEY} set in {{config_file}}: this installation predates project support, run `aqua install` to upgrade it"
)
"""Error for an installation without the project key, formatted with its ``config_file``."""


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

    @cached_property
    def project(self) -> str | None:
        """
        Name of the active project. Cached automatically on first access.

        The ``project`` key of the configuration file decides whether the installation has the
        project layout: without it (installation predating project support, refused by
        ``get_project_folder``) there is no project, whatever ``AQUA_PROJECT`` says. With it,
        the environment variable ``AQUA_PROJECT`` overrides the name given by the key.

        Returns:
            str | None: Active project name, or None for an installation without project.
        """
        if not self.has_project_layout():
            self.logger.debug("No %s key in %s", PROJECT_KEY, self.config_file)
            return None

        project = os.environ.get(PROJECT_ENV)
        if project:
            self.logger.debug("Project set to %s by the %s environment variable", project, PROJECT_ENV)
            return project

        project = self.config_dict.get(PROJECT_KEY)
        self.logger.debug("Project found in configuration file, set to %s", project)
        return project or None

    def has_project_layout(self) -> bool:
        """
        Whether the configuration file has the ``project`` key, i.e. the installation has per-project folders.

        The key may be empty (no active project) once every project has been removed.

        Returns:
            bool: False for an installation that predates project support.
        """
        return PROJECT_KEY in self.config_dict

    def get_project_folder(self, name: str) -> str:
        """
        Return the path of a project-scoped configuration folder.

        The path is ``<configdir>/<name>/<project>``. The flat ``<configdir>/<name>`` layout
        of installations predating project support is not supported, and there is no
        fallback from one project to another.

        Args:
            name (str): Folder name, one of ``analysis``, ``collections``, ``tools``.

        Returns:
            str: Path of the folder.

        Raises:
            ValueError: If ``name`` is not a project-scoped folder.
            FileNotFoundError: If no project is set or the folder does not exist.
        """
        if name not in PROJECT_FOLDERS:
            raise ValueError(f"'{name}' is not a project folder, choose among {', '.join(PROJECT_FOLDERS)}")

        if self.project is None:
            raise FileNotFoundError(PRE_PROJECT_MESSAGE.format(config_file=self.config_file))

        folder = os.path.join(self.configdir, name, self.project)
        if not os.path.isdir(folder):
            raise FileNotFoundError(f"Cannot find the {name} folder for project '{self.project}' in {folder}")
        return folder

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
