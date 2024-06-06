"""Utility functions for getting the configuration files"""
import os
from .yaml import load_yaml


catname = 'catalog'

class ConfigPath():

    """
    Class to set the configuration path and dir in a robust way
    """

    def __init__(self, configdir=None, filename='config-aqua.yaml', catalog=None):

        self.filename = filename
        if not configdir:
            self.configdir = self.get_config_dir()
        else:
            self.configdir = configdir
        self.config_file = os.path.join(self.configdir, self.filename)
        if not catalog:
            self.catalog = self.get_catalog()
        else:
            self.catalog = catalog

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

        # Finally for developers if AQUA is defined
        aquadir = os.environ.get('AQUA')
        if aquadir:
            configdirs.append(os.path.join(aquadir, 'config'))  

        # Autosearch for the config folder
        for configdir in configdirs:
            if os.path.exists(os.path.join(configdir, self.filename)):
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
            try:
                return base[catname]
            except KeyError as exc:
                raise KeyError(f'Cannot find {catname} information in {self.config_file}') from exc
        else:
            raise FileNotFoundError(f'Cannot find the basic configuration file {self.config_file}!')

    def get_reader_filenames(self):
        """
        Extract the filenames for the reader for catalog, regrid and fixer

        Returns:
            Four strings for the path of the catalog, fixer, regrid and config files
        """

        # Build the template dictionary
        definitions = {catname: self.catalog, 'configdir': self.configdir}

        if os.path.exists(self.config_file):
            base = load_yaml(infile=self.config_file, definitions=definitions, jinja=True)
            catalog_file = base['reader']['catalog']
            if not os.path.exists(catalog_file):
                raise FileNotFoundError(f'Cannot find catalog file in {catalog_file}')
            fixer_folder = base['reader']['fixer']
            if not os.path.exists(fixer_folder):
                raise FileNotFoundError(f'Cannot find the fixer folder in {fixer_folder}')
            grids_folder = base['reader']['regrid']
            if not os.path.exists(grids_folder):
                raise FileNotFoundError(f'Cannot find the regrid folder in {grids_folder}')
        else:
            raise FileNotFoundError(f'Cannot find the basic configuration file {self.config_file}!')

        return catalog_file, fixer_folder, grids_folder, self.config_file
