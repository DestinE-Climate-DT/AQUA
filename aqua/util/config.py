"""Utility functions for getting the configuration files"""
import os
from .yaml import load_yaml


class ConfigPath():

    """
    Class to set the configuration path and dir in a robust way
    """

    def __init__(self, configdir=None, filename='config-aqua.yaml', machine=None):

        self.filename = filename
        if not configdir:
            self.configdir = self.get_config_dir()
        else:
            self.configdir = configdir
        self.config_file = os.path.join(self.configdir, self.filename)
        if not machine:
            self.machine = self.get_machine()
        else:
            self.machine = machine

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

        # if AQUA is defined
        aquadir = os.environ.get('AQUA')
        if aquadir:
            configdirs.append(os.path.join(aquadir, 'config'))
            
        # the predefined configdirs is in the main folder of the AQUA repository
        configdirs.extend([os.path.dirname(__file__) + "/../../config"])

        # if the home is defined
        homedir = os.environ.get('HOME')
        if homedir:
            configdirs.append(os.path.join(homedir, '.aqua', 'config'))

        # autosearch
        for configdir in configdirs:
            if os.path.exists(os.path.join(configdir, self.filename)):
                return configdir

        raise FileNotFoundError(f"No config file {self.filename} found in {configdirs}")

    def get_machine(self):
        """
        Extract the name of the machine from the configuration file

        Returns:
            The name of the machine read from the configuration file
        """

        if os.path.exists(self.config_file):
            base = load_yaml(self.config_file)
            try:
                return base['machine']
            except KeyError:
                raise KeyError(f'Cannot find machine information in {self.config_file}')
        else:
            raise FileNotFoundError(f'Cannot find the basic configuration file {self.config_file}!')

    def get_reader_filenames(self):
        """
        Extract the filenames for the reader for catalog, regrid and fixer

        Returns:
            Four strings for the path of the catalog, regrid, fixer and config files
        """

        if os.path.exists(self.config_file):
            base = load_yaml(self.config_file)
            catalog_file = base['reader']['catalog'].format(machine=self.machine,
                                                            configdir=self.configdir)
            if not os.path.exists(catalog_file):
                raise FileNotFoundError(f'Cannot find catalog file in {catalog_file}')
            fixer_folder = base['reader']['fixer'].format(machine=self.machine,
                                                          configdir=self.configdir)
            if not os.path.exists(fixer_folder):
                raise FileNotFoundError(f'Cannot find catalog file in {fixer_folder}')

        return catalog_file, fixer_folder, self.config_file
