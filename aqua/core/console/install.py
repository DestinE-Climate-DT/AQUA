#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA installation operations mixin - includes also listing
'''

import os
import shutil
import sys

from aqua.core.lock import SafeFileLock
from aqua.core.util import load_yaml, dump_yaml
from aqua.core.configurer import ConfigPath

from .util import query_yes_no

# check if aqua.diagnostics is installed
try:
    from aqua.diagnostics import DIAGNOSTIC_CONFIG_DIRECTORIES
    from aqua.diagnostics import DIAGNOSTIC_TEMPLATE_DIRECTORIES
except ImportError:
    DIAGNOSTIC_CONFIG_DIRECTORIES = []
    DIAGNOSTIC_TEMPLATE_DIRECTORIES = []

# folder used for reading/storing catalogs
CATPATH = 'catalogs'

# directories to be installed in the AQUA config folder
CORE_CONFIG_DIRECTORIES = ['catgen', 'data_model', 'fixes', 'grids', 'styles']
CORE_TEMPLATE_DIRECTORIES = ['catgen', 'drop', 'gridbuilder']


class InstallMixin:
    """Mixin for AQUA installation operations"""

    def install(self, args):
        """Install AQUA, find the folders and then install

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Running the AQUA install')

        # configure where to install AQUA
        if args.path is None:
            self._config_home()
        else:
            self._config_path(args.path)
    
        # define the template path
        self.templatepath = os.path.join(self.configpath, 'templates')

        # define from where aqua is installed and copy/link the files
        if args.editable is None:
            self._install_default()
        else:
            self._install_editable(args.editable)

        self._set_machine(args)

    def _config_home(self):
        """Configure the AQUA installation folder, by default inside $HOME"""

        if 'HOME' in os.environ:
            path = os.path.join(os.environ['HOME'], '.aqua')
            self.configpath = path
            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True)
            else:
                self.logger.warning('AQUA already installed in %s', path)
                check = query_yes_no(f"Do you want to overwrite AQUA installation in {path}. "
                                     "You will lose all catalogs installed.", "no")
                if not check:
                    sys.exit()
                else:
                    self.logger.warning('Removing the content of %s', path)
                    shutil.rmtree(path)
                    os.makedirs(path, exist_ok=True)
        else:
            self.logger.error('$HOME not found.'
                              'Please specify a path where to install AQUA and define AQUA_CONFIG as environment variable')
            sys.exit(1)

    def _config_path(self, path):
        """Define the AQUA installation folder when a path is specified

        Args:
            path (str): the path where to install AQUA
        """

        self.configpath = path
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        else:
            if not os.path.isdir(path):
                self.logger.error("Path chosen is not a directory")
                sys.exit(1)

        check = query_yes_no(f"Do you want to create a link in the $HOME/.aqua to {path}", "yes")
        if check:
            if 'HOME' in os.environ:
                link = os.path.join(os.environ['HOME'], '.aqua')
                if os.path.exists(link) or os.path.islink(link):
                    self.logger.warning('Removing the content of %s', link)
                    if os.path.islink(link):
                        os.unlink(link)
                    elif os.path.isdir(link):
                        shutil.rmtree(link)
                    else:
                        os.remove(link)
                os.symlink(path, link)
            else:
                self.logger.error('$HOME not found. Cannot create a link to the installation path')
                self.logger.warning('AQUA will be installed in %s, but please remember to define AQUA_CONFIG environment variable', path)
        else:
            self.logger.warning('AQUA will be installed in %s, but please remember to define AQUA_CONFIG environment variable',
                                path)

    def _install_default(self):
        """Copying the installation file"""

        print("Installing AQUA to", self.configpath)
        for file in ['config-aqua.tmpl']:
            target_file = os.path.splitext(file)[0] + '.yaml'  # replace the tmpl with yaml
            self._copy_update_folder_file(f'{self.corepath}/{file}', f'{self.configpath}/{target_file}')
        for directory in CORE_CONFIG_DIRECTORIES:
            self._copy_update_folder_file(os.path.join(self.corepath, directory),
                                     os.path.join(self.configpath, directory))
        for directory in CORE_TEMPLATE_DIRECTORIES:
            self._copy_update_folder_file(os.path.join(self.corepath, '..', 'templates', directory),
                                     os.path.join(self.templatepath, directory))
        if self.diagpath is not None:
            for directory in DIAGNOSTIC_CONFIG_DIRECTORIES:
                self._copy_update_folder_file(os.path.join(self.diagpath, directory),
                                         os.path.join(self.configpath, directory))
            for directory in DIAGNOSTIC_TEMPLATE_DIRECTORIES:
                self._copy_update_folder_file(os.path.join(self.diagpath, '..', 'templates', directory),
                                         os.path.join(self.templatepath, directory))
        os.makedirs(f'{self.configpath}/{CATPATH}', exist_ok=True)

    def _install_editable(self, editable):
        """
        Linking the installation file in editable mode.
        The editable folder must be the main level of the AQUA repository.

        Args:
            editable (str): the path where to link the AQUA installation files
        """

        editable = os.path.abspath(editable)
        self.logger.info('Installing AQUA in editable mode from %s', editable)
        editable = os.path.join(editable, 'aqua', 'core', 'config')
        print("Installing AQUA with a link from ", editable, " to ", self.configpath)
        for file in ['config-aqua.tmpl']:
            target_file = os.path.splitext(file)[0] + '.yaml'
            if os.path.isfile(os.path.join(editable, file)):
                self._copy_update_folder_file(f'{self.corepath}/{file}', f'{self.configpath}/{target_file}')
            else:
                self.logger.error('%s folder does not include AQUA configuration files. Please use AQUA', editable)
                os.rmdir(self.configpath)
                sys.exit(1)
        for directory in CORE_CONFIG_DIRECTORIES:
            self._copy_update_folder_file(f'{editable}/{directory}', f'{self.configpath}/{directory}', link=True)

        os.makedirs(f'{self.configpath}/templates', exist_ok=True)
        for directory in CORE_TEMPLATE_DIRECTORIES:
            self._copy_update_folder_file(os.path.join(editable, '..', 'templates', directory), f'{self.templatepath}/{directory}', link=True)

        os.makedirs(f'{self.configpath}/{CATPATH}', exist_ok=True)

    def _set_machine(self, args):
        """Modify the config-aqua.yaml with the identified machine"""

        if args.machine is not None:
            machine = args.machine
        else:
            machine = ConfigPath(configdir=self.configpath).get_machine()

        if machine is None:
            self.logger.info('Unknown machine!')
        else:
            self.configfile = os.path.join(self.configpath, 'config-aqua.yaml')
            self.logger.info('Setting machine name to %s', machine)

            with SafeFileLock(self.configfile + '.lock', loglevel=self.loglevel):
                cfg = load_yaml(self.configfile)
                cfg['machine'] = machine
                dump_yaml(self.configfile, cfg)

    def uninstall(self, args):
        """Remove AQUA"""
        print('Remove the AQUA installation')
        self._check()
        check = query_yes_no(f"Do you want to uninstall AQUA from {self.configpath}", "no")
        if check:
            # Remove the AQUA installation both for folder and link case
            if os.path.islink(self.configpath):
                # Remove link and data in the linked folder
                self.logger.info('Removing the link %s', self.configpath)
                os.unlink(self.configpath)
            else:
                self.logger.info('Uninstalling AQUA from %s', self.configpath)
                shutil.rmtree(self.configpath)
        else:
            sys.exit()

    def update(self, args):
        """Update an existing catalog by copying it if not installed in editable mode"""

        self._check()
        if args.catalog:
            if args.catalog == 'all':
                self.logger.info('Updating all AQUA catalogs')
                catalogs = self._list_folder(f'{self.configpath}/{CATPATH}', return_list=True, silent=True)
                for catalog in catalogs:
                    print(f'Updating catalog {catalog} ..')
                    self._update_catalog(os.path.basename(catalog))
            else:
                self._update_catalog(args.catalog)
        else:
            self.logger.info('Updating AQUA installation...')
            for directory in CORE_CONFIG_DIRECTORIES:
                self._copy_update_folder_file(os.path.join(self.corepath, directory),
                                         os.path.join(self.configpath, directory),
                                         update=True)

            for directory in CORE_TEMPLATE_DIRECTORIES:
                self._copy_update_folder_file(os.path.join(self.corepath, '..', 'templates', directory),
                                         os.path.join(self.templatepath, directory),
                                         update=True)
            if self.diagpath is not None:
                for directory in DIAGNOSTIC_CONFIG_DIRECTORIES:
                    self._copy_update_folder_file(os.path.join(self.diagpath, directory),
                                             os.path.join(self.configpath, directory),
                                             update=True)
                for directory in DIAGNOSTIC_TEMPLATE_DIRECTORIES:
                    self._copy_update_folder_file(os.path.join(self.diagpath, '..', 'templates', directory),
                                             os.path.join(self.templatepath, directory),
                                             update=True)

    def _copy_update_folder_file(self, source, target, link=False, update=False):
        """Generic function to copy or update a source to a target folder"""

        # Check if the target exists
        if os.path.exists(target):
            if os.path.islink(target):
                self.logger.error('AQUA has been installed in editable mode, no need to update')
                sys.exit(1)
            # Update case
            if update:
                self.logger.info('Updating %s ...', target)
                if os.path.isdir(target):
                    shutil.rmtree(target)
                else:
                    os.remove(target)

        if os.path.exists(target):
            self.logger.error('%s already exist, please consider update or uninstall', target)
            sys.exit(1)

        # Handle linking
        if link:
            self.logger.info('Linking from %s to %s', source, target)
            os.symlink(source, target)

        # Handle copying
        else:
            if os.path.isdir(source):
                self.logger.info('Copying directory from %s to %s', source, target)
                shutil.copytree(source, target)
            else:
                self.logger.info('Copying file from %s to %s', source, target)
                shutil.copy2(source, target)

    def list(self, args):
        """List installed catalogs"""

        self._check()

        cdir = f'{self.configpath}/{CATPATH}'

        print('AQUA current installed catalogs in', cdir, ':')
        self._list_folder(cdir)

        if args.all:
            for content in CORE_CONFIG_DIRECTORIES + DIAGNOSTIC_CONFIG_DIRECTORIES:
                print(f'AQUA current installed {content} in {self.configpath}:')
                self._list_folder(os.path.join(self.configpath, content))

    def _list_folder(self, mydir, return_list=False, silent=False):
        """
        List all the files in a AQUA config folder and check if they are link or file/folder
        
        Args:
            mydir (str): the directory to be listed
            return_list (bool): if True, return the list of files for further processing
            silent (bool): if True, do not print the files, just return the list
        
        Returns:
            None or list: a list of files if return_list is True, otherwise nothing
        """

        list_files = []
        yaml_files = os.listdir(mydir)
        for file in yaml_files:
            file = os.path.join(mydir, file)
            if os.path.islink(file):
                orig_path = os.readlink(file)
                if not silent:
                    print(f"\t - {file} (editable from {orig_path})")
            else:
                if not silent:
                    print(f"\t - {file}")
                list_files.append(file)
 
        if return_list:
            return list_files
        return None
