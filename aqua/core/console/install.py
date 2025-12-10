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

    def _check(self, silent=False, return_info=False):
        """
        Check installation and optionally return detailed information
        
        Args:
            silent (bool): If True, suppress errors and use ERROR loglevel
            return_info (bool): If True, return installation info dict instead of exiting
            
        Returns:
            dict or None: If return_info=True, returns:
                {
                    'installed': bool,
                    'configpath': str or None,
                    'core': {'installed': bool, 'mode': str},
                    'diagnostics': {'installed': bool, 'mode': str}
                }
        """
        checklevel = 'ERROR' if silent else self.loglevel

        try:
            self.configpath = ConfigPath(loglevel=checklevel).configdir
            self.configfile = os.path.join(self.configpath, 'config-aqua.yaml')
            self.templatepath = os.path.join(self.configpath, 'templates')
            self.logger.debug('AQUA found in %s', self.configpath)

            if return_info:
                # Gather detailed installation information
                info = {
                    'installed': True,
                    'configpath': self.configpath,
                    'core': {
                        'installed': self._check_component_installed('core'),
                        'mode': self._get_component_mode('core')
                    },
                    'diagnostics': {
                        'installed': self._check_component_installed('diagnostics'),
                        'mode': self._get_component_mode('diagnostics')
                    }
                }
                return info
                
        except FileNotFoundError:
            if return_info:
                return {
                    'installed': False,
                    'configpath': None,
                    'core': {'installed': False, 'mode': 'not_installed'},
                    'diagnostics': {'installed': False, 'mode': 'not_installed'}
                }
            
            self.logger.error('No AQUA installation found!')
            sys.exit(1)
        
    def _check_component_installed(self, component):
        """
        Check if a specific component is already installed
        
        Args:
            component (str): 'core' or 'diagnostics'
            
        Returns:
            bool: True if component is installed
        """

        # get the first config directory for the component
        directories = self._get_config_dirs(component)
        for directory in directories:
            if os.path.exists(os.path.join(self.configpath, directory)):
                return True
        return False

    def _get_component_mode(self, component):
        """
        Check if a component is installed in editable mode
        
        Args:
            component (str): 'core' or 'diagnostics'
            
        Returns:
            str: 'editable', 'standard', or 'not_installed'
        """
        # Check the installation mode based on the first config directory
        directories = self._get_config_dirs(component)
        for directory in directories:
            path = os.path.join(self.configpath, directory)
            if os.path.islink(path):
                return 'editable'
            if os.path.exists(path):
                return 'standard'

        return None

    def _validate_component_path(self, component, custom_path):
        """Validate that editable install path exists and return the source path
        
        Args:
            component (str): 'core' or 'diagnostics'
            custom_path (str): Path to the editable installation
            
        Returns:
            str: Absolute path to the component config directory
        """
        actual_source = os.path.join(
            os.path.abspath(custom_path), 'aqua', component, 'config'
        )
        if not os.path.exists(actual_source):
            self.logger.error(f'{actual_source} does not exist for {component}')
            sys.exit(1)
        return actual_source

    def _install_component(self, component, mode, source_path, config_dirs, template_dirs):
        """
        Install a component (core or diagnostics) in either standard or editable mode.
        
        Args:
            component (str): 'core' or 'diagnostics'
            mode (dict): Installation mode with 'mode' and 'path' keys
            source_path (str): Default source path for the component
            config_dirs (list): List of config directories to install
            template_dirs (list): List of template directories to install
        """
        install_mode = mode['mode']
        custom_path = mode['path']

        self.logger.info(f'Installing {component} in {install_mode} mode')

        # define if we need to link
        link = bool(install_mode == 'editable')

        # Determine the actual source path
        if link:
            actual_source = self._validate_component_path(component, custom_path)
        else:
            # Use the default source path
            actual_source = source_path
    
        # Install config directories
        os.makedirs(self.configpath, exist_ok=True)
        for directory in config_dirs:
            source = os.path.join(actual_source, directory)
            target = os.path.join(self.configpath, directory)
        
            self._copy_update_folder_file(source, target, link=link)
    
        # do the same for the templates
        template_source_base = os.path.join(actual_source, '..', 'templates')

        os.makedirs(self.templatepath, exist_ok=True)
        for directory in template_dirs:
            source = os.path.join(template_source_base, directory)
            target = os.path.join(self.templatepath, directory)
    
            if os.path.exists(source):
                self._copy_update_folder_file(source, target, link=link)

    def _determine_component_mode(self, core_arg, diag_arg, component):
        """
        Determine installation mode for a component based on CLI arguments.
        
        Args:
            core_arg: CLI argument for --core
            diag_arg: CLI argument for --diagnostics
            component (str): 'core' or 'diagnostics' - which component to check
            
        Returns:
            dict or False: Installation mode dict {'mode': str, 'path': str|None} 
                          or False to skip component
        """
        if component == 'core':
            if core_arg is not None:
                return self._get_install_mode(core_arg)
            if diag_arg is not None:
                return False
        else:  # diagnostics
            if diag_arg is not None:
                return self._get_install_mode(diag_arg)
            if core_arg is not None:
                return False
  
        # Neither specified: full install in standard mode
        return {'mode': 'standard', 'path': None}

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
    
        # Check current installation status
        install_info = self._check(silent=True, return_info=True)
        
        # Determine installation mode for each component
        core_mode = self._determine_component_mode(args.core, args.diagnostics, 'core')
        diag_mode = self._determine_component_mode(args.core, args.diagnostics, 'diagnostics')
        self.logger.debug('Installation modes - core: %s, diagnostics: %s', core_mode, diag_mode)

        if install_info['installed']:
            self.logger.warning('AQUA installation found in %s', self.configpath)
            if core_mode:
                self.logger.warning('Proceeding will remove the existing installation and all catalogs.')
                self.logger.warning('It will remove the diagnostics component if already installed.')
                if not self._remove_installation(confirm=True):
                    sys.exit()
            
            if core_mode is False and diag_mode:
                if not install_info['core']['installed']:
                    self.logger.error('Cannot install diagnostics without core. Install core first or use full installation.')
                    sys.exit(1)
                if install_info['diagnostics']['installed']:
                    self.logger.error('Diagnostics component is already installed. We cannot add it again, please use \'aqua uninstall\' before')
                    sys.exit(1)
                self.logger.info('Core already installed (%s mode), adding diagnostics component',
                           install_info['core']['mode'])
    
        # Validation: if installing only diagnostics, core must already be installed
        if core_mode is False and diag_mode:
            if not install_info['core']['installed']:
                self.logger.error('Cannot install diagnostics without core. Install core first or use full installation.')
                sys.exit(1)
            self.logger.info('Core already installed (%s mode), adding diagnostics component',
                           install_info['core']['mode'])

        # Install core
        if core_mode:
            self.logger.debug('Installing core component')
            self._install_component(
                component='core',
                mode=core_mode,
                source_path=self.corepath,
                config_dirs=CORE_CONFIG_DIRECTORIES,
                template_dirs=CORE_TEMPLATE_DIRECTORIES
            )
        
            self.logger.debug('Creating config-aqua.yaml configuration file')
            self._copy_update_folder_file(
                os.path.join(self.corepath, 'config-aqua.tmpl'),
                os.path.join(self.configpath, 'config-aqua.yaml')
            )

        # Install diagnostics if available and not skipped
        if diag_mode:
            if self.diagpath is not None:
                self.logger.debug('Installing diagnostics component')
                self._install_component(
                    component='diagnostics',
                    mode=diag_mode,
                    source_path=self.diagpath,
                    config_dirs=DIAGNOSTIC_CONFIG_DIRECTORIES,
                    template_dirs=DIAGNOSTIC_TEMPLATE_DIRECTORIES
                )
            else:
                if core_mode:
                    self.logger.warning('aqua.diagnostics package not found. Skipping diagnostics installation.')
                else:
                    self.logger.error('aqua.diagnostics package not found. Skipping diagnostics installation.')
                    sys.exit(1)

        # Create catalogs directory
        os.makedirs(os.path.join(self.configpath, CATPATH), exist_ok=True)

        # set machine
        self._set_machine(args)

    def _config_home(self):
        """Configure the AQUA installation folder, by default inside $HOME"""

        if 'HOME' in os.environ:
            path = os.path.join(os.environ['HOME'], '.aqua')
            self.configpath = path
            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True)
            else:
                self.logger.debug('AQUA installation found in %s', path)
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

    def _remove_installation(self, confirm=True):
        """
        Remove AQUA installation directory
        
        Args:
            confirm (bool): Whether to ask for confirmation
            
        Returns:
            bool: True if removed, False if user cancelled
        """
        if confirm:
            check = query_yes_no(
                f"Do you want to remove AQUA installation in {self.configpath}. "
                "You will lose all catalogs installed.", "no"
            )
            if not check:
                return False

        self.logger.warning('Removing the content of %s', self.configpath)

        if os.path.islink(self.configpath):
            self.logger.info('Removing the link %s', self.configpath)
            os.unlink(self.configpath)
        else:
            self.logger.info('Removing directory %s', self.configpath)
            shutil.rmtree(self.configpath)

        return True

    def uninstall(self, args):
        """Remove AQUA"""
        print('Remove the AQUA installation')
        self._check()
        if not self._remove_installation(confirm=True):
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

            # Update core if not in editable mode
            self._update_component(
                source_path=self.corepath,
                config_dirs=CORE_CONFIG_DIRECTORIES,
                template_dirs=CORE_TEMPLATE_DIRECTORIES,
                component_name='core'
            )

            # Update diagnostics if available and not in editable mode
            if self.diagpath is not None:
                self._update_component(
                    source_path=self.diagpath,
                    config_dirs=DIAGNOSTIC_CONFIG_DIRECTORIES,
                    template_dirs=DIAGNOSTIC_TEMPLATE_DIRECTORIES,
                    component_name='diagnostics'
                )

    def _update_component(self, source_path, config_dirs, template_dirs, component_name):
        """
        Update a component (core or diagnostics), skipping directories in editable mode.
        
        Args:
            source_path (str): Source path for the component
            config_dirs (list): List of config directories
            template_dirs (list): List of template directories
            component_name (str): Name of the component for logging
        """
        for directory in config_dirs:
            target = os.path.join(self.configpath, directory)
            
            # Skip if in editable mode
            if os.path.islink(target):
                self.logger.info(f'Skipping {component_name}/{directory} (editable mode)')
                continue
                
            source = os.path.join(source_path, directory)
            self._copy_update_folder_file(source, target, update=True)

        for directory in template_dirs:
            target = os.path.join(self.templatepath, directory)
            
            # Skip if in editable mode
            if os.path.islink(target):
                self.logger.info(f'Skipping {component_name}/{directory} template (editable mode)')
                continue
                
            source = os.path.join(source_path, '..', 'templates', directory)
            self._copy_update_folder_file(source, target, update=True)

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

    @staticmethod
    def _list_folder(mydir, return_list=False, silent=False):
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
        
    @staticmethod
    def _get_config_dirs(component):
        """
        Get the config directories for a component
        
        Args:
            component (str): 'core' or 'diagnostics'

        Returns:
            list: List of config directories
        """
        if component == 'core':
            return CORE_CONFIG_DIRECTORIES
        if component == 'diagnostics':
            return DIAGNOSTIC_CONFIG_DIRECTORIES
        raise ValueError(f"Unknown component: {component}")
    
    @staticmethod
    def _get_install_mode(specific_arg):
        """
        Determine the installation mode for a component.
        
        Args:
            specific_arg: Value of --core or --diagnostics argument
            
        Returns:
            dict: {'mode': 'editable'|'standard', 'path': str|None}
        """
        # If specific_arg is a string path (not 'standard'), use editable mode
        if isinstance(specific_arg, str) and specific_arg != 'standard':
            return {'mode': 'editable', 'path': specific_arg}

        # If specific_arg is 'standard' (flag without value), use standard mode
        return {'mode': 'standard', 'path': None}
