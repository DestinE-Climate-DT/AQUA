#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA command line main functions
'''

import os
import shutil
import sys
import subprocess
from aqua.util import load_yaml, dump_yaml, load_multi_yaml
from aqua.logger import log_configure
from aqua.util import ConfigPath
from aqua.cli.parser import parse_arguments
from aqua.util.util import HiddenPrints
from aqua import __path__ as pypath
from aqua import catalogue

# folder used for reading/storing catalogs
catpath = 'catalogs'

class AquaConsole():
    """Class for AquaConsole, the AQUA command line interface for
    installation, catalog, grids and fixes editing"""

    def __init__(self):
        """The main AQUA command line interface"""

        self.pypath = pypath[0]
        self.aquapath = os.path.join(os.path.dirname(self.pypath), 'config')
        self.configpath = None
        self.configfile = 'config-aqua.yaml'
        self.grids = None
        self.logger = None

        self.command_map = {
            'install': self.install,
            'enable': {
                'tropical_rainfall': self.enable_tropical_rainfall,
            },
            'add': self.add,
            'remove': self.remove,
            'set': self.set,
            'uninstall': self.uninstall,
            'list': self.list,
            'update': self.update,
            'fixes': {
                'add': self.fixes_add,
                'remove': self.remove_file
            },
            'grids': {
                'add': self.grids_add,
                'remove': self.remove_file
            }
        }

    def execute(self):
        """Parse AQUA class and run the required command"""

        parser_dict = parse_arguments()
        parser = parser_dict['main']
        args = parser.parse_args(sys.argv[1:])

        # Set the log level
        if args.very_verbose or (args.verbose and args.very_verbose):
            loglevel = 'DEBUG'
        elif args.verbose:
            loglevel = 'INFO'
        else:
            loglevel = 'WARNING'
        self.logger = log_configure(loglevel, 'AQUA')

        command = args.command
        method = self.command_map.get(command, parser.print_help)
        if command not in self.command_map:
            parser.print_help()
        else:
            # Handle nested commands
            if isinstance(self.command_map[command], dict):
                if hasattr(args, 'nested_command') and args.nested_command:
                    nested_method = self.command_map[command].get(args.nested_command, parser_dict[command].print_help)
                    nested_method(args)
                else:
                    parser_dict[command].print_help()
            else:
                method(args)


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
                if os.path.exists(link):
                    self.logger.warning('Removing the content of %s', link)
                    shutil.rmtree(link)
                os.symlink(path, link)
            else:
                self.logger.error('$HOME not found. Cannot create a link to the installation path')
                self.logger.warning('AQUA will be installed in %s, but please remember to define AQUA_CONFIG environment variable', path)  # noqa
        else:
            self.logger.warning('AQUA will be installed in %s, but please remember to define AQUA_CONFIG environment variable',
                                path)

    def _install_default(self):
        """Copying the installation file"""

        print("Installing AQUA to", self.configpath)
        for file in ['config-aqua.tmpl']:
            target_file = os.path.splitext(file)[0] + '.yaml' #replace the tmpl with yaml
            if not os.path.exists(os.path.join(self.configpath, target_file)):
                self.logger.info('Copying from %s to %s', self.aquapath, self.configpath)
                shutil.copy(f'{self.aquapath}/{file}', f'{self.configpath}/{target_file}')
        for directory in ['fixes', 'data_models', 'grids']:
            if not os.path.exists(os.path.join(self.configpath, directory)):
                self.logger.info('Copying from %s to %s',
                                 os.path.join(self.aquapath, directory), self.configpath)
                shutil.copytree(f'{self.aquapath}/{directory}', f'{self.configpath}/{directory}')
        os.makedirs(f'{self.configpath}/{catpath}', exist_ok=True)

    def _install_editable(self, editable):
        """Linking the installation file in editable

        Args:
            editable (str): the path where to link the AQUA installation files
        """

        editable = os.path.abspath(editable)
        print("Installing AQUA with a link from ", editable, " to ", self.configpath)
        for file in ['config-aqua.tmpl']:
            target_file = os.path.splitext(file)[0] + '.yaml'
            if os.path.isfile(os.path.join(editable, file)):
                if not os.path.exists(os.path.join(self.configpath, file)):
                    self.logger.info('Linking from %s to %s', editable, self.configpath)
                    #os.symlink(f'{editable}/{file}', f'{self.configpath}/{file}')
                    shutil.copy(f'{editable}/{file}', f'{self.configpath}/{target_file}')
            else:
                self.logger.error('%s folder does not include AQUA configuration files. Please use AQUA/config', editable)
                os.rmdir(self.configpath)
                sys.exit(1)
        for directory in ['fixes', 'data_models', 'grids']:
            if not os.path.exists(os.path.join(self.configpath, directory)):
                self.logger.info('Linking from %s to %s',
                                 os.path.join(editable, directory), self.configpath)
                os.symlink(f'{editable}/{directory}', f'{self.configpath}/{directory}')
        os.makedirs(f'{self.configpath}/{catpath}', exist_ok=True)

    def _set_machine(self, args):
        """Modify the config-aqua.yaml with the identified machine"""

        if args.machine is not None:
            machine = args.machine
        else:
            machine = ConfigPath(configdir=self.configpath).get_machine()

        if machine is None:
            self.logger.info('Unknown machine!')
        else:
            #if args.editable:
            #    self.logger.info('Editable version installed, not modifying the machine name and leaving in auto')
            #else:
            self.configfile = os.path.join(self.configpath, 'config-aqua.yaml')
            self.logger.info('Setting machine name to %s', machine)
            cfg = load_yaml(self.configfile)
            cfg['machine'] = machine
            
            dump_yaml(self.configfile, cfg)


    def set(self, args):
        """Set an installed catalog as the one used in the config-aqua.yaml

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        self._check()

        if os.path.exists(f"{self.configpath}/{catpath}/{args.catalog}"):
            self._set_catalog(args.catalog)
        else:
            self.logger.error('%s catalog is not installed!', args.catalog)
            sys.exit(1)

    def list(self, args):
        """List installed catalogs"""

        self._check()

        cdir = f'{self.configpath}/{catpath}'
        contents = os.listdir(cdir)

        print('AQUA current installed catalogs in', cdir, ':')
        self._list_folder(cdir)

        if args.all:
            contents = ['data_models', 'grids', 'fixes']
            for content in contents:
                print(f'AQUA current installed {content} in {self.configpath}:')
                self._list_folder(os.path.join(self.configpath, content))

    def _list_folder(self, mydir):
        """List all the files in a AQUA config folder and check if they are link or file/folder"""

        yaml_files = os.listdir(mydir)
        for file in yaml_files:
            file = os.path.join(mydir, file)
            if os.path.islink(file):
                orig_path = os.readlink(file)
                print(f"\t - {file} (editable from {orig_path})")
            else:
                print(f"\t - {file}")

    def fixes_add(self, args):
        """Add a fix file

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        compatible = self._check_file(kind='fixes', file=args.file)
        if compatible:
            self._file_add(kind='fixes', file=args.file, link=args.editable)

    def grids_add(self, args):
        """Add a grid file

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        compatible = self._check_file(kind='grids', file=args.file)
        if compatible:
            self._file_add(kind='grids', file=args.file, link=args.editable)

    def _file_add(self, kind, file, link=False):
        """Add a personalized file to the fixes/grids folder

        Args:
            kind (str): the kind of file to be added, either 'fixes' or 'grids'
            file (str): the file to be added
            link (bool): whether to add the file as a link or not
        """

        file = os.path.abspath(file)
        self._check()
        basefile = os.path.basename(file)
        pathfile = f'{self.configpath}/{kind}/{basefile}'
        if not os.path.exists(pathfile):
            if link:
                self.logger.info('Linking %s to %s', file, pathfile)
                os.symlink(file, pathfile)
            else:
                self.logger.info('Installing %s to %s', file, pathfile)
                shutil.copy(file, pathfile)
        else:
            self.logger.error('%s for file %s already installed, or a file with the same name exists', kind, file)
            sys.exit(1)

    def add(self, args):
        """Add a catalog and set it as a default in config-aqua.yaml

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        print('Adding the AQUA catalog', args.catalog)
        self._check()


        if args.editable is not None:
            self._add_catalog_editable(args.catalog, args.editable)
        else:
            self._add_catalog_default(args.catalog)

        # verify that the new catalog is compatible with AQUA, loading it with catalogue()
        try:
            with HiddenPrints():
                catalogue()
        except Exception as e:
            self.remove(args)
            self.logger.error('Current catalog is not compatible with AQUA, removing it for safety!')
            self.logger.error(e)
            sys.exit(1)


    def _add_catalog_editable(self, catalog, editable):
        """Add a catalog in editable mode (i.e. link)"""

        cdir = f'{self.configpath}/{catpath}/{catalog}'
        editable = os.path.abspath(editable)
        print('Installing catalog in editable mode from', editable, 'to', self.configpath)
        if os.path.exists(editable):
            if os.path.exists(cdir):
                self.logger.error('Catalog %s already installed in %s, please consider `aqua remove`',
                                  catalog, cdir)
                sys.exit(1)
            else:
                os.symlink(editable, cdir)
        else:
            self.logger.error('Catalog %s cannot be found in %s', catalog, editable)
            sys.exit(1)
    
        self._set_catalog(catalog)

    def _add_catalog_default(self, catalog):
        """Add a catalog in default mode"""

        # check if catalog is a path or a name
        if '/' in catalog:
            if os.path.exists(catalog):
                sdir = catalog
                catalog = os.path.basename(catalog)
                self.logger.info('%s catalog is installed from disk from %s', catalog, sdir)
            else:
                self.logger.error('Cannot find %s catalog, is the path correct?', catalog)
                sys.exit(1)
        else:
            sdir = f'{self.aquapath}/{catpath}/{catalog}'

        # define target
        cdir = f'{self.configpath}/{catpath}/{catalog}'

        if not os.path.exists(cdir):
            if os.path.isdir(sdir):
                shutil.copytree(sdir, cdir)
            else:
                self.logger.error('Catalog %s does not appear to exist in %s', catalog, sdir)
                self.logger.error('Available catalogs are: %s', os.listdir(f'{self.aquapath}/{catpath}'))
                sys.exit(1)
        else:
            self.logger.error("Catalog %s already installed in %s, please consider `aqua update`.",
                              catalog, cdir)
            sys.exit(1)

        self._set_catalog(catalog)

    def update(self, args):
        """Update an existing catalog by copying it if not installed in editable mode"""

        self._check()
        cdir = f'{self.configpath}/{catpath}/{args.catalog}'
        sdir = f'{self.aquapath}/{catpath}/{args.catalog}'
        if os.path.exists(cdir):
            if os.path.islink(cdir):
                self.logger.error('%s catalog has been installed in editable mode, no need to update', args.catalog)
                sys.exit(1)
            else:
                self.logger.info('Removing %s from %s', args.catalog, sdir)
                shutil.rmtree(cdir)
                #self.logger.info('Copying %s from %s', args.catalog, sdir)
                #shutil.copytree(sdir, cdir)
                self._add_catalog_default(args.catalog)
        else:
            self.logger.error('%s does not appear to be installed, please consider `aqua add`', args.catalog)
            sys.exit(1)

    def _set_catalog(self, catalog):
        """Modify the config-aqua.yaml with the proper catalog

        Args:
            catalog (str): the catalog to be set as the default in the config-aqua.yaml
        """

        self.logger.info('Setting catalog name to %s', catalog)
        cfg = load_yaml(self.configfile)
        cfg['catalog'] = catalog
        dump_yaml(self.configfile, cfg)

    def remove(self, args):
        """Remove a catalog

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        if '/' in args.catalog:
            args.catalog = os.path.basename(args.catalog)
        cdir = f'{self.configpath}/{catpath}/{args.catalog}'
        print('Remove the AQUA catalog', args.catalog, 'from', cdir)
        if os.path.exists(cdir):
            if os.path.islink(cdir):
                os.unlink(cdir)
            else:
                shutil.rmtree(cdir)
        else:
            self.logger.error('Catalog %s is not installed in %s, cannot remove it',
                              args.catalog, cdir)
            sys.exit(1)

    def remove_file(self, args):
        """Add a personalized file to the fixes/grids folder

        Args:
            kind (str): the kind of file to be added, either 'fixes' or 'grids'
            file (str): the file to be added
        """

        self._check()
        kind = args.command
        file = os.path.basename(args.file)
        pathfile = f'{self.configpath}/{kind}/{file}'
        if os.path.exists(pathfile):
            self.logger.info('Removing %s', pathfile)
            if os.path.islink(pathfile):
                os.unlink(pathfile)
            else:
                os.remove(pathfile)
        else:
            self.logger.error('%s file %s is not installed in AQUA, cannot remove it',
                              kind, file)
            sys.exit(1)

    def _check(self):
        """check installation"""
        try:
            self.configpath = ConfigPath().configdir
            self.configfile = os.path.join(self.configpath, 'config-aqua.yaml')
            self.logger.debug('AQUA found in %s', self.configpath)
        except FileNotFoundError:
            self.logger.error('No AQUA installation found!')
            sys.exit(1)

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

    def _check_file(self, kind, file=None):
        """
        Check if a new file can be merged with AQUA load_multi_yaml()
        It works also without a new file to check that the existing files are compatible

        Args:
            kind (str): the kind of file to be added, either 'fixes' or 'grids'
            file (str): the file to be added
        """
        if kind not in ['fixes', 'grids']:
            raise ValueError('Kind must be either fixes or grids')

        self._check()
        try:
            load_multi_yaml(folder_path=f'{self.configpath}/{kind}',
                            filenames=[file]) if file is not None else load_multi_yaml(folder_path=f'{self.configpath}/{kind}')

            if file is not None:
                self.logger.debug('File %s is compatible with the existing files in %s', file, kind)

            return True
        except Exception as e:
            if file is not None:
                if not os.path.exists(file):
                    self.logger.error('%s is not a valid file!', file)
                else:
                    self.logger.error("It is not possible to add the file %s to the %s folder", file, kind)
            else:
                self.logger.error("Existing files in the %s folder are not compatible", kind)
            self.logger.error(e)
            return False
        
    def enable_tropical_rainfall(self, args):
        """Enable Tropical Rainfall package

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Enabling Tropical Rainfall package')

        # Use pip to install the tropical_rainfall package
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "diagnostics/tropical_rainfall/"])
            self.logger.info('Tropical Rainfall package enabled successfully')
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Failed to enable Tropical Rainfall package: {e}")
            sys.exit(1)


def main():
    """AQUA main installation tool"""
    aquacli = AquaConsole()
    aquacli.execute()


def query_yes_no(question, default="yes"):
    # from stackoverflow
    """Ask a yes/no question via input() and return their answer.

    Args:
        question (str): the question to be asked to the user
        default (str): the default answer if the user just hits <Enter>.
                       It must be "yes" (the default), "no" or None (meaning
                       an answer is required of the user).

    Returns:
        bool: True for yes, False for no
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError(f"invalid default answer: {default}")

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').")
