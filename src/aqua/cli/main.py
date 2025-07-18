#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA command line main functions
'''

import os
import shutil
import sys
from urllib.error import HTTPError
import fsspec

from aqua import __path__ as pypath
from aqua import catalog as print_catalog
from aqua.util import load_yaml, dump_yaml, load_multi_yaml, ConfigPath, create_folder
from aqua.logger import log_configure
from aqua.util.util import HiddenPrints, to_list

from aqua.cli.parser import parse_arguments
from aqua.cli.diagnostic_config import diagnostic_config
from aqua.cli.lra import lra_execute
from aqua.cli.catgen import catgen_execute
from aqua.cli.builder import builder_execute


# folder used for reading/storing catalogs
CATPATH = 'catalogs'

# directories to be installed in the AQUA config folder
BASIC_DIRECTORIES = ['fixes', 'data_model', 'grids', 'catgen', 'datachecker', 'styles']


class AquaConsole():
    """Class for AquaConsole, the AQUA command line interface for
    installation, catalog, grids and fixes editing"""

    def __init__(self):
        """The main AQUA command line interface"""

        self.pypath = pypath[0]
        # NOTE: the aqua src code is in the src/aqua folder, so we need to go up one level
        #       to find the config folder
        self.aquapath = os.path.join(os.path.dirname(self.pypath), '../config')
        self.configpath = None
        self.configfile = 'config-aqua.yaml'
        self.grids = None
        self.logger = None
        self.loglevel = 'WARNING'

        self.command_map = {
            'install': self.install,
            'add': self.add,
            'remove': self.remove,
            'set': self.set,
            'uninstall': self.uninstall,
            'avail': self.avail,
            'list': self.list,
            'update': self.update,
            'fixes': {
                'add': self.fixes_add,
                'remove': self.remove_file
            },
            'grids': {
                'add': self.grids_add,
                'remove': self.remove_file,
                'set': self.grids_set,
                'build': self.grids_build
            },
            'lra': self.lra,
            'catgen': self.catgen
        }

    def execute(self):
        """Parse AQUA class and run the required command"""

        parser_dict = parse_arguments()
        parser = parser_dict['main']
        args = parser.parse_args()

        # Set the log level
        if args.very_verbose or (args.verbose and args.very_verbose):
            self.loglevel = 'DEBUG'
        elif args.verbose:
            self.loglevel = 'INFO'

        self.logger = log_configure(self.loglevel, 'AQUA')

        command = args.command
        method = self.command_map.get(command, parser_dict['main'].print_help)

        if command not in self.command_map:
            parser_dict['main'].print_help()
        else:
            # nested map
            if isinstance(self.command_map[command], dict):
                if args.nested_command:
                    self.command_map[command][args.nested_command](args)
                else:
                    parser_dict[command].print_help()
            # default
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
            for diagnostic_type in diagnostic_config:
                self._install_default_diagnostics(diagnostic_type)
        else:
            self._install_editable(args.editable)
            for diagnostic_type in diagnostic_config:
                self._install_editable_diagnostics(diagnostic_type, args.editable)

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
            target_file = os.path.splitext(file)[0] + '.yaml'  # replace the tmpl with yaml
            self._copy_update_folder_file(f'{self.aquapath}/{file}', f'{self.configpath}/{target_file}')
        for directory in BASIC_DIRECTORIES:
            self._copy_update_folder_file(os.path.join(self.aquapath, directory),
                                     os.path.join(self.configpath, directory))
        for directory in ['templates']:
            self._copy_update_folder_file(os.path.join(self.aquapath, '..', directory),
                                     os.path.join(self.configpath, directory))
        os.makedirs(f'{self.configpath}/{CATPATH}', exist_ok=True)

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
                self._copy_update_folder_file(f'{self.aquapath}/{file}', f'{self.configpath}/{target_file}')
            else:
                self.logger.error('%s folder does not include AQUA configuration files. Please use AQUA/config', editable)
                os.rmdir(self.configpath)
                sys.exit(1)
        for directory in BASIC_DIRECTORIES:
            self._copy_update_folder_file(f'{editable}/{directory}', f'{self.configpath}/{directory}', link=True)

        for directory in ['templates']:
            self._copy_update_folder_file(os.path.join(editable, '..', directory), f'{self.configpath}/{directory}', link=True)

        os.makedirs(f'{self.configpath}/{CATPATH}', exist_ok=True)

    def _install_default_diagnostics(self, diagnostic_type):
        """Copy the config file from the diagnostics path to AQUA"""

        if diagnostic_type not in diagnostic_config:
            self.logger.error('Unknown diagnostic type: %s', diagnostic_type)
            sys.exit(1)

        for config in diagnostic_config[diagnostic_type]:
            # NOTE: the aqua src code is in the src/aqua folder, so we need to go up one level
            #       to find the config folder
            source_path = os.path.join(os.path.dirname(self.pypath), '../', config['source_path'])
            config_file = config['config_file']
            target_directory = os.path.join(self.configpath, config['target_path'])
            target_file = os.path.join(target_directory, config_file)

            if not os.path.exists(source_path):
                self.logger.error('The source path %s does not exist. Please check the path.', source_path)
                sys.exit(1)

            source_file = os.path.join(source_path, config_file)

            if not os.path.isfile(source_file):
                self.logger.error('The config file %s does not exist in the source path. Please check the path.', source_file)
                sys.exit(1)

            # Ensure the target directory exists using create_folder
            create_folder(target_directory, loglevel=self.loglevel)

            if not os.path.exists(target_file):
                self.logger.debug('Copying from %s to %s', source_file, target_file)
                shutil.copy(source_file, target_file)
            else:
                self.logger.debug('Config file %s already exists in the target path %s. Skipping copy.',
                                  config_file, target_directory)

    def _install_editable_diagnostics(self, diagnostic_type, editable):
        """Create a symbolic link for the config file from the diagnostics path to AQUA"""

        if diagnostic_type not in diagnostic_config:
            self.logger.error('Unknown diagnostic type: %s', diagnostic_type)
            sys.exit(1)

        if not os.path.exists(editable):
            self.logger.error('The editable path %s does not exist. Please check the path.', editable)
            sys.exit(1)

        for config in diagnostic_config[diagnostic_type]:
            editable = os.path.abspath(editable)
            self.logger.info("Copying %s config files from %s to %s", diagnostic_type, editable, self.configpath)
            source_path = os.path.join(os.path.dirname(editable), config['source_path'])
            config_file = config['config_file']
            target_directory = os.path.join(self.configpath, config['target_path'])
            target_file = os.path.join(target_directory, config_file)

            if not os.path.exists(source_path):
                self.logger.error('The source path %s does not exist. Please check the path.', source_path)
                sys.exit(1)

            source_file = os.path.join(source_path, config_file)

            if not os.path.isfile(source_file):
                self.logger.error('The config file %s does not exist in the source path. Please check the path.', source_file)
                sys.exit(1)

            # Ensure the target directory exists using create_folder
            create_folder(target_directory, loglevel=self.loglevel)

            if not os.path.exists(target_file):
                self.logger.debug('Linking from %s to %s', source_file, target_file)
                os.symlink(source_file, target_file)
            else:
                self.logger.debug('Config file %s already exists in the target path %s. Skipping link.',
                                  config_file, target_directory)

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
            cfg = load_yaml(self.configfile)
            cfg['machine'] = machine

            dump_yaml(self.configfile, cfg)

    def set(self, args):
        """Set an installed catalog as the one used in the config-aqua.yaml

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        self._check()

        if os.path.exists(f"{self.configpath}/{CATPATH}/{args.catalog}"):
            self._set_catalog(args.catalog)
        else:
            self.logger.error('%s catalog is not installed!', args.catalog)
            sys.exit(1)

    def list(self, args):
        """List installed catalogs"""

        self._check()

        cdir = f'{self.configpath}/{CATPATH}'
        contents = os.listdir(cdir)

        print('AQUA current installed catalogs in', cdir, ':')
        self._list_folder(cdir)

        if args.all:
            for content in BASIC_DIRECTORIES:
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
        else:
            return None

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
    
    def grids_build(self, args):
        """Build grids from data sources

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        print('Running AQUA grids builder')
        builder_execute(args)

    def grids_set(self, args):
        """
        Set the grids (and concurrently the weights and areas) paths in the config-aqua.yaml
        This will override the grids paths defined in the individual catalogs
        
        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        grids_path = args.path + '/grids'
        areas_path = args.path + '/areas'
        weights_path = args.path + '/weights'

        self.logger.info('Setting grids path to %s, weights path to %s and areas path to %s',
                         grids_path, weights_path, areas_path)
        
        # Check if the paths exist and if not create them
        for path in [grids_path, areas_path, weights_path]:
            if not os.path.exists(path):
                self.logger.info('Creating path %s', path)
                os.makedirs(path, exist_ok=True)

        cfg = load_yaml(os.path.join(self.configpath, self.configfile))
        path_dict = {
            'paths': {
                'grids': grids_path,
                'areas': areas_path,
                'weights': weights_path
            }
        }

        # If the paths already exist, we just update them
        if 'paths' in cfg:
            self.logger.info('Updating existing paths in %s', self.configfile)
            cfg['paths'].update(path_dict['paths'])
        else:
            self.logger.info('Adding new paths to %s', self.configfile)
            cfg['paths'] = path_dict['paths']

        dump_yaml(self.configfile, cfg)

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
        self._check(silent=True)

        if args.editable is not None:
            self._add_catalog_editable(args.catalog, args.editable)
        else:
            self._add_catalog_github(args.catalog, args.repository)

        # verify that the new catalog is compatible with AQUA, loading it with catalog()
        try:
            with HiddenPrints():
                print_catalog()
        except Exception as e:
            self.remove(args)
            self.logger.error('Current catalog is not compatible with AQUA, removing it for safety!')
            self.logger.error(e)
            sys.exit(1)

    def _add_catalog_editable(self, catalog, editable):
        """Add a catalog in editable mode (i.e. link)"""

        cdir = f'{self.configpath}/{CATPATH}/{catalog}'
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

    def _github_explore(self, repository=None):
        """
        Explore the remote GitHub repository
        
        Args:
            repository (str): the repository to explore, if None it uses the default
                              DestinE-Climate-DT/Climate-DT-catalog

        Returns:
            fsspec.filesystem: the filesystem object for the GitHub repository
        """
        if repository is None:
            org = 'DestinE-Climate-DT'
            repo = 'Climate-DT-catalog'
        else:
            try:
                org, repo = repository.split('/')
            except ValueError:
                self.logger.error('Repository should be in the format user/repo, got %s', repository)
                sys.exit(1)
        try:
            # Detect if running in GitHub Actions
            is_github_actions = os.getenv("GITHUB_ACTIONS") == "true"
            token = os.getenv("GITHUB_TOKEN")  # Get GitHub token

            # Use authentication only when running inside GitHub Actions
            username = 'github-actions' if is_github_actions else os.getenv("GITHUB_USER")
            if token and username:
                auth_kwargs = {"username": username, "token": token}
                self.logger.info("Using authenticated access to GitHub API.")
            else:
                auth_kwargs = {}
                self.logger.warning("Running without authentication. Rate limits may apply.")
                self.logger.warning("Consider setting GITHUB_TOKEN and GITHUB_USER environment variables for authenticated access.")

            fs = fsspec.filesystem(
                "github",
                org=org,
                repo=repo,
                **auth_kwargs  # Apply authentication if available
            )
            self.logger.info('Accessed remote repository https://github.com/%s/%s', org, repo)
        except HTTPError:
            self.logger.error('Permission issues in accessing Climate-DT catalog, please contact AQUA maintainers')
            sys.exit(1)

        return fs

    def avail(self, args):
        """
        Return the catalog available on the Github website
        
        Args:
            args (argparse.Namespace): arguments from the command line
        """
        fs = self._github_explore(repository=args.repository)
        available_catalog = [os.path.basename(x) for x in fs.ls(f"{CATPATH}/")]
        print('Available ClimateDT catalogs at are:')
        print(available_catalog)


    def _add_catalog_github(self, catalog, repository=None):
        """
        Add a catalog from a remote Github repository.
        Default repository is the Climate-DT repository
        DestinE-Climate-DT/Climate-DT-catalog
          
        Args:
            catalog (str): the catalog to be added
            repository (str): the repository from which to fetch the catalog, if None it uses the default
        """
        # recursive copy
        cdir = f'{self.configpath}/{CATPATH}/{catalog}'
        if not os.path.exists(cdir):
            fs = self._github_explore(repository)
            available_catalog = [os.path.basename(x) for x in fs.ls(f"{CATPATH}/")]
            if catalog not in available_catalog:
                self.logger.error('Cannot find on %s the requested catalog %s, available are %s',
                                  repository if repository else 'DestinE-Climate-DT/Climate-DT-catalog',
                                  catalog, available_catalog)
                sys.exit(1)

            source_dir = f"{CATPATH}/{catalog}"
            self.logger.info('Fetching remote catalog %s from github to %s', catalog, cdir)
            os.makedirs(cdir, exist_ok=True)
            fsspec_get_recursive(fs, source_dir, cdir)
            self.logger.info('Download complete!')
            self._set_catalog(catalog)
        else:
            self.logger.error("Catalog %s already installed in %s, please consider `aqua update`.",
                              catalog, cdir)
            sys.exit(1)

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
            for directory in BASIC_DIRECTORIES:
                self._copy_update_folder_file(os.path.join(self.aquapath, directory),
                                         os.path.join(self.configpath, directory),
                                         update=True)

            for directory in ['templates']:
                self._copy_update_folder_file(os.path.join(self.aquapath, '..', directory),
                                         os.path.join(self.configpath, directory),
                                         update=True)
                
    def _update_catalog(self, catalog):
        """Update a catalog by copying it if not installed in editable mode

        Args:
            catalog (str): the catalog to be updated
        """
        cdir = f'{self.configpath}/{CATPATH}/{catalog}'
        sdir = f'{self.aquapath}/{CATPATH}/{catalog}'
        if os.path.exists(cdir):
            if os.path.islink(cdir):
                self.logger.error('%s catalog has been installed in editable mode, no need to update', catalog)
                sys.exit(1)
            self.logger.info('Removing %s from %s', catalog, sdir)
            shutil.rmtree(cdir)
            self._add_catalog_github(catalog)
        else:
            self.logger.error('%s does not appear to be installed, please consider `aqua add`', catalog)
            sys.exit(1)


    def _set_catalog(self, catalog):
        """Modify the config-aqua.yaml with the proper catalog

        Args:
            catalog (str): the catalog to be set as the default in the config-aqua.yaml
        """

        self.logger.info('Setting catalog name to %s', catalog)
        cfg = load_yaml(self.configfile)
        if cfg['catalog'] is None:
            self.logger.debug('No catalog previously installed: setting catalog name to %s', catalog)
            cfg['catalog'] = catalog
        else:
            if catalog not in to_list(cfg['catalog']):
                self.logger.debug('Adding catalog %s to the existing list %s', catalog, cfg['catalog'])
                cfg['catalog'] = [catalog] + to_list(cfg['catalog'])
            else:
                if isinstance(cfg['catalog'], list):
                    other_catalogs = [x for x in cfg['catalog'] if x != catalog]
                    self.logger.debug('Catalog %s is already there, setting it as first entry before %s',
                                      catalog, other_catalogs)
                    cfg['catalog'] = [catalog] + other_catalogs
                else:
                    self.logger.debug('Catalog %s is already there, but is the only installed', catalog)
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
        cdir = f'{self.configpath}/{CATPATH}/{args.catalog}'
        print('Remove the AQUA catalog', args.catalog, 'from', cdir)
        if os.path.exists(cdir):
            if os.path.islink(cdir):
                os.unlink(cdir)
            else:
                shutil.rmtree(cdir)
            self._clean_catalog(args.catalog)
        else:
            self.logger.error('Catalog %s is not installed in %s, cannot remove it',
                              args.catalog, cdir)
            sys.exit(1)

    def _clean_catalog(self, catalog):
        """
        Remove catalog from the configuration file
        """

        cfg = load_yaml(self.configfile)
        if isinstance(cfg['catalog'], str):
            cfg['catalog'] = None
        else:
            cfg['catalog'].remove(catalog)
        self.logger.info('Catalog %s removed, catalogs %s are available', catalog, cfg['catalog'])
        dump_yaml(self.configfile, cfg)

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

    def _check(self, silent=False):
        """check installation"""

        checklevel = 'ERROR' if silent else self.loglevel
        try:
            self.configpath = ConfigPath(loglevel=checklevel).configdir
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
            _ = load_multi_yaml(folder_path=f'{self.configpath}/{kind}',
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

    def lra(self, args):
        """Run the Low Resolution Archive generator"""

        print('Running the Low Resolution Archive generator')
        lra_execute(args)

    def catgen(self, args):
        """Run the FDB catalog generator"""

        print("Running the catalog generator")
        catgen_execute(args)       

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


# Function to recursively copy files and directories
def fsspec_get_recursive(fs, src_dir, dest_dir):
    """
    Recursive function to download from a fsspec object

    Args:
        fs: fsspec filesystem object, as github instance
        src_dir (str): source directory
        dest_dir (str): target directory

    Returns:
        Remotely copy data from source to dest directory
    """
    data = fs.ls(src_dir)
    for item in data:
        relative_path = os.path.relpath(item, src_dir)
        dest_path = os.path.join(dest_dir, relative_path)

        if fs.isdir(item):
            # Create the directory in the destination
            os.makedirs(dest_path, exist_ok=True)
            # Recursively copy the contents of the directory
            fsspec_get_recursive(fs, item, dest_path)
        else:
            # Ensure the directory exists before copying the file
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            # Copy the file
            fs.get(item, dest_path)
