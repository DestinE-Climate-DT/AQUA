#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA catalog operations mixin
'''

import os
import sys
import shutil
from urllib.error import HTTPError

import fsspec

from aqua.core.lock import SafeFileLock
from aqua.core.util import load_yaml, dump_yaml
from aqua.core.util.util import to_list, HiddenPrints
from aqua.core.reader.catalog import show_catalog_content as print_catalog

# folder used for reading/storing catalogs
CATPATH = 'catalogs'


class CatalogMixin:
    """Mixin for AQUA catalog operations"""

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
            self._fsspec_get_recursive(fs, source_dir, cdir)
            self.logger.info('Download complete!')
            self._set_catalog(catalog)
        else:
            self.logger.error("Catalog %s already installed in %s, please consider `aqua update`.",
                              catalog, cdir)
            sys.exit(1)

    def _update_catalog(self, catalog):
        """Update a catalog by copying it if not installed in editable mode

        Args:
            catalog (str): the catalog to be updated
        """
        cdir = f'{self.configpath}/{CATPATH}/{catalog}'
        if os.path.exists(cdir):
            if os.path.islink(cdir):
                self.logger.error('%s catalog has been installed in editable mode, no need to update', catalog)
                sys.exit(1)
            self.logger.info('Removing %s from %s', catalog, cdir)
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
        with SafeFileLock(self.configfile + '.lock', loglevel=self.loglevel):
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

        with SafeFileLock(self.configfile + '.lock', loglevel=self.loglevel):
            cfg = load_yaml(self.configfile)
            if isinstance(cfg['catalog'], str):
                cfg['catalog'] = None
            else:
                cfg['catalog'].remove(catalog)
            self.logger.info('Catalog %s removed, catalogs %s are available', catalog, cfg['catalog'])
            dump_yaml(self.configfile, cfg)

    @staticmethod
    def _fsspec_get_recursive(fs, src_dir, dest_dir):
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
                CatalogMixin._fsspec_get_recursive(fs, item, dest_path)
            else:
                # Ensure the directory exists before copying the file
                os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                # Copy the file
                fs.get(item, dest_path)
