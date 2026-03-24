#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA files operations mixin
'''

import os
import sys
import shutil

from aqua.core.lock import SafeFileLock
from aqua.core.util import load_yaml, dump_yaml, load_multi_yaml


class FilesMixin:
    """Mixin for AQUA file operations"""

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

        filename = os.path.join(self.configpath, self.configfile)
        with SafeFileLock(filename + '.lock', loglevel=self.loglevel):
            cfg = load_yaml(filename)
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

            dump_yaml(filename, cfg)

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
