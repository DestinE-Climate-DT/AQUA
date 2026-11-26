#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA files operations mixin
"""

import os
import shutil
import sys

from aqua.core.gridbuilder import GridDeployer
from aqua.core.lock import SafeFileLock
from aqua.core.util import dump_yaml, load_multi_yaml, load_yaml


class FilesMixin:
    """Mixin for AQUA file operations"""

    def file_add(self, args):
        """Add a grids, fixes or styles file, as given by the noun of the command

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        if self._check_file(kind=args.command, file=args.file):
            self._file_add(kind=args.command, file=args.file, link=args.editable)

    def file_list(self, args):
        """List the installed grids, fixes or styles files, as given by the noun of the command

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        folder = os.path.join(self.configpath, args.command)
        print(f"AQUA current installed {args.command} in {folder}:")
        self._list_folder(folder)

    def grids_path(self, args):
        """
        Set the grids (and concurrently the weights and areas) paths in the config-aqua.yaml
        This will override the grids paths defined in the individual catalogs

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()
        grids_path = args.path + "/grids"
        areas_path = args.path + "/areas"
        weights_path = args.path + "/weights"

        self.logger.info(
            "Setting grids path to %s, weights path to %s and areas path to %s", grids_path, weights_path, areas_path
        )

        # Check if the paths exist and if not create them
        for path in [grids_path, areas_path, weights_path]:
            if not os.path.exists(path):
                self.logger.info("Creating path %s", path)
                os.makedirs(path, exist_ok=True)

        filename = os.path.join(self.configpath, self.configfile)
        with SafeFileLock(filename + ".lock", loglevel=self.loglevel):
            cfg = load_yaml(filename)
            path_dict = {"paths": {"grids": grids_path, "areas": areas_path, "weights": weights_path}}

            # If the paths already exist, we just update them
            if "paths" in cfg:
                self.logger.info("Updating existing paths in %s", self.configfile)
                cfg["paths"].update(path_dict["paths"])
            else:
                self.logger.info("Adding new paths to %s", self.configfile)
                cfg["paths"] = path_dict["paths"]

            dump_yaml(filename, cfg)

    def grids_deploy(self, args):
        """
        Deploy the grids selected by the user, which can specify a source_grid_name.

        Since by default every catalog has is own grids folder,
        this utility allows to deploy the grids only if the grids_path method has been
        used to set the grids path in the config-aqua.yaml file.

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        grid_deployer = GridDeployer(loglevel=self.loglevel)
        grid_deployer.deploy(source_grid_name=args.source_grid_name)

    def _file_add(self, kind, file, link=False):
        """Add a personalized file to the fixes/grids/styles folder

        Args:
            kind (str): the kind of file to be added: 'fixes', 'grids' or 'styles'
            file (str): the file to be added
            link (bool): whether to add the file as a link or not
        """

        file = os.path.abspath(file)
        self._check()
        basefile = os.path.basename(file)
        pathfile = f"{self.configpath}/{kind}/{basefile}"
        if not os.path.exists(pathfile):
            if link:
                self.logger.info("Linking %s to %s", file, pathfile)
                os.symlink(file, pathfile)
            else:
                self.logger.info("Installing %s to %s", file, pathfile)
                shutil.copy(file, pathfile)
        else:
            self.logger.error("%s for file %s already installed, or a file with the same name exists", kind, file)
            sys.exit(1)

    def remove_file(self, args):
        """Remove a grids, fixes or styles file, as given by the noun of the command

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        self._check()
        kind = args.command
        file = os.path.basename(args.file)
        pathfile = f"{self.configpath}/{kind}/{file}"
        # lexists: an editable file whose source is gone is a dangling link, still to be removed
        if os.path.lexists(pathfile):
            self._remove_target(pathfile)
        else:
            self.logger.error("%s file %s is not installed in AQUA, cannot remove it", kind, file)
            sys.exit(1)

    def _check_file(self, kind, file=None):
        """
        Check if a new file can be merged with AQUA load_multi_yaml()
        It works also without a new file to check that the existing files are compatible.
        Styles are matplotlib style files, not YAML: only their extension is checked.

        Args:
            kind (str): the kind of file to be added: 'fixes', 'grids' or 'styles'
            file (str): the file to be added
        """
        if kind not in ["fixes", "grids", "styles"]:
            raise ValueError("Kind must be fixes, grids or styles")

        self._check()
        if kind == "styles":
            if file is not None and not (os.path.isfile(file) and file.endswith(".mplstyle")):
                self.logger.error("%s is not a valid matplotlib style file (.mplstyle)!", file)
                return False
            return True
        try:
            _ = (
                load_multi_yaml(folder_path=f"{self.configpath}/{kind}", filenames=[file])
                if file is not None
                else load_multi_yaml(folder_path=f"{self.configpath}/{kind}")
            )

            if file is not None:
                self.logger.debug("File %s is compatible with the existing files in %s", file, kind)

            return True
        except Exception as e:
            if file is not None:
                if not os.path.exists(file):
                    self.logger.error("%s is not a valid file!", file)
                else:
                    self.logger.error("It is not possible to add the file %s to the %s folder", file, kind)
            else:
                self.logger.error("Existing files in the %s folder are not compatible", kind)
            self.logger.error(e)
            return False
