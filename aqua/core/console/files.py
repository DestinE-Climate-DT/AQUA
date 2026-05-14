#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA files operations mixin
"""

import fnmatch
import os
import re
import shutil
import sys

import requests

from aqua.core.lock import SafeFileLock
from aqua.core.util import create_folder, dump_yaml, load_multi_yaml, load_yaml, to_list


class FilesMixin:
    """Mixin for AQUA file operations"""

    def fixes_add(self, args):
        """Add a fix file

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        compatible = self._check_file(kind="fixes", file=args.file)
        if compatible:
            self._file_add(kind="fixes", file=args.file, link=args.editable)

    def grids_add(self, args):
        """Add a grid file

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        compatible = self._check_file(kind="grids", file=args.file)
        if compatible:
            self._file_add(kind="grids", file=args.file, link=args.editable)

    def grids_set(self, args):
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
        this utility allows to deploy the grids only if the grids_set method has been
        used to set the grids path in the config-aqua.yaml file.

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self._check()

        # Check if the default grids path is set in the config-aqua.yaml file
        # otherswise we cannot deploy the grids
        filename = os.path.join(self.configpath, self.configfile)
        cfg = load_yaml(filename)
        if "paths" not in cfg or "grids" not in cfg["paths"]:
            self.logger.error(
                "Default grids path is not set in %s. Please set it with the aqua grids set command before deploying the grids.",  # noqa E501
                self.configfile,
            )
            sys.exit(1)
        else:
            grids_path = cfg["paths"]["grids"]
            self.logger.info("Deploying grids to the grids path set in %s: %s", self.configfile, grids_path)

        # Load the grids yaml file to scan for the source_grid_name and deploy the matching grids
        _, grids_folder = self.configurer.get_reader_filenames()

        # Load the grids information from the auxiliary file.
        grids_dict = load_multi_yaml(folder_path=grids_folder, loglevel=self.loglevel)

        # Determine which grids to deploy
        if "*" in args.source_grid_name:
            # Wildcard matching
            grids_to_deploy = [name for name in grids_dict["grids"].keys() if fnmatch.fnmatch(name, args.source_grid_name)]

            if not grids_to_deploy:
                self.logger.error("No grids found matching pattern %s.", args.source_grid_name)
                return

            self.logger.info(
                "Found %d grids matching pattern %s: %s",
                len(grids_to_deploy),
                args.source_grid_name,
                ", ".join(grids_to_deploy),
            )
        elif args.source_grid_name in grids_dict["grids"]:
            # Exact match
            grids_to_deploy = [args.source_grid_name]
        else:
            self.logger.error("No exact match found for grid name %s.", args.source_grid_name)
            return

        # Deploy each grid
        for grid_name in grids_to_deploy:
            self.logger.info("Deploying grid %s", grid_name)
            single_dict = grids_dict["grids"][grid_name]

            # Extract the path(s) to deploy
            paths_to_deploy = self._grids_deploy_path(single_dict, source_grid_name=grid_name)
            for path in paths_to_deploy:
                file_name = path.split("/")[-1]  # Extract the grid file name
                grid_dir = "/".join(path.split("/")[:-1])  # Extract the parent
                self._download_grid(grid_dir=grid_dir, grid_name=file_name, targetdir=grids_path)

    def _grids_deploy_path(self, single_dict, source_grid_name=None):
        """
        Deploy the grid files to the default grids path set in the config-aqua.yaml file.

        Args:
            single_dict (dict): the dictionary containing the grid information, including the path
            source_grid_name (str): the name of the source grid to be deployed, used for logging purposes

        Returns:
            list: the list of paths to deploy
        """
        # Extract the path, since multiple paths may be available, the output is always a list.
        # With this method the download can be always handled as a for loop.
        grid_path = single_dict["path"]
        if isinstance(grid_path, str):
            self.logger.debug(f"Grid path: {grid_path}")
            grid_path = to_list(grid_path)  # Convert to list for consistency
        elif isinstance(grid_path, dict):
            self.logger.debug(f"Grid has multiple paths: {grid_path}")
            paths = []
            for key, path in grid_path.items():
                paths.append(path)
            grid_path = paths
        else:
            self.logger.error(f"Grid has an unexpected path format: {grid_path}")
            return []

        # Paths are in the format { SOME_VARIABLE }/path/to/grid, we need to extract the path after the variable.
        extracted_paths = []
        for path in grid_path:
            if "{{" in path and "}}" in path:
                extracted_path = path.split("}}")[1]  # Extract the part after the variable
                self.logger.debug(f"Extracted path for grid {source_grid_name}: {extracted_path}")
                extracted_paths.append(extracted_path)
            else:
                self.logger.error(
                    f"Grid {source_grid_name} path format is incorrect: {path}."
                    f"Expected format is {{ SOME_VARIABLE }}/path/to/grid."
                )  # noqa E501
                return []

        return extracted_paths

    def _download_grid(
        self, grid_dir: str, grid_name: str, targetdir: str, bucket: str = "https://lumidata.eu/465000454:aqua-grids/grids"
    ):
        """
        Download the grid from the bucket to the target directory.

        Args:
            grid_dir (str): Directory of the grid to be downloaded, relative to the bucket.
            grid_name (str): Name of the grid file to be downloaded.
            targetdir (str): Path to the main target directory where the grid will be deployed.
            bucket (str): URL of the bucket where the grids are stored.
        """
        url = f"{bucket}/{grid_dir}/{grid_name}"
        # Remove double // if present in the url
        url = re.sub(r"(?<!:)//+", "/", url)
        self.logger.debug(f"Downloading grid from {url}.")

        # HACK: no idea why the normalized is needed
        normalized_grid_dir = grid_dir.lstrip("/\\")
        final_folder = os.path.join(targetdir, normalized_grid_dir)

        create_folder(final_folder)
        final_path = os.path.join(final_folder, grid_name)
        if os.path.exists(final_path):
            self.logger.warning(f"Grid {grid_name} already exists at {final_path}. Skipping download.")
            return
        else:
            self.logger.debug(f"Grid {grid_name} does not exist at {final_path}. Downloading grid.")

        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(final_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        self.logger.info(f"Grid {grid_name} downloaded successfully to {final_path}.")

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
        """Add a personalized file to the fixes/grids folder

        Args:
            kind (str): the kind of file to be added, either 'fixes' or 'grids'
            file (str): the file to be added
        """

        self._check()
        kind = args.command
        file = os.path.basename(args.file)
        pathfile = f"{self.configpath}/{kind}/{file}"
        if os.path.exists(pathfile):
            self.logger.info("Removing %s", pathfile)
            if os.path.islink(pathfile):
                os.unlink(pathfile)
            else:
                os.remove(pathfile)
        else:
            self.logger.error("%s file %s is not installed in AQUA, cannot remove it", kind, file)
            sys.exit(1)

    def _check_file(self, kind, file=None):
        """
        Check if a new file can be merged with AQUA load_multi_yaml()
        It works also without a new file to check that the existing files are compatible

        Args:
            kind (str): the kind of file to be added, either 'fixes' or 'grids'
            file (str): the file to be added
        """
        if kind not in ["fixes", "grids"]:
            raise ValueError("Kind must be either fixes or grids")

        self._check()
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
