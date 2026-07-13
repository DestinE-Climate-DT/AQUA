import fnmatch
import os
import re

import requests

from aqua.core.configurer import ConfigPath
from aqua.core.logger import log_configure
from aqua.core.util import create_folder, load_multi_yaml, load_yaml, to_list


class GridDeployer:
    """
    Class to deploy the grids to the default grids path set in the config-aqua.yaml file.
    The deploy method takes the name of the source grid to be deployed,
    which can be either an exact name or a wildcard pattern.
    """

    def __init__(self, loglevel: str = "WARNING"):
        """
        Initialize the GridDeployer.

        Args:
            loglevel (str): the logging level to be used for the GridDeployer logger.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=self.loglevel, log_name="GridDeployer")
        self.configurer = ConfigPath(loglevel=self.loglevel)
        self.configpath = self.configurer.configdir
        self.configfile = os.path.join(self.configpath, "config-aqua.yaml")

    def deploy(self, source_grid_name: str):
        """
        Deploy the grid files to the default grids path set in the config-aqua.yaml file.
        This path is needed to avoid overwriting grid files that may be available based on
        the default path in the machine.yaml file of a catalog

        Args:
            source_grid_name (str): the name of the source grid to be deployed, can be
            either an exact name or a wildcard pattern.
        """
        # Get the config file and checks that the "paths" block or the "grids" path is set.
        filename = os.path.join(self.configpath, self.configfile)
        cfg = load_yaml(filename)
        if "paths" not in cfg or "grids" not in cfg["paths"]:
            self.logger.error(
                "Default grids path is not set in %s. Please set it with the aqua grids set command before deploying the grids.",  # noqa E501
                self.configfile,
            )
            raise ValueError("Default grids path is not set in the config file.")
        else:
            grids_path = cfg["paths"]["grids"]
            self.logger.info("Deploying grids to the grids path set in %s: %s", self.configfile, grids_path)

        # Load the grids yaml file to scan for the source_grid_name and deploy the matching grids
        _, grids_folder = self.configurer.get_reader_filenames()

        # Load the grids information from the auxiliary file.
        grids_dict = load_multi_yaml(folder_path=grids_folder, loglevel=self.loglevel)

        # Determine which grids to deploy
        if "*" in source_grid_name:
            # Wildcard matching
            grids_to_deploy = [name for name in grids_dict["grids"].keys() if fnmatch.fnmatch(name, source_grid_name)]

            if not grids_to_deploy:
                self.logger.error("No grids found matching pattern %s.", source_grid_name)
                return

            self.logger.info(
                "Found %d grids matching pattern %s: %s",
                len(grids_to_deploy),
                source_grid_name,
                ", ".join(grids_to_deploy),
            )
        elif source_grid_name in grids_dict["grids"]:
            # Exact match
            grids_to_deploy = [source_grid_name]
        else:
            self.logger.error("No exact match found for grid name %s.", source_grid_name)
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
