"""
This module is used to manage the grid entry in the grid file.
It is used to create the grid entry name, block, and write the grid entry to the grid file.
It also control the filename of the grid file.
"""

import os
import glob
import re
from typing import Optional, Any, Dict
from aqua.util import load_yaml, dump_yaml
from aqua.util import ConfigPath
from aqua.logger import log_configure

class GridEntryManager:
    """
    Class to manage grid entry naming, block creation, and YAML file writing.
    It also control the filename of the grid file.
    Handles all context for naming and entry logic.
    """
    def __init__(
        self,
        model_name: Optional[str] = None,
        grid_name: Optional[str] = None,
        original_resolution: Optional[str] = None,
        vert_coord: Optional[str] = None,
        loglevel: str = 'warning'
    ):
        # get useful paths
        self.configpath = ConfigPath().get_config_dir()
        self.gridpath = os.path.join(self.configpath, 'grids')

        # try to keep model and grid names as lowercase
        self.model_name = model_name.lower() if model_name else None
        self.grid_name = grid_name.lower() if grid_name else None
        self.original_resolution = original_resolution.lower() if original_resolution else None
        self.vert_coord = vert_coord

        # set log level and logger
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='GridEntryManager')

    def get_basename(
        self, aquagrid: str, cdogrid: Optional[str] = None, masked: Optional[str] = None) -> str:
        """
        Get the basename for the grid type based on the context and aquagrid name.
        """
        
        # if masked is not set
        if not masked:
            if cdogrid:
                return aquagrid
            if aquagrid and self.model_name:
                return f"{self.model_name}_{aquagrid}"          
            raise ValueError("Grid name or model name are not set, please provide at least a grid name")

        # masking
        basename = f"{self.model_name}"
        if self.original_resolution:
            basename += f"_{self.original_resolution}"
        basename += f"_{aquagrid}"
        if self.vert_coord:
            basename += f"_3d_{self.vert_coord}"
        else:
            basename += "_2d"
        return basename

    def create_grid_entry_name(self, aquagrid: str, cdogrid: Optional[str] = None, masked: Optional[str] = None) -> str:
        """Create a grid entry name based on the grid type and vertical coordinate."""
        return self.get_basename(aquagrid, cdogrid, masked).replace('_', '-')

    def create_grid_entry_block(
        self,
        path: str,
        horizontal_dims: Optional[str] = None,
        cdo_options: Optional[str] = None,
        remap_method: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a grid entry block for the gridtype, with only cdo_options and remap_method."""
        grid_block = {
            'path': f"{path}",
        }
        if horizontal_dims:
            grid_block['space_coord'] = horizontal_dims
        if self.vert_coord:
            grid_block['vert_coord'] = self.vert_coord
            grid_block['path'] = {self.vert_coord: f"{path}"}
        if cdo_options:
            grid_block['cdo_options'] = cdo_options
        if remap_method and remap_method != 'con':
            grid_block['remap_method'] = remap_method
        return grid_block

    def get_gridfilename(self, cdogrid, gridkind) -> str:
        """
        Get the grid filename based on the grid kind.
        """
        if cdogrid:
            gridfilename = f'{gridkind.lower()}.yaml'
        else:
            gridfilename = f'{self.model_name}-{gridkind.lower()}.yaml'
        return os.path.join(self.gridpath, gridfilename)

    def get_versioned_basepath(self, outdir: str, basename: str, version: Optional[int] = None) -> str:
        """
        Returns the correct basepath (without .nc) for the grid file, handling versioning logic.
        Raises ValueError if versioning rules are violated (e.g., versioned files exist but no version specified).
        Does NOT remove or create any files.
        """
        basepath = os.path.join(outdir, basename)
        existing_files = glob.glob(f"{basepath}*.nc")
        if existing_files and version is None:
            pattern = re.compile(r"_v\d+\.nc$")
            check_version = [bool(pattern.search(file)) for file in existing_files]
            if any(check_version):
                raise ValueError(f"Versioned files already exist for {basepath}. Please specify a version")
        if version is not None:
            basepath = f"{basepath}_v{version}"
        return basepath

    def create_grid_entry(self, gridfile, grid_entry_name, grid_block, rebuild=False):
        """
        Create or update a grid entry in the grid YAML file.
        """
        if self.logger:
            self.logger.info("Grid entry name: %s", grid_entry_name)
            self.logger.info("Grid block: %s", grid_block)

        if not os.path.exists(gridfile):
            if self.logger:
                self.logger.info("Grid file %s does not exist, creating it", gridfile)
            final_block = {'grids': {grid_entry_name: grid_block}}
        else:
            if self.logger:
                self.logger.info("Grid file %s exists, adding the grid entry %s", gridfile, grid_entry_name)
            final_block = load_yaml(gridfile)
            if grid_entry_name in final_block.get('grids', {}) and not rebuild:
                if self.logger:
                    self.logger.warning("Grid entry %s already exists in %s, skipping", grid_entry_name, gridfile)
                return
            final_block['grids'][grid_entry_name] = grid_block
        dump_yaml(gridfile, final_block) 