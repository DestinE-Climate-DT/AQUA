import os
from typing import Optional, Any, Dict
from aqua.util import load_yaml, dump_yaml

class GridEntryManager:
    """
    Class to manage grid entry naming, block creation, and YAML file writing.
    Handles all context for naming and entry logic.
    """
    def __init__(
        self,
        gridpath: str,
        gridkind: str,
        model_name: Optional[str] = None,
        grid_name: Optional[str] = None,
        original_resolution: Optional[str] = None,
        vert_coord: Optional[str] = None,
        masked: Optional[str] = None,
        logger=None
    ):
        self.gridpath = gridpath
        self.gridkind = gridkind
        self.model_name = model_name
        self.grid_name = grid_name
        self.original_resolution = original_resolution
        self.vert_coord = vert_coord
        self.masked = masked
        self.logger = logger

    def get_basename(self, aquagrid: str) -> str:
        """
        Get the basename for the grid type based on the context and aquagrid name.
        """
        # no oceanic masking: name is defined by the AQUA grid name or grid_name param
        if self.masked is None:
            if self.grid_name:
                if self.logger:
                    self.logger.error(self.grid_name)
                basename = f"{self.grid_name}"
            else:
                basename = f"{aquagrid}"
        # land masking: not supported yet
        elif self.masked == "land":
            raise NotImplementedError("Land masking is not implemented yet!")
        # oceanic masking: improvement needed. TODO: see original logic
        elif self.masked == "oce":
            if self.grid_name:
                basename = f"{self.grid_name}"
            else:
                basename = f"{self.model_name}"
            if self.original_resolution:
                basename += f"-{self.original_resolution}"
            if aquagrid != self.model_name:
                basename += f"_{aquagrid}"
            basename += "_oce"
            if self.vert_coord:
                basename += f"_{self.vert_coord}"
        return basename

    def create_grid_entry_name(self, aquagrid: str) -> str:
        """Create a grid entry name based on the grid type and vertical coordinate."""
        name = self.get_basename(aquagrid)
        vert_coord = self.vert_coord
        if vert_coord is not None:
            name = name.replace(vert_coord, '3d')
        name = name.replace('_oce_', '_').replace('_', '-')
        return name

    def create_grid_entry_block(
        self,
        gridtype: Any,
        basepath: str,
        cdo_options: Optional[str] = None,
        remap_method: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a grid entry block for the gridtype, with only cdo_options and remap_method."""
        vert_coord = self.vert_coord
        grid_block = {
            'path': f"{basepath}.nc",
            'space_coord': gridtype.horizontal_dims,
        }
        if vert_coord:
            grid_block['vert_coord'] = vert_coord
            grid_block['path'] = {vert_coord: f"{basepath}.nc"}
        if cdo_options:
            grid_block['cdo_options'] = cdo_options
        if remap_method and remap_method != 'con':
            grid_block['remap_method'] = remap_method
        return grid_block

    def create_grid_entry(self, model_name, grid_entry_name, grid_block, rebuild=False):
        """
        Create or update a grid entry in the grid YAML file.
        """
        gridkind = self.gridkind
        if self.logger:
            self.logger.info("Grid entry name: %s", grid_entry_name)
            self.logger.info("Grid block: %s", grid_block)

        if model_name is None:
            gridfilename = f'{gridkind}.yaml'
        else:
            gridfilename = f'{model_name}.yaml'
        gridfile = os.path.join(self.gridpath, gridfilename)

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