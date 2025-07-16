"""Module for aqua grid build"""
import os
import re
from glob import glob
from typing import Optional, Any
from cdo import Cdo
from smmregrid import GridInspector

from aqua.logger import log_configure, log_history
from aqua.util import ConfigPath, load_yaml, dump_yaml
from aqua.regridder.extragridbuilder import HealpixGridBuilder, RegularGridBuilder
from aqua.regridder.extragridbuilder import UnstructuredGridBuilder, CurvilinearGridBuilder

class GridBuilder():
    """
    Class to build automatically grids from data sources.
    Currently supports HEALPix grids and can be extended for other grid types.
    """
    GRIDTYPE_REGISTRY = {
        'HEALPix': HealpixGridBuilder,
        'Regular': RegularGridBuilder,
        'Unstructured': UnstructuredGridBuilder,
        'Curvilinear': CurvilinearGridBuilder,
        # Add more grid types here as needed
    }

    def __init__(
            self,
            outdir: str = '.',
            model_name: Optional[str] = None,
            original_resolution: Optional[str] = None,
            vert_coord: Optional[str] = None,
            loglevel: str = 'warning'
        ) -> None:
        """
        Initialize the GridBuilder with a reader instance.

        Args:
            outdir (str): The output directory for the grid files.
            model_name (str, optional): The name of the model, if different from the model argument.
            original_resolution (str, optional): The original resolution of the grid if using an interpolated source.
            vert_coord (str, optional): The vertical coordinate to consider for the grid build, to override the one detected by the GridInspector.
            loglevel (str, optional): The logging level for the logger. Defaults to 'warning'.
        """
        # store output directory
        self.outdir = outdir

        # store original resolution if necessary
        self.original_resolution = original_resolution

        # set model name
        self.model_name = model_name

        # loglevel
        self.logger = log_configure(log_level=loglevel, log_name='GridBuilder')
        self.loglevel = loglevel

        # vertical coordinates to consider for the grid build for the 3d case.
        self.vert_coord = vert_coord

        # get useful paths and CDO instance
        self.cdo = Cdo()
        self.configpath = ConfigPath().get_config_dir()
        self.gridpath = os.path.join(self.configpath, 'grids')
        self.gridfile = os.path.join(self.gridpath, f'{self.model_name}.yaml')


    def build(self, data, rebuild=False, version=None, verify=True):
        """
        Retrieve and build the grid data for all gridtypes available.
        
        Args:
            rebuild (bool): Whether to rebuild the grid file if it already exists. Defaults to False.
            fix (bool): Whether to fix the original source. Might be useful for some models. Defaults to False.
            version (int, optional): The version number to append to the grid file name. Defaults to None.
            verify (bool): Whether to verify the grid file after creation. Defaults to True.
        """
        gridtypes = GridInspector(data).get_gridtype()
        if not gridtypes:
            self.logger.error("No grid type detected, skipping grid build")
            self.logger.error("You can try to fix the source when calling the Reader() with the --fix flag")
            return
        self.logger.info("Build on %s gridtypes", len(gridtypes))
        for gridtype in gridtypes:
            self._build_gridtype(data, gridtype, rebuild=rebuild, version=version, verify=verify)
            
    def _build_gridtype(
        self,
        data: Any,
        gridtype: Any,
        rebuild: bool = False,
        version: Optional[int] = None,
        verify: bool = True
    ) -> None:
        """
        Build the grid data based on the detected grid type.
        """
        self.logger.info("Detected grid type: %s", gridtype)
        kind = gridtype.kind
        self.logger.info("Grid type is: %s", kind)

        # access the class registry to get the builder class appropriate for the gridtype
        BuilderClass = self.GRIDTYPE_REGISTRY.get(kind)
        if not BuilderClass:
            raise NotImplementedError(f"Grid type {kind} is not implemented yet")
        self.logger.debug("Builder class: %s", BuilderClass)

        # vertical coordinate detection
        vert_coord = self.vert_coord if self.vert_coord else gridtype.vertical_dim
        self.logger.info("Detected vertical coordinate: %s", vert_coord)

        # add vertical information to help CDO
        if vert_coord:
            data[vert_coord].attrs['axis'] = 'Z'

        # Initialize the builder
        builder = BuilderClass(
            vert_coord=vert_coord, model_name=self.model_name,
            original_resolution=self.original_resolution, loglevel=self.loglevel
        )

        # data reduction. Load the data into memory for convenience.
        data3d = builder.data_reduction(data, gridtype, vert_coord).load()

        # add history attribute, get metadata from the attributes
        exp = data3d['mask'].attrs.get('AQUA_exp', None)
        source = data3d['mask'].attrs.get('AQUA_source', None)
        model = data3d['mask'].attrs.get('AQUA_model', None)
        log_history(data3d, msg=f'Gridfile generated with GridBuilder from {model}_{exp}_{source}')

        # store the data in a temporary netcdf file
        filename_tmp = f"{self.model_name}_{exp}_{source}.nc"
        self.logger.debug("Saving tmp data in %s", filename_tmp)
        data3d.to_netcdf(filename_tmp)

        # select the 2D slice of the data and detect the mask type
        # TODO: this will likely not work for 3d unstructured grids.
        # An alternative might be to check if the NaN are changing along the vertical coordinate.
        data2d = builder.select_2d_slice(data3d, vert_coord)
        builder.detect_mask_type(data2d)
        self.logger.info("Masked type: %s", builder.masked)

        # get the basename and metadata for the grid file
        basename, metadata = builder.prepare(data3d)

        # create the base path for the grid file
        basepath = os.path.join(self.outdir, basename)

        # verify the existence of files and handle versioning
        existing_files = glob(f"{basepath}*.nc")
        self.logger.info("Existing files: %s", existing_files)
        if existing_files and version is None:
            pattern = re.compile(r"_v\d+\.nc$")
            check_version = [bool(pattern.search(file)) for file in existing_files]
            if any(check_version):
                raise ValueError(f"Versioned files already exist for {basepath}. Please specify a version")

        # check if version has been defined and set the filename accordingly
        if version is not None:
            basepath = f"{basepath}_v{version}"

        # check if the file already exists and clean it if needed
        filename = f"{basepath}.nc"
        if os.path.exists(filename):
            if rebuild:
                self.logger.warning('File %s already exists, removing it', filename)
                os.remove(filename)
            else:
                self.logger.error("File %s already exists, skipping", filename)
                return
        
        # write the grid file with the class specific method
        builder.write_gridfile(
            input_file=filename_tmp, output_file=filename, metadata=metadata
        )

        # cleanup
        self.logger.info('Removing temporary file %s', filename_tmp)
        os.remove(filename_tmp)

        # verify the creation of the weights
        if verify:
            builder.verify_weights(filename, metadata=metadata)


        # create the grid entry in the grid file
        grid_entry_name = builder.create_grid_entry_name(os.path.basename(basepath), vert_coord)
        grid_block = builder.create_grid_entry_block(gridtype, basepath, vert_coord, metadata=metadata)
        self.create_grid_entry(grid_entry_name, grid_block, rebuild=rebuild)
    
    def create_grid_entry(self, grid_entry_name, grid_block, rebuild=False):
        """
        Create a grid entry in the grid file for the given gridtype.

        Args:
            grid_entry_name (str): The name of the grid entry.
            grid_block (dict): The grid block to add to the grid file.
            rebuild (bool): Whether to rebuild the grid entry if it already exists. Defaults to False
        """

        self.logger.info("Grid entry name: %s", grid_entry_name)
        self.logger.info("Grid block: %s", grid_block)

        # if file do not exist, create it
        if not os.path.exists(self.gridfile):
            self.logger.info("Grid file %s does not exist, creating it", self.gridfile)
            final_block = {'grids': {grid_entry_name: grid_block}}
        # else, add the grid entry to the existing file
        else:
            self.logger.info("Grid file %s exists, adding the grid entry %s", self.gridfile, grid_entry_name)
            final_block = load_yaml(self.gridfile)
            if grid_entry_name in final_block.get('grids', {}) and not rebuild:
                self.logger.warning("Grid entry %s already exists in %s, skipping", grid_entry_name, self.gridfile)
                return
            final_block['grids'][grid_entry_name] = grid_block
        dump_yaml(self.gridfile, final_block)

