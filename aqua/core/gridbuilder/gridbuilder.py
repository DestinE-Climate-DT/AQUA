"""Module for aqua grid build"""
import os
from typing import Optional, Any
from smmregrid import GridInspector

from aqua.core.logger import log_configure, log_history
from .extragridbuilder import HealpixGridBuilder, RegularGridBuilder
from .extragridbuilder import UnstructuredGridBuilder, CurvilinearGridBuilder
from .extragridbuilder import GaussianRegularGridBuilder
from .gridentrymanager import GridEntryManager


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
        'GaussianRegular': GaussianRegularGridBuilder,
        # Add more grid types here as needed
    }

    def __init__(
        self,
        outdir: str = '.',
        model_name: Optional[str] = None,
        grid_name: Optional[str] = None,
        original_resolution: Optional[str] = None,
        vert_coord: Optional[str] = None,
        loglevel: str = 'warning'
    ) -> None:
        """
        Initialize the GridBuilder with a reader instance.

        Args:
            outdir (str): The output directory for the grid files.
            model_name (str, optional): The name of the model, if different from the model argument.
            grid_name (str, optional): The name of the grid, to specify extra information in the grid file
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
        self.grid_name = grid_name

        # loglevel
        self.logger = log_configure(log_level=loglevel, log_name='GridBuilder')
        self.loglevel = loglevel

        # vertical coordinates to consider for the grid build for the 3d case.
        self.vert_coord = vert_coord

        # Initialize GridEntryManager to generate the grid file name and entry
        self.gem = GridEntryManager(
            model_name=self.model_name,
            grid_name=self.grid_name,
            original_resolution=self.original_resolution,
            loglevel=loglevel
        )

    def build(self, data, rebuild=False, version=None, verify=True, create_yaml=True):
        """
        Retrieve and build the grid data for all gridtypes available.

        Args:
            rebuild (bool): Whether to rebuild the grid file if it already exists. Defaults to False.
            fix (bool): Whether to fix the original source. Might be useful for some models. Defaults to False.
            version (int, optional): The version number to append to the grid file name. Defaults to None.
            verify (bool): Whether to verify the grid file after creation. Defaults to True.
            create_yaml (bool): Whether to create the grid entry in the grid file. Defaults to True.
        """
        gridtypes = GridInspector(data).get_gridtype()
        if not gridtypes:
            self.logger.error("No grid type detected, skipping grid build")
            self.logger.error("You can try to fix the source when calling the Reader() with the --fix flag")
            return
        self.logger.info("Build on %s gridtypes", len(gridtypes))
        for gridtype in gridtypes:
            self._build_gridtype(
                data, gridtype, rebuild=rebuild,
                version=version, verify=verify,
                create_yaml=create_yaml)

    def _build_gridtype(
        self,
        data: Any,
        gridtype: Any,
        rebuild: bool = False,
        version: Optional[int] = None,
        verify: bool = True,
        create_yaml: bool = True
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

        # Initialize the builder
        builder = BuilderClass(
            vert_coord=vert_coord, model_name=self.model_name, grid_name=self.grid_name,
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
        self.logger.info("Saving tmp data in %s", filename_tmp)

        # configure attributes for the grid file
        data3d = builder.clean_attributes(data3d)
        data3d.to_netcdf(filename_tmp)

        # select the 2D slice of the data and detect the mask type
        # TODO: this will likely not work for 3d unstructured grids.
        # An alternative might be to check if the NaN are changing along the vertical coordinate.
        data2d = builder.select_2d_slice(data3d, vert_coord)
        masked = builder.detect_mask_type(data2d)
        self.logger.info("Masked type: %s", builder.masked)

        # get the basename and metadata for the grid file
        # need to read grid property from 2d data
        metadata = builder.get_metadata(data2d)
        aquagrid = metadata['aquagrid']
        cdogrid = metadata['cdogrid']
        self.logger.debug("Grid metadata: %s", metadata)

        # Initialize GridEntryManager for this gridtype
        basename = self.gem.get_basename(aquagrid, cdogrid, masked, vert_coord)

        if not cdogrid or masked:
            self.logger.warning("No CDO grid detected, or mask, need physical file")

            # create the base path for the grid file
            basepath = self.gem.get_versioned_basepath(self.outdir, basename, version=version)

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
        else:
            self.logger.warning("CDO grid %s detected without mask, skipping physical file creation for %s", cdogrid, basename)
            filename = cdogrid

        # cleanup
        self.logger.info('Removing temporary file %s', filename_tmp)
        os.remove(filename_tmp)

        # verify the creation of the weights
        if verify:
            builder.verify_weights(filename, metadata=metadata)

        # create the grid entry in the grid file
        if create_yaml:
            gridfile = self.gem.get_gridfilename(cdogrid, kind)
            self.logger.info("Creating grid entry in %s", gridfile)
            grid_entry_name = self.gem.create_grid_entry_name(aquagrid, cdogrid, masked, vert_coord)
            cdo_options = metadata.get('cdo_options') if metadata else None
            remap_method = metadata.get('remap_method') if metadata else None
            grid_block = self.gem.create_grid_entry_block(
                filename, horizontal_dims=gridtype.horizontal_dims, cdo_options=cdo_options, remap_method=remap_method,
                vert_coord=vert_coord
            )
            self.gem.create_grid_entry(gridfile, grid_entry_name, grid_block, rebuild=rebuild)
