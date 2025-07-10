"""Module for aqua grid build"""
import os
import re
from glob import glob
from typing import Optional, Dict, Any
import xarray as xr
from cdo import Cdo
from smmregrid import GridInspector
from aqua import Reader
from aqua.regridder.regridder_util import detect_grid
from aqua.logger import log_configure, log_history
from aqua.util import ConfigPath, load_yaml, dump_yaml
from aqua.regridder.gridtypebuilder import HealpixGridTypeBuilder, RegularGridTypeBuilder
from aqua.regridder.gridtypebuilder import UnstructuredGridTypeBuilder, CurvilinearGridTypeBuilder


class GridBuilder():
    """
    Class to build automatically grids from data sources.
    Currently supports HEALPix grids and can be extended for other grid types.
    """
    GRIDTYPE_REGISTRY = {
        'Healpix': HealpixGridTypeBuilder,
        'Regular': RegularGridTypeBuilder,
        'Unstructured': UnstructuredGridTypeBuilder,
        'Curvilinear': CurvilinearGridTypeBuilder,
        # Add more grid types here as needed
    }

    def __init__(
            self,
            model: str,
            exp: str,
            source: str,
            outdir: str = '.',
            model_name: Optional[str] = None,
            original_resolution: Optional[str] = None,
            loglevel: str = 'warning'
        ) -> None:
        """
        Initialize the GridBuilder with a reader instance.

        Args:
            model (str): The model name.
            exp (str): The experiment name.
            source (str): The source of the data.
            outdir (str): The output directory for the grid files.
            model_name (str, optional): The name of the model, if different from the model argument.
            original_resolution (str, optional): The original resolution of the grid if using an interpolated source.
            loglevel (str): The logging level for the logger. Defaults to 'warning'.
        """
        self.model = model
        self.exp = exp
        self.source = source
        self.outdir = outdir

        # If model_name is not provided, use the model name in lowercase
        self.model_name = model_name.lower() if model_name else model
        self.original_resolution = original_resolution
        
        # loglevel
        self.logger = log_configure(log_level=loglevel, log_name='GridBuilder')
        self.loglevel = loglevel

        # TODO: we need to find a nicer way to handle vertical coordinates
        self.reasonable_vert_coords = ['depth_full', 'depth_half', 'level']
       
        self.cdo = Cdo()
        self.configpath = ConfigPath().get_config_dir()
        self.gridpath = os.path.join(self.configpath, 'grids')
        self.gridfile = os.path.join(self.gridpath, f'{self.model_name}.yaml')


    def retrieve(self, fix=False):
        """
        Retrieve the grid data based on the model, experiment, and source.

        Returns:
            xarray.Dataset: The retrieved grid data.
        """
        reader = Reader(model=self.model, exp=self.exp, source=self.source, loglevel=self.loglevel,
                        areas=False, fix=fix)
        return reader.retrieve()
    
   
    def build(self, rebuild=False, fix=False, version=None):
        """
        Retrieve and build the grid data for all gridtypes available.
        
        Args:
            rebuild (bool): Whether to rebuild the grid file if it already exists. Defaults to False.
            fix (bool): Whether to fix the original source. Might be useful for some models. Defaults to False.
            version (int, optional): The version number to append to the grid file name. Defaults to None.
        """
        data = self.retrieve(fix=fix)
        gridtypes = GridInspector(data).get_gridtype()
        if not gridtypes:
            self.logger.error("No grid type detected, skipping grid build")
            self.logger.error("You can try to fix the source when calling the Reader() with the --fix flag")
            return
        for gridtype in gridtypes:
            self._build_gridtype(data, gridtype, rebuild=rebuild, version=version)
            
    def _build_gridtype(
        self,
        data: Any,
        gridtype: Any,
        rebuild: bool = False,
        version: Optional[int] = None
    ) -> None:
        """
        Build the grid data based on the detected grid type.
        """
        self.logger.info("Detected grid type: %s", gridtype)
        kind = detect_grid(data)
        self.logger.info("Grid type is: %s", kind)

        builder_cls = self.GRIDTYPE_REGISTRY.get(kind)
        if not builder_cls:
            raise NotImplementedError(f"Grid type {kind} is not implemented yet")
        self.logger.debug("Builder class: %s", builder_cls)

        # vertical coordinate detection
        vert_coord_candidates = list(set(self.reasonable_vert_coords) & set(gridtype.other_dims))
        vert_coord = vert_coord_candidates[0] if vert_coord_candidates else None
        # add vertical information to help CDO
        if vert_coord:
            self.logger.info("Detected vertical coordinate: %s", vert_coord)
            data[vert_coord].attrs['axis'] = 'Z'

        # Use the builder's data_reduction method
        builder = builder_cls(data, None, vert_coord, self.model_name, self.original_resolution, self.loglevel)
        data3d = builder.data_reduction(data, gridtype, vert_coord).load()
        
        # add history attribute
        log_history(data, msg=f'Gridfile generated with GridBuilder from {self.model}_{self.exp}_{self.source}')

        # store the data in a temporary netcdf file
        filename_tmp = f"{self.model_name}_{self.exp}_{self.source}.nc"
        self.logger.debug("Saving tmp data in %s", filename_tmp)
        data3d.to_netcdf(filename_tmp)

        # select the 2D slice of the data and detect the mask type
        data2d = builder.select_2d_slice(data3d, vert_coord)
        masked = builder.detect_mask_type(data2d)
        self.logger.info("Masked type: %s", masked)

        # get the basename and metadata for the grid file
        basename, metadata = builder.prepare()

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

        filename = f"{basepath}.nc"
        if os.path.exists(filename):
            if rebuild:
                self.logger.warning('File %s already exists, removing it', filename)
                os.remove(filename)
            else:
                self.logger.error("File %s already exists, skipping", filename)
                return
        
        builder.write_gridfile(filename_tmp, filename, metadata, self.cdo, logger=self.logger)
        self.logger.info('Removing temporary file %s', filename_tmp)
        os.remove(filename_tmp)

        try:
            self.logger.info("Verifying the creation of the weights from the grid file")
            self.cdo.gencon("r180x90", input=filename, options="-f nc  --force")
            self.logger.info("Weights generated successfully for %s!!! This grid file is approved for AQUA, take a bow!", filename)
        except Exception as e:
            self.logger.error("Error generating weights, something is wrong with the obtianed file: %s", e)
            raise

        # create the grid entry in the grid file
        self.create_grid_entry(gridtype, basepath, vert_coord=vert_coord, rebuild=rebuild)

    def create_grid_entry(self, gridtype, basepath, vert_coord=None, rebuild=False):
        """
        Create a grid entry in the grid file for the given gridtype.

        Args:
            gridtype (GridInspector): The grid type object containing grid information.
            basepath (str): The base path for the grid file.
            vert_coord (str, optional): The vertical coordinate if applicable.
            rebuild (bool): Whether to rebuild the grid entry if it already exists. Defaults to False
        """

        grid_entry_name = self.create_grid_entry_name(os.path.basename(basepath), vert_coord)
        grid_block = self._create_grid_entry_block(gridtype, basepath, vert_coord)

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
            
        
    @staticmethod
    def _create_grid_entry_block(
        gridtype: Any,
        basepath: str,
        vert_coord: Optional[str] = None
    ) -> Dict[str, Any]:
        """ Create a grid entry for the gridtype."""
        
        grid_block = {
            'cdo_options': '--force',
            'path': f"{basepath}.nc",
            'space_coord': gridtype.horizontal_dims,
        }
        if vert_coord:
            grid_block['vert_coord'] = vert_coord
            grid_block['path'] = {vert_coord: f"{basepath}.nc"}
        return grid_block

    @staticmethod
    def create_grid_entry_name(
        name: str,
        vert_coord: Optional[str]
    ) -> str:
        """ Create a grid entry name based on the grid type and vertical coordinate."""

        if vert_coord is not None:
            name = name.replace(vert_coord, '3d')  # Replace _hpz10 with -hpz7
    
        return name.replace('_oce_', '_').replace('_', '-')

    @staticmethod
    def _data_reduction(
        data: xr.Dataset,
        gridtype: Any,
        vert_coord: Optional[str] = None
    ) -> xr.Dataset:
        """
        Reduce the data to a single variable and time step.

        Args:
            data (xarray.Dataset): The dataset containing grid data.
            gridtype (GridInspector): The grid object containing GridType info.

        Returns:
            xarray.DataArray: The reduced data.
        """
        # extract first var fro GridType, and guess time dimension from there
        var = next(iter(gridtype.variables))

        # this is somehow needed because GridInspector is not able to detect the time dimension
        timedim = gridtype.time_dims[0] if gridtype.time_dims else None
        if timedim:
            data = data.isel({timedim: 0}, drop=True)

        # keep bounds if present
        if gridtype.bounds:
            load_vars = [var] + gridtype.bounds
        else:
            load_vars = [var]
        data = data[load_vars]
        data = data.rename({var: 'mask'})  # rename to mask for consistency

        if vert_coord and f"idx_{vert_coord}" in data.coords:
            data = data.drop_vars(f"idx_{vert_coord}")

        # set the mask variable to 1 where data is not null
        data['mask'] = xr.where(data['mask'].isnull(), data['mask'], 1, keep_attrs=True)


        return data
    

    def _detect_mask_type(self, data: xr.Dataset) -> Optional[str]:
        """ Detect the type of mask based on the data and grid kind. """

        nan_count = float(data['mask'].isnull().sum().values) / data['mask'].size
        self.logger.debug("NaN count: %s", nan_count)
        if nan_count == 0:
            return None
        if 0 < nan_count < 0.5:
            return "oce"
        if nan_count >= 0.5:
            return "land"
        
        raise ValueError(f"Unexpected nan count {nan_count}")
    


