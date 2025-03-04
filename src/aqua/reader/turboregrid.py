"""New Regrid class independent from the Reader"""
import os
import xarray as xr
from smmregrid import CdoGenerate, Regridder, GridInspector
from aqua.logger import log_configure

class TurboRegrid():
    """Refactor of regridder class"""
    
    def __init__(self, cfg_grid_dict: dict,
                 src_grid_name: str,
                 data=None, loglevel="WARNING"):
        """
        The TurboRegrid constructor.

        Args:
            cfg_grid_dict (d ict): The dictionary containing the full AQUA grid configuration.
            src_grid_name (str): The name of the source grid in the AQUA convention.
            data (xarray.Dataset): The dataset to be regridded, to be provided in src_grid_path is missing.
            loglevel (str): The logging level.
        """

        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='TurboRegrid')

        # define basic attributes:
        self.cfg_grid_dict = cfg_grid_dict #full grid dictionary
        self.src_grid_name = src_grid_name # source grid name
        self.src_grid_dict = cfg_grid_dict['grids'][src_grid_name] # source grid dictionary

        self.regridder = {} # regridders for each vertical coordinate
        self.src_grid_area = None # source grid area
        self.dst_grid_area = None # destination grid area

        # check that a path exists
        self.src_grid_path = self.src_grid_dict.get('path', None)
        if not self.src_grid_path:
            if data is not None:
                #gridtype = GridInspector(data).get_grid_info()[0]
                self.src_grid_path = {'2d': data}
                # this has to be expanded. It should create a dictionary for the src_grid_path. 
            else:
                self.logger.warning("Grid file path not found in the configuration. Need sample data.")
                return

        # convert to dictionary if it is a single path. Assume is a 2d grid.
        if not isinstance(self.src_grid_path, dict):
            self.src_grid_path = {'2d': self.src_grid_path}
        
        # verify that all provided paths are valid
        if isinstance(self.src_grid_path, dict):
            for key in self.src_grid_path:
                if not os.path.exists(self.src_grid_path[key]):
                    raise FileNotFoundError(f"Grid file {self.src_grid_path} does not exist.")
    
        self.logger.info("Grid name: %s", self.src_grid_name)
        self.logger.info("Grid file path: %s", self.src_grid_path)

    def load_generate_areas(self, dst_grid_name=None, rebuild=False, reader_kwargs=None, **kwargs):
        """
        Load or generate regridding areas calling smmregrid
        """

        if dst_grid_name is not None:
            target_grid = self.cfg_grid_dict['grids'][dst_grid_name]
            source_grid = None
        else:
            target_grid = None
            source_grid = self.src_grid_path['2d']

        # get the cdo options from the configuration
        cdo_extra = self.src_grid_dict.get('cdo_extra', None)
        cdo_options = self.src_grid_dict.get('cdo_options', None)

        area_filename = f'tmp_areas.nc'
        #weights_filename = self._make_filename(dst_grid_name, self.grid_name, **reader_kwargs)
        #keep fallback option for filename when weights/areas block is not present in the configuration

        # check if weights already exist, if not, generate them
        if rebuild or os.path.exists(area_filename):
  
            # smmregrid call
            generator = CdoGenerate(source_grid=source_grid,
                            target_grid=target_grid,
                            cdo_download_path=None,
                            cdo_icon_grids=None,
                            cdo_extra=cdo_extra,
                            cdo_options=cdo_options,
                            cdo='cdo',
                            loglevel=self.loglevel)
            
            # generate and save the areas, depending on the src/tgt grid
            if target_grid is not None:
                grid_area = generator.areas(target=True)
            else:
                grid_area = generator.areas(target=False)
        
            grid_area.to_netcdf(area_filename)
        else:
            # load the weights
            grid_area = xr.open_dataset(area_filename)

        # assign to the object
        if target_grid is not None:
            self.src_grid_area = grid_area
        else:
            self.dst_grid_area = grid_area

        
    def load_generate_weights(self, dst_grid_name, rebuild=False, reader_kwargs=None, **kwargs):
        """
        Load or generate regridding weights calling smmregrid
        """

        # get the cdo options from the configuration 
        cdo_extra = self.src_grid_dict.get('cdo_extra', None)
        cdo_options = self.src_grid_dict.get('cdo_options', None)

        # loop over the vertical coordinates: 2d, 2dm, or any other
        for vertical_dim in self.src_grid_path:

            weights_filename = f'tmp_{vertical_dim}.nc'
            #weights_filename = self._make_filename(dst_grid_name, self.grid_name, **reader_kwargs)
            #keep fallback option for filename when weights/areas block is not present in the configuration

            # check if weights already exist, if not, generate them
            if rebuild or os.path.exists(weights_filename):
                
                # smmregrid call
                generator = CdoGenerate(source_grid=self.src_grid_path[vertical_dim],
                                target_grid=self.cfg_grid_dict['grids'][dst_grid_name],
                                cdo_download_path=None,
                                cdo_icon_grids=None,
                                cdo_extra=cdo_extra,
                                cdo_options=cdo_options,
                                cdo='cdo',
                                loglevel=self.loglevel)
                
                # define the vertical coordinate in the smmregrid world
                smm_vert_coord = None if vertical_dim in ['2d', '2dm'] else vertical_dim

                # generate and save the weights
                weights = generator.weights(method="con", vert_coord=smm_vert_coord, nproc=1)
                weights.to_netcdf(weights_filename)

            else:
                # load the weights
                weights = xr.open_dataset(weights_filename)

            # initialize the regridder
            self.regridder[vertical_dim] = Regridder(weights=weights)
        
    # def _configure_gridtype(self, data=None):
    #     """
    #     Configure the GridType object based on the grid_dict.
    #     """

    #     # 2d and 2dm should be passed to GridType and GridInspector as extra vert_dims
    #     vertical_dims = self.src_grid_dict.get('vert_coords', None)
    #     horizontal_dims = self.src_grid_dict.get('space_coords', None)
    #     grid_dims = to_list(horizontal_dims) + to_list(vertical_dims)

    #     if grid_dims: # check on self.grid_dict
    #         grid = GridType(dims=grid_dims)
    #     elif data:  # If not, we need to guess
    #         # It is the first element of a list because I can handle with smmregrid
    #         # multiple horizontal grids.
    #         grid = GridInspector(data).get_grid_info()[0]
    #     else:
    #         return None

    #     return grid




