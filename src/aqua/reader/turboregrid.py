"""New Regrid class independent from the Reader"""
import os
import re
import xarray as xr
from smmregrid import CdoGenerate, Regridder
from smmregrid.util import CDO_GRID_PATTERNS
from aqua.logger import log_configure

# parameters which will affect the weights and areas name
DEFAULT_WEIGHTS_AREAS_PARAMETERS = ['zoom']

class TurboRegrid():
    """Refactor of regridder class"""

    def __init__(self, cfg_grid_dict: dict,
                 src_grid_name: str,
                 data: xr.Dataset = None,
                 loglevel: str = "WARNING"):
        """
        The TurboRegrid constructor.

        Args:
            cfg_grid_dict (dict): The dictionary containing the full AQUA grid configuration.
            src_grid_name (str): The name of the source grid in the AQUA convention.
            data (xarray.Dataset, optional): The dataset to be regridded, 
                                   to be provided in src_grid_path is missing.
            loglevel (str): The logging level.
        """

        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='TurboRegrid')

        # define basic attributes:
        self.cfg_grid_dict = cfg_grid_dict #full grid dictionary
        self.src_grid_name = src_grid_name # source grid name
        self.src_grid_dict = cfg_grid_dict['grids'].get(src_grid_name) # source grid dictionary
        if not self.src_grid_dict:
            raise ValueError(f"Source grid '{src_grid_name}' not found in the configuration.")
        self.src_grid_dict = self._normalize_grid_dict(self.src_grid_dict)


        self.regridder = {} # regridders for each vertical coordinate
        self.src_grid_area = None # source grid area
        self.dst_grid_area = None # destination grid area

        self.src_grid_path = self.src_grid_dict.get('path')
        self._normalize_grid_path(self.src_grid_path, data)


        self.logger.info("Grid name: %s", self.src_grid_name)
        self.logger.info("Grid file path: %s", self.src_grid_path)

    @staticmethod
    def _is_cdo_grid(grid):
        """
        Check if the grid name is a valid CDO grid name.
        Imported from smmregrid
        """

        # Check if the string matches any of the grid patterns
        for key, pattern in CDO_GRID_PATTERNS.items():
            if pattern.match(grid):
                return True
        return False
    
    def _normalize_grid_dict(self, grid):
        """
        Normalize the grid dictionary to a dictionary with the 2d key.
        """

        # grid dict is a string: this is the case of a CDO grid name
        if isinstance(grid, str):
            if self._is_cdo_grid(grid):
                self.logger.info("Grid definition %s is a valid CDO grid name.", grid)
                return {"path": {"2d": grid}}
            raise ValueError(f"Grid '{grid}' is not a valid CDO grid name.")
        if isinstance(grid, dict):
            return grid
        raise ValueError(f"Grid definition '{grid}' is not a valid type")


    def _normalize_grid_path(self, path, data=None):
        """
        Normalize the grid path to a dictionary with the 2d key.
        3 cases handled: 
            - a dictionary with a path string, a CDO grid name or a file path
            - a dictionary with a dictionary of path, one for each vertical coordinate
            - a dictionary without path with data provided, to get info from the dataset
        
        Returns:
            dict: The normalized grid path dictionary. "2d" key is mandatory.
        """

        # case path is a string: check if it is a valid CDO grid name or a file path
        if isinstance(path, str):
            if self._is_cdo_grid(path):
                self.logger.info("Grid path %s is a valid CDO grid name.", path)
                return {"2d": path}
            if self._check_existing_file(path):
                self.logger.info("Grid path %s is a valid file path.", path)
                return {"2d": path}
            raise FileNotFoundError(f"Grid file '{path}' does not exist.")

        # case path is a dictionary: check if the values are valid file paths
        # (could extend to CDO names?)
        if isinstance(path, dict):
            for _, value in path.items():
                if not (self._is_cdo_grid(value) or self._check_existing_file(value)):
                    raise ValueError(f"Grid path '{value}' is not a valid CDO grid name nor a file path.")
            self.logger.info("Grid path %s is a valid dictionary of file paths.", path)
            return path

        # grid path is None: check if data is provided to extract information for CDO
        if path is None:
            if data is not None:
                self.logger.info("Using provided dataset for grid path.")
                return {"2d": data}
            raise ValueError("Grid path missing in config and no sample data provided.")

        raise ValueError(f"Grid path '{path}' is not a valid type.")


    def _check_existing_file(self, filename):
        """
        Checks if an area/weights file exists and is valid.
        Return true if the file has some records.
        """
        return os.path.exists(filename) and os.path.getsize(filename) > 0

    def load_generate_areas(self, dst_grid_name=None, rebuild=False, reader_kwargs=None):
        """
        Load or generate regridding areas calling smmregrid
        """

        # get the grid name for logging
        area_logname = "target" if dst_grid_name else "source"

        if dst_grid_name:
            # get the target from the configuration
            dst_grid_dict = self._normalize_grid_dict(self.cfg_grid_dict['grids'].get(dst_grid_name))
            dst_grid_path = self._normalize_grid_path(dst_grid_dict.get('path'))
            cdo_extra = dst_grid_dict.get('cdo_extra')
            cdo_options = dst_grid_dict.get('cdo_options')
        else:
            dst_grid_path = {'2d': None}
            cdo_extra = self.src_grid_dict.get('cdo_extra')
            cdo_options = self.src_grid_dict.get('cdo_options')
        
        area_filename = f'tmp_areas.nc'
        #weights_filename = self._make_filename(dst_grid_name, self.grid_name, **reader_kwargs)
        #keep fallback option for filename when weights/areas block is not present in the configuration

        # check if weights already exist, if not, generate them
        if rebuild or self._check_existing_file(area_filename):
  
            # smmregrid call: get the first element of the list
            # this will not work if we do not have a 2d grid, need to be generalized
            generator = CdoGenerate(source_grid=self.src_grid_path['2d'],
                            target_grid=dst_grid_path['2d'],
                            cdo_extra=cdo_extra,
                            cdo_options=cdo_options,
                            cdo='cdo',
                            loglevel=self.loglevel)
            
            # if target_grid is not None, will generate the areas for the target grid
            grid_area = generator.areas(target=bool(dst_grid_name))
            grid_area.to_netcdf(area_filename)
            self.logger.info("Saved %s area to %s.", area_logname, area_filename)
        else:
            self.logger.info("Loading existing %s area from %s.", area_logname, area_filename)
            grid_area = xr.open_dataset(area_filename)

        # assign to the object
        if dst_grid_name is None:
            self.src_grid_area = grid_area
        else:
            self.dst_grid_area = grid_area
        
    def load_generate_weights(self, dst_grid_name, rebuild=False, reader_kwargs=None):
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
            if rebuild or not self._check_existing_file(weights_filename):
                
                # smmregrid call
                generator = CdoGenerate(source_grid=self.src_grid_path[vertical_dim],
                                target_grid=self.cfg_grid_dict['grids'][dst_grid_name],
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

    def _generate_area_filename(self, dst_grid_name, reader_kwargs):

        area_dict = self.cfg_grid_dict.get('areas')

        # destination grid name is provided
        if dst_grid_name:
            filename = area_dict["template_grid"].format(grid=dst_grid_name)
        # source grid name is provided
        else:
            if something:
                filename = area_dict["template_grid"].format(grid=self.src_grid_name)
            else:
                filename =  area_dict["template_default"].format(model=reader_kwargs["model"],
                                                                 exp=reader_kwargs["exp"],
                                                                 source=reader_kwargs["source"])
        # add the kwargs naming in the template file
        for parameter in DEFAULT_WEIGHTS_AREAS_PARAMETERS:
            if parameter in reader_kwargs:
                filename = re.sub(r'\.nc',
                                    '_' + parameter + str(reader_kwargs[parameter]) + r'\g<0>',
                                    filename)

        return filename
        
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




