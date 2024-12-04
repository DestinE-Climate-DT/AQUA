"""Regridder mixin for the Reader class"""

import os
import types
import subprocess
import tempfile
import xarray as xr
from smmregrid import CdoGenerate

class RegridMixin():
    """Regridding mixin for the Reader class"""

    def _make_dst_area_file(self, areafile, grid):
        """
        Helper function to create destination (regridded) area files.

        Args:
            areafile (str): The path to the destination area file to be created.
            grid (str): The destination grid specification.

        Returns:
            None
        """

        self.logger.warning("Destination areas file not found: %s", areafile)
        self.logger.warning("Attempting to generate it ...")

        generator = CdoGenerate(source_grid=None, target_grid=grid,
                                cdo_extra=None,
                                cdo_options=None, cdo_download_path=None, 
                                cdo_icon_grids=None,
                                cdo=self.cdo, loglevel=self.loglevel)
        grid_area = generator.areas(target=True)['cell_area']

        #dst_extra = f"-const,1,{grid}"
        #grid_area = self.cdo_generate_areas(source=dst_extra)

        # Make sure that grid areas contain exactly the same coordinates
        data = self._retrieve_plain()
        data = self.regrid(data)

        grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.dst_space_coord})

        grid_area.to_netcdf(self.dst_areafile)
        self.logger.warning("Success!")

    def _make_src_area_file(self, areafile, source_grid,
                            gridpath="", icongridpath=""):
        """
        Helper function to create source area files.

        Args:
            areafile (str): The path to the source area file to be created.
            source_grid (dict): The source grid specification.
            gridpath (str, optional): The path to the grid files. Defaults to an empty string.
            icongridpath (str, optional): The path to the ICON grid files. Defaults to an empty string.

        Returns:
            None
        """
       
        if self.vert_coord and self.vert_coord != ["2d"]:
            vert_coord = self.vert_coord[0]  # We need only the first one for areas
        else:
            vert_coord = None

        sgrid = self._get_source_grid(source_grid, vert_coord)
       
        self.logger.warning("Source areas file not found: %s", areafile)
        self.logger.warning("Attempting to generate it ...")

        src_extra = source_grid.get("cdo_extra", [])

        #grid_area = self.cdo_generate_areas(source=sgrid,
        #                                    gridpath=gridpath,
        #                                    icongridpath=icongridpath,
        #                                    extra=src_extra)

        generator = CdoGenerate(sgrid, cdo_extra=src_extra,
                                cdo_options=None, cdo_download_path=gridpath, 
                                cdo_icon_grids=icongridpath,
                                cdo=self.cdo, loglevel=self.loglevel)
        grid_area = generator.areas()['cell_area']


        # Make sure that the new DataArray uses the expected spatial dimensions
        grid_area = _rename_dims(grid_area, self.src_space_coord)
        data = self._retrieve_plain(startdate=None)
        grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.src_space_coord})
        grid_area.to_netcdf(areafile)
        self.logger.warning("Success!")

    def _weights_generation_time(self, original_grid_size=None, new_grid_size=None, nproc=None, vert_coord_size=None):
        """
        Helper function to estimate the time required for generating regridding weights.

        Args:
            original_grid_size (int, optional): Size of the original grid. Defaults to None.
            new_grid_size (int, optional): Size of the new grid. Defaults to None.
            nproc (int, optional): Number of processors to be used in the computation. Defaults to None.
            vert_coord_size (int, optional): Size of the vertical coordinate. Defaults to None.

        Returns:
            None: This function does not return a value but logs the estimated time for weight generation.
        """
        if None in [original_grid_size, new_grid_size, nproc, vert_coord_size]:
            self.logger.error("Missing required parameter for weight generation time estimation.")
            return

        warning_threshold = 59  # seconds

        # Log comparison of grid sizes
        self.logger.debug(f"Grid size comparison - Original: {original_grid_size}, New: {new_grid_size}.")

        # Assumptions for Instructions Per Second (IPS)
        IPS_original = 0.00013 / max(vert_coord_size, 1)
        IPS_new = 0.000043 / max(vert_coord_size / (nproc + 1), 1)

        clock_speed = 3.5 * 10**9  # Hz

        # Operations Per Second (OPS)
        OPS_original = clock_speed * IPS_original
        OPS_new = clock_speed * IPS_new

        expected_time_original = original_grid_size / OPS_original
        expected_time_new = new_grid_size / OPS_new
        expected_time = expected_time_original + expected_time_new

        self.logger.debug(f"The total expected processing time is {expected_time} seconds.")

        if expected_time > warning_threshold:
            hours, remainder = divmod(int(expected_time), 3600)
            minutes = round((int(expected_time) % 3600) / 60)
            formatted_time = f'{hours} hours, {minutes} minutes'
            self.logger.warning(f'Time to generate the weights will take approximately {formatted_time}.')


    def _make_weights_file(self, weightsfile, source_grid, cfg_regrid, method='ycon', regrid=None, extra=None, 
                           vert_coord=None, original_grid_size=None, nproc=None):
        """
        Helper function to produce weights file.

        Args:
            weightsfile (str): The path to the weights file to be created.
            source_grid (dict): The source grid specification.
            cfg_regrid (dict): The regrid configuration.
            regrid (str, optional): The regrid option. Defaults to None.
            extra (str or list, optional): Extra command(s) to apply to source grid before weight generation. Defaults to None.
            vert_coord (str, optional): The vertical coordinate to use for weight generation. Defaults to None.
            method (str, optional): The interpolation method to be used (see CDO manual). Defaults to 'ycon'.
        Returns:
            None
        """

        sgrid = self._get_source_grid(source_grid, vert_coord)

        self.logger.warning("Weights file not found: %s", weightsfile)
        self.logger.warning("Attempting to generate it ...")

        if vert_coord == "2d" or vert_coord == "2dm":  # if 2d we need to pass None to smmregrid
            vert_coord = None

        width, height = map(int, cfg_regrid['grids'][regrid][1:].split('x'))
        new_grid_size = width * height
        
        total_size = sgrid.sizes
        total_elements = 1
        for dim_size in total_size.values():
            total_elements *= dim_size

        if original_grid_size > 0:  # Prevent division by zero
            vert_coord_size = total_elements / original_grid_size
        else:
            vert_coord_size = 1

        self._weights_generation_time(original_grid_size=original_grid_size,
                                      new_grid_size=new_grid_size, vert_coord_size=vert_coord_size, nproc=nproc)

        # hack to  pass a correct list of all options
        src_extra = source_grid.get("cdo_extra", [])
        src_options = source_grid.get("cdo_options", [])
        if src_extra:
            if not isinstance(src_extra, list):
                src_extra = [src_extra]
        if extra:
            extra = [extra]
        else:
            extra = []
        extra = extra + src_extra

        sgrid.load()  # load the data to avoid problems with dask in smmregrid
        sgrid = sgrid.compute()  # for some reason both lines are needed 
         
        generator = CdoGenerate(source_grid=sgrid,
                                target_grid=cfg_regrid["grids"][regrid],
                                cdo_download_path=cfg_regrid["cdo-paths"]["download"],
                                cdo_icon_grids=cfg_regrid["cdo-paths"]["icon"],
                                cdo_extra=extra,
                                cdo_options=src_options,
                                cdo=self.cdo,
                                loglevel=self.loglevel)
        weights = generator.weights(method=method, vert_coord=vert_coord, nproc=self.nproc)
        # weights = rg.cdo_generate_weights(source_grid=sgrid,
        #                                   target_grid=cfg_regrid["grids"][regrid],
        #                                   method=method,
        #                                   gridpath=cfg_regrid["cdo-paths"]["download"],
        #                                   icongridpath=cfg_regrid["cdo-paths"]["icon"],
        #                                   cdo_extra=extra,
        #                                   cdo_options=src_options,
        #                                   cdo=self.cdo,
        #                                   vert_coord=vert_coord,
        #                                   nproc=self.nproc,
        #                                   loglevel=self.loglevel)

        weights.to_netcdf(weightsfile)
        self.logger.warning("Success!")

    def _get_source_grid(self, source_grid, vert_coord):
        """
        Helper function to get the source grid path.

        Args:
            source_grid (dict): The source grid specification.
            vert_coord (list): vertical coordinate

        Returns:
            xarray.DataArray: The source grid path.
        """

        sgrid = source_grid.get("path", None)
        self.logger.info("Source grid: %s", sgrid)

        if not sgrid:
            # there is no source grid path at all defined in the regrid.yaml file:
            # let's reconstruct it from the file itself

            self.logger.warning('Grid file is not defined, retrieving the source itself...')
            data = self._retrieve_plain()

            # use slicing to copy the object and not create a pointer
            coords = self.src_space_coord[:]

            # If we have also a vertical coordinate, include it in the sample
            if vert_coord and vert_coord != "2d" and vert_coord != "2dm":
                coords.append(vert_coord)

            data = _get_spatial_sample(data, coords, self.support_dims)

            if vert_coord and vert_coord != "2d" and vert_coord != "2dm":
                varsel = [var for var in data.data_vars if vert_coord in data[var].dims]
                if varsel:
                    data = data[varsel]
                else:
                    raise ValueError(f"No variable with dimension {vert_coord} found in the dataset")

            # We need only one variable and we do not want vars with "bnds/bounds"
            available_vars = [var for var in list(data.data_vars) if 'bnds' not in var and 'bounds' not in var]
            if available_vars:
                sgrid = data[available_vars[0]]
            else:
                raise ValueError("Cannot find any variabile to extract a grid sample")

        else:
            if isinstance(sgrid, dict):
                if vert_coord:
                    sgrid = sgrid[vert_coord]
                else:
                    sgrid = sgrid["2d"]
                sgrid = sgrid.format(**self.kwargs)
            sgrid = xr.open_dataset(sgrid)

        return sgrid

    def _retrieve_plain(self, *args, **kwargs):
        """
        Retrieves making sure that no fixer and agregation are used,
        read only first variable and converts iterator to data
        """
        if self.sample_data is not None:
            self.logger.debug('Sample data already availabe, avoid _retrieve_plain()')
            return self.sample_data

        self.logger.debug('Getting sample data through _retrieve_plain()...')
        aggregation = self.aggregation
        chunks = self.chunks
        fix = self.fix
        streaming = self.streaming
        startdate = self.startdate
        enddate = self.enddate
        preproc = self.preproc
        self.fix = False
        self.aggregation = None
        self.chunks = None
        self.streaming = False
        self.startdate = None
        self.enddate = None
        self.preproc = None
        data = self.retrieve(sample=True, history=False, *args, **kwargs)
        # HACK: ensuring we load only a single time step if possible:
        if 'time' in data.coords:
            data = data.isel(time=0)
        else:
            self.logger.warning('No time dimension found while sampling the data!')
        self.aggregation = aggregation
        self.chunks = chunks
        self.fix = fix
        self.streaming = streaming
        self.startdate = startdate
        self.enddate = enddate
        self.preproc = preproc

        if isinstance(data, types.GeneratorType):
            data = next(data)

        # select only first relevant variable
        variables = [var for var in data.data_vars if
                not var.endswith("_bnds") and not var.startswith("bounds") and not var.endswith("_bounds")]
        self.sample_data = data[[variables[0]]]

        return self.sample_data

    def _guess_coords(self, space_coord, vert_coord, default_horizontal_dims, default_vertical_dims):
        """
        Given a set of default space and vertical dimensions, 
        find the one present in the data and return them

        Args:
            space_coord (str or list): horizontal dimension already defined. If None, autosearch enabled.
            vert_coord (str or list): vertical dimension already defined. If None, autosearch enabled.
            default_horizontal_dims (list): default dimensions for the horizontal search
            default_vertical_dims (list): default dimensions for the vertical search 

        Return
            space_coord and vert_coord from the data source
        """

        if space_coord is None:

            data = self._retrieve_plain(startdate=None)
            space_coord = [x for x in data.dims if x in default_horizontal_dims]
            if not space_coord:
                self.logger.debug('Default dims that are screened are %s', default_horizontal_dims)
                raise KeyError('Cannot identify any space_coord, you will will need to define it regrid.yaml')
            self.logger.info('Space_coords deduced from the source are %s', space_coord)

        if vert_coord is None:
     
            # this is done to load only if necessary
            data = self._retrieve_plain(startdate=None)
            vert_coord = [x for x in data.dims if x in default_vertical_dims]
            if not vert_coord:
                self.logger.debug('Default dims that are screened are %s', default_vertical_dims)
                self.logger.debug('Assuming this is a 2d file, i.e. vert_coord=2d')
                # If not specified we assume that this is only a 2D case
                vert_coord = '2d'
            
            self.logger.info('vert_coord deduced from the source are %s', vert_coord)

        return space_coord, vert_coord


def _rename_dims(data, dim_list):
    """
    Renames the dimensions of a DataArray so that any dimension which is already
    in `dim_list` keeps its name, and the others are renamed to whichever other
    dimension name is in `dim_list`.
    If `da` has only one dimension with a name which is different from that in `dim_list`,
    it is renamed to that new name.
    If it has two coordinate names (e.g. "lon" and "lat") which appear also in `dim_list`,
    these are not touched.

    Args:
        da (xarray.DataArray): The input DataArray to rename.
        dim_list (list of str): The list of dimension names to use.

    Returns:
        xarray.DataArray: A new DataArray with the renamed dimensions.
    """

    dims = list(data.dims)
    # Lisy of dims which are already there
    shared_dims = list(set(dims) & set(dim_list))
    # List of dims in B which are not in space_coord
    extra_dims = list(set(dims) - set(dim_list))
    # List of dims in da which are not in dim_list
    new_dims = list(set(dim_list) - set(dims))
    i = 0
    da_out = data
    for dim in extra_dims:
        if dim not in shared_dims:
            da_out = data.rename({dim: new_dims[i]})
            i += 1
    return da_out


def _get_spatial_sample(data, space_coord, support_dims):
    """
    Selects a single spatial sample along the dimensions specified in `space_coord`.

    Arguments:
        da (xarray.DataArray): Input data array to select the spatial sample from.
        space_coord (list of str): List of dimension names corresponding to the spatial coordinates to select.
        support_dims (list of str): List of additional dimensions to keep when a single slice is taken (eg. "cell_corners")

    Returns:
        Data array containing a single spatial sample along the specified dimensions.
    """

    dims = list(data.dims)
    extra_dims = list(set(dims) - set(space_coord) - set(support_dims))
    da_out = data.isel({dim: 0 for dim in extra_dims})
    return da_out
