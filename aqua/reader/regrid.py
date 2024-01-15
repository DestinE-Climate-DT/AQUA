"""Regridder mixin for the Reader class"""

import os

import types
import subprocess
import tempfile
import xarray as xr

import smmregrid as rg


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

        dst_extra = f"-const,1,{grid}"
        grid_area = self.cdo_generate_areas(source=dst_extra)

        # Make sure that grid areas contain exactly the same coordinates
        data = self._retrieve_plain()
        data = self.regrid(data)

        grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.dst_space_coord})

        grid_area.to_netcdf(self.dst_areafile)
        self.logger.warning("Success!")

    def _make_src_area_file(self, areafile, source_grid,
                            gridpath="", icongridpath="", zoom=None):
        """
        Helper function to create source area files.

        Args:
            areafile (str): The path to the source area file to be created.
            source_grid (dict): The source grid specification.
            gridpath (str, optional): The path to the grid files. Defaults to an empty string.
            icongridpath (str, optional): The path to the ICON grid files. Defaults to an empty string.
            zoom (int, optional): The zoom level for the grid (for HealPix grids). Defaults to None.

        Returns:
            None
        """
       
        if self.vert_coord and self.vert_coord != ["2d"]:
            vert_coord = self.vert_coord[0]  # We need only the first one for areas
        else:
            vert_coord = None

        sgridpath = self._get_source_gridpath(source_grid, vert_coord, zoom)
       
        self.logger.warning("Source areas file not found: %s", areafile)
        self.logger.warning("Attempting to generate it ...")

        src_extra = source_grid.get("extra", [])

        grid_area = self.cdo_generate_areas(source=sgridpath,
                                            gridpath=gridpath,
                                            icongridpath=icongridpath,
                                            extra=src_extra)
        # Make sure that the new DataArray uses the expected spatial dimensions
        grid_area = _rename_dims(grid_area, self.src_space_coord)
        data = self._retrieve_plain(startdate=None)
        grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.src_space_coord})
        grid_area.to_netcdf(areafile)
        self.logger.warning("Success!")

    def _make_weights_file(self, weightsfile, source_grid, cfg_regrid, method='ycon',
                           regrid=None, extra=None, zoom=None, vert_coord=None):
        """
        Helper function to produce weights file.

        Args:
            weightsfile (str): The path to the weights file to be created.
            source_grid (dict): The source grid specification.
            cfg_regrid (dict): The regrid configuration.
            regrid (str, optional): The regrid option. Defaults to None.
            extra (str or list, optional): Extra command(s) to apply to source grid before weight generation. Defaults to None.
            zoom (int, optional): The zoom level for the grid (for HealPix grids). Defaults to None.
            vert_coord (str, optional): The vertical coordinate to use for weight generation. Defaults to None.
            method (str, optional): The interpolation method to be used (see CDO manual). Defaults to 'ycon'.
        Returns:
            None
        """

        sgridpath = self._get_source_gridpath(source_grid, vert_coord, zoom)

        if vert_coord == "2d" or vert_coord == "2dm":  # if 2d we need to pass None to smmregrid
            vert_coord = None

        self.logger.warning("Weights file not found: %s", weightsfile)
        self.logger.warning("Attempting to generate it ...")

        # hack to  pass a correct list of all options
        src_extra = source_grid.get("extra", [])
        if src_extra:
            if not isinstance(src_extra, list):
                src_extra = [src_extra]
        if extra:
            extra = [extra]
        else:
            extra = []
        extra = extra + src_extra

        weights = rg.cdo_generate_weights(source_grid=sgridpath,
                                          target_grid=cfg_regrid["grids"][regrid],
                                          method=method,
                                          gridpath=cfg_regrid["cdo-paths"]["download"],
                                          icongridpath=cfg_regrid["cdo-paths"]["icon"],
                                          extra=extra,
                                          cdo=self.cdo,
                                          vert_coord=vert_coord,
                                          nproc=self.nproc)
        weights.to_netcdf(weightsfile)
        self.logger.warning("Success!")

    def _get_source_gridpath(self, source_grid, vert_coord, zoom):
        """
        Helper function to get the source grid path.

        Args:
            source_grid (dict): The source grid specification.
            vert_coord (list): vertical coordinate
            zoom (str): zoom option

        Returns:
            xarray.DataArray: The source grid path.
        """

        sgridpath = source_grid.get("path", None)
        self.logger.info("Source grid: %s", sgridpath)

        if not sgridpath:
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
                sgridpath = data[available_vars[0]]
            else:
                raise ValueError("Cannot find any variabile to extract a grid sample")

        else:
            if isinstance(sgridpath, dict):
                if vert_coord:
                    sgridpath = sgridpath[vert_coord]
                else:
                    sgridpath = sgridpath["2d"]
            if zoom is not None:
                sgridpath = sgridpath.format(zoom=zoom)
            sgridpath = xr.open_dataset(sgridpath)

        return sgridpath

    def cdo_generate_areas(self, source, icongridpath=None, gridpath=None, extra=None):
        """
            Generate grid areas using CDO

            Args:
                source (xarray.DataArray or str): Source grid
                gridpath (str): where to store downloaded grids
                icongridpath (str): location of ICON grids (e.g. /pool/data/ICON)
                extra (str): command(s) to apply to source grid before weight generation (can be a list)

            Returns:
                xarray.DataArray: A DataArray containing cell areas.
        """

        # Make some temporary files that we'll feed to CDO
        area_file = tempfile.NamedTemporaryFile()

        if isinstance(source, str):
            sgrid = source
        else:
            source_grid_file = tempfile.NamedTemporaryFile()
            source.to_netcdf(source_grid_file.name)
            sgrid = source_grid_file.name

        # Setup environment
        env = os.environ
        if gridpath:
            env["CDO_DOWNLOAD_PATH"] = gridpath
        if icongridpath:
            env["CDO_ICON_GRIDS"] = icongridpath

        try:
            # Run CDO
            if extra:
                # make sure extra is a flat list if it is not already
                if not isinstance(extra, list):
                    extra = [extra]

                subprocess.check_output(
                    [
                        self.cdo,
                        "-f", "nc4",
                        "gridarea",
                    ] + extra +
                    [
                        sgrid,
                        area_file.name,
                    ],
                    stderr=subprocess.PIPE,
                    env=env,
                )
            else:
                subprocess.check_output(
                    [
                        self.cdo,
                        "-f", "nc4",
                        "gridarea",
                        sgrid,
                        area_file.name,
                    ],
                    stderr=subprocess.PIPE,
                    env=env,
                )

            areas = xr.load_dataset(area_file.name, engine="netcdf4")
            areas.cell_area.attrs['units'] = 'm2'
            areas.cell_area.attrs['standard_name'] = 'area'
            areas.cell_area.attrs['long_name'] = 'area of grid cell'
            return areas.cell_area

        except subprocess.CalledProcessError as err:
            # Print the CDO error message
            self.logger.critical(err.stderr.decode('utf-8'))
            raise

        finally:
            # Clean up the temporary files
            if not isinstance(source, str):
                source_grid_file.close()
            area_file.close()

    def _retrieve_plain(self, *args, **kwargs):
        """
        Retrieves making sure that no fixer and agregation are used,
        read only first variable and converts iterator to data
        """

        aggregation = self.aggregation
        fix = self.fix
        streaming = self.streaming
        self.fix = False
        self.aggregation = None
        self.streaming = False
        data = self.retrieve(sample=True, history=False, *args, **kwargs)
        self.aggregation = aggregation
        self.fix = fix
        self.streaming = streaming

        if isinstance(data, types.GeneratorType):
            data = next(data)

        vars = [var for var in data.data_vars if not var.endswith("_bnds")]
        data = data[[vars[0]]]

        return data

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
        data = None

        if space_coord is None:

            # this is done to load only if necessary
            if data is None:
                data = self._retrieve_plain(startdate=None)
            space_coord = [x for x in data.dims if x in default_horizontal_dims]
            if not space_coord:
                self.logger.debug('Default dims that are screened are %s', default_horizontal_dims)
                raise KeyError('Cannot identify any space_coord, you will will need to define it regrid.yaml')
            self.logger.info('Space_coords deduced from the source are %s', space_coord)

        if vert_coord is None:
        
            # this is done to load only if necessary
            if data is None:
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
