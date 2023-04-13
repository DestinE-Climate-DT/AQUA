"""Regridder mixin for the Reader class"""

import os
import sys

import subprocess
import tempfile
import xarray as xr

import smmregrid as rg


class RegridMixin():
    """Regridding mixin for the Reader class"""

    def _make_dst_area_file(self, areafile, grid):
        """Helper function to create destination (regridded) area files."""

        self.logger.warning("Destination areas file not found: %s", areafile)
        self.logger.warning("Attempting to generate it ...")

        dst_extra = f"-const,1,{grid}"
        grid_area = self.cdo_generate_areas(source=dst_extra)

        # Make sure that grid areas contain exactly the same coordinates
        data = self.retrieve(fix=False)
        data = self.regridder.regrid(data.isel(time=0))
        grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.dst_space_coord})

        grid_area.to_netcdf(self.dst_areafile)
        self.logger.warning("Success!")

    def _make_src_area_file(self, areafile, source_grid,
                            gridpath="", icongridpath="", zoom=None):
        """Helper function to create source area files."""

        sgridpath = source_grid.get("path", None)
        if not sgridpath:
            # there is no source grid path at all defined in the regrid.yaml file:
            # let's reconstruct it from the file itself
            data = self.retrieve(fix=False)
            temp_file = tempfile.NamedTemporaryFile(mode='w')
            sgridpath = temp_file.name
            data.isel(time=0).to_netcdf(sgridpath)
        else:
            temp_file = None
            if zoom:
                sgridpath = sgridpath.format(zoom=9-zoom)

        self.logger.warning("Source areas file not found: %s", areafile)
        self.logger.warning("Attempting to generate it ...")
        self.logger.warning("Source grid: %s", sgridpath)
        src_extra = source_grid.get("extra", [])
        grid_area = self.cdo_generate_areas(source=sgridpath,
                                            gridpath=gridpath,
                                            icongridpath=icongridpath,
                                            extra=src_extra)
        # Make sure that the new DataArray uses the expected spatial dimensions
        grid_area = _rename_dims(grid_area, self.src_space_coord)
        data = self.retrieve(fix=False)
        grid_area = grid_area.assign_coords({coord: data.coords[coord] for coord in self.src_space_coord})
        grid_area.to_netcdf(areafile)
        self.logger.warning("Success!")

    def _make_weights_file(self, weightsfile, source_grid, cfg_regrid, regrid=None, extra=None, zoom=None):
        """Helper function to produce weights file"""

        sgridpath = source_grid.get("path", None)
        if not sgridpath:
            # there is no source grid path at all defined in the regrid.yaml file:
            # let's reconstruct it from the file itself
            data = self.retrieve(fix=False)
            temp_file = tempfile.NamedTemporaryFile(mode='w')
            sgridpath = temp_file.name
            data.isel(time=0).to_netcdf(sgridpath)
        else:
            temp_file = None
            if zoom:
                sgridpath = sgridpath.format(zoom=9-zoom)

        self.logger.warning("Weights file not found: %s", weightsfile)
        self.logger.warning("Attempting to generate it ...")
        self.logger.warning("Source grid: %s", sgridpath)

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
                                          target_grid=cfg_regrid["target_grids"][regrid],
                                          method='ycon',
                                          gridpath=cfg_regrid["cdo-paths"]["download"],
                                          icongridpath=cfg_regrid["cdo-paths"]["icon"],
                                          extra=extra)
        weights.to_netcdf(weightsfile)
        self.logger.warning("Success!")

    def cdo_generate_areas(self, source, icongridpath=None, gridpath=None, extra=None):
        """
            Generate grid areas using CDO

            Args:
                source (xarray.DataArray or str): Source grid
                gridpath (str): where to store downloaded grids
                icongridpath (str): location of ICON grids (e.g. /pool/data/ICON)
                extra: command(s) to apply to source grid before weight generation (can be a list)

            Returns:
                :obj:`xarray.DataArray` with cell areas
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
                        "cdo",
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
                        "cdo",
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
            self.logger.critical(err.output.decode(), file=sys.stderr)
            raise

        finally:
            # Clean up the temporary files
            if not isinstance(source, str):
                source_grid_file.close()
            area_file.close()


def _rename_dims(da, dim_list):
    """
    Renames the dimensions of a DataArray so that any dimension which is already
    in `dim_list` keeps its name, and the others are renamed to whichever other
    dimension name is in `dim_list`.
    If `da` has only one dimension with a name which is different from that in `dim_list`,
    it is renamed to that new name.
    If it has two coordinate names (e.g. "lon" and "lat") which appear also in `dim_list`,
    these are not touched.

    Parameters
    ----------
    da (xarray.DataArray):  The input DataArray to rename.
    dim_list (list of str): The list of dimension names to use.

    Returns
    -------
    xarray.DataArray
        A new DataArray with the renamed dimensions.
    """

    dims = list(da.dims)
    # Lisy of dims which are already there
    shared_dims = list(set(dims) & set(dim_list))
    # List of dims in B which are not in space_coord
    extra_dims = list(set(dims) - set(dim_list))
    # List of dims in da which are not in dim_list
    new_dims = list(set(dim_list) - set(dims))
    i = 0
    da_out = da
    for dim in extra_dims:
        if dim not in shared_dims:
            da_out = da.rename({dim: new_dims[i]})
            i += 1
    return da_out
