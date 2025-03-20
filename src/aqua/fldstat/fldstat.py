"""AQUA class for field statitics"""
import xarray as xr

from smmregrid import GridInspector

from aqua.logger import log_configure, log_history
from aqua.util import area_selection


class FldStat():
    """AQUA class for field statitics"""

    def __init__(self, area, horizontal_dims=None, grid_name=None, loglevel='WARNING'):
        """
        Initialize the FldStat.

        Args:
            area (str): The area to calculate the statistics for.
            loglevel (str): The logging level.
        """

        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='FldStat')
        self.area = area

        if self.area is None:
            self.logger.warning("No area provided, no weighted area can be provided.")
            return

        # safety checks
        if not isinstance(area, (xr.DataArray, xr.Dataset)):
            raise ValueError("Area must be an xarray DataArray or Dataset.")
        
        # TODO: add a guess with smmregrid GridInspector
        if horizontal_dims is None:
            self.logger.warning("No horizontal dimensions provided, will try to guess from data when provided!")
        self.horizontal_dims = horizontal_dims
        self.logger.debug('Space coordinates are %s', self.horizontal_dims)
        self.grid_name = grid_name

        
    def fldmean(self, data, lon_limits=None, lat_limits=None, **kwargs):
        """
        Perform a weighted global average.
        If a subset of the data is provided, the average is performed only on the subset.

        Arguments:
            data (xr.DataArray or xarray.DataDataset):  the input data
            lon_limits (list, optional):  the longitude limits of the subset
            lat_limits (list, optional):  the latitude limits of the subset

        Kwargs:
            - box_brd (bool,opt): choose if coordinates are comprised or not in area selection.
                                  Default is True

        Returns:
            the value of the averaged field
        """

        if not isinstance(data, (xr.DataArray, xr.Dataset)):
            raise ValueError("Data must be an xarray DataArray or Dataset.")

        # if horizontal_dims is not provided, try to guess it
        if self.horizontal_dims is None:
            data_gridtype = GridInspector(data).get_grid_info()
            if len(data_gridtype) > 1:
                raise ValueError("Multiple grid types found in the data, please provide horizontal_dims!")
            self.horizontal_dims = data_gridtype[0].horizontal_dims

        #if area is not provided, return the raw mean
        if self.area is None:
            return data.mean(dim=self.horizontal_dims)
        
        # align dimensions of area and data
        self.area = self.align_area_dimensions(data)

        if lon_limits is not None or lat_limits is not None:
            data = area_selection(data, lon=lon_limits, lat=lat_limits,
                                  loglevel=self.loglevel, **kwargs)

        # cleaning coordinates which have "multiple" coordinates in their own definition
        # grid_area = self._clean_spourious_coords(grid_area, name = "area")
        # data = self._clean_spourious_coords(data, name = "data")

        # check if coordinates are aligned
        try:
            xr.align(self.area, data, join='exact')
        except ValueError as err:
            raise ValueError('Coordinates are not aligned!') from err
            # # check in the dimensions what is wrong
            # for coord in grid_area.coords:
            #     if coord in space_coord:
            #         xcoord = data.coords[coord]

            #         # first case: shape different
            #         if len(grid_area[coord]) != len(xcoord):
            #             raise ValueError(f'{coord} has different shape between area files and your dataset.'
            #                              'If using the LRA, try setting the regrid=r100 option') from err
            #         # shape are ok, but coords are different
            #         if not grid_area[coord].equals(xcoord):
            #             # if they are fine when sorted, there is a sorting mismatch
            #             if grid_area[coord].sortby(coord).equals(xcoord.sortby(coord)):
            #                 self.logger.warning('%s is sorted in different way between area files and your dataset. Flipping it!',
            #                                     coord)
            #                 grid_area = grid_area.reindex({coord: list(reversed(grid_area[coord]))})
            #             else:
            #                 raise ValueError(f'{coord} has a mismatch in coordinate values!') from err

        self.logger.debug('Computing the weighted average over  %s', self.horizontal_dims)
        out = data.weighted(weights=self.area.fillna(0)).mean(dim=self.horizontal_dims)

        if self.grid_name is not None:
            log_history(data, f"Spatially averaged by fldmean from {self.grid_name} grid")

        return out
    
    def align_area_dimensions(self, data):
        """
        Align the area dimensions with the data dimensions.
        If the area and data have different number of horizontal dimensions, try to rename them.
        """

        # verify that horizontal dimensions area the same in the two datasets.
        # If not, try to rename them. Use gridtype to get the horizontal dimensions
        area_gridtype = GridInspector(self.area, extra_dims={"horizontal": ["rgrid"]}).get_grid_info()
        area_horizontal_dims = area_gridtype[0].horizontal_dims

        if set(area_horizontal_dims) == set(self.horizontal_dims):
            return self.area

        # check if area and data have the same number of horizontal dimensions
        if len(area_horizontal_dims) != len(self.horizontal_dims):
            raise ValueError("Area and data have different number of horizontal dimensions!")

        # check if area and data have the same horizontal dimensions
        self.logger.warning("Area %s and data %s have different horizontal dimensions! Renaming them!",
                            area_horizontal_dims, self.horizontal_dims)
        # create a dictionary for renaming matching dimensions have the same length
        matching_dims = {a: d for a, d in zip(area_horizontal_dims, self.horizontal_dims) if self.area.sizes[a] == data.sizes[d]}
        self.logger.info("Area dimensions has been renamed with %s",  matching_dims)
        return self.area.rename(matching_dims)


