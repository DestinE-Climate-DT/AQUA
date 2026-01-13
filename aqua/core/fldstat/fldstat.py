"""AQUA class for field statitics"""
import xarray as xr
import numpy as np
import regionmask

from smmregrid import GridInspector

from aqua.core.logger import log_configure, log_history
from aqua.core.util import multiply_units

from .area_selection import AreaSelection

# set default options for xarray
xr.set_options(keep_attrs=True)


class FldStat():
    """AQUA class for field statitics"""

    def __init__(self,
                 area: xr.Dataset | xr.DataArray | None = None,
                 horizontal_dims: list[str] | None = None,
                 grid_name: str | None = None,
                 loglevel: str = 'WARNING'):
        """
        Initialize the FldStat.

        Args:
            area (xr.Dataset, xr.DataArray, optional): The area to calculate the statistics for.
            horizontal_dims (list, optional): The horizontal dimensions of the data.
            grid_name (str, optional): The name of the grid, used for logging history.
            loglevel (str, optional): The logging level.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='FldStat')
        self.area = area
        if horizontal_dims is None:
            self.logger.warning("No horizontal dimensions provided, will try to guess from data when provided!")
        self.horizontal_dims = horizontal_dims

        if self.area is None:
            self.logger.warning("No area provided, no weighted area can be provided.")
            return

        # safety checks
        if not isinstance(area, (xr.DataArray, xr.Dataset)):
            raise ValueError("Area must be an xarray DataArray or Dataset.")

        self.grid_name = grid_name

        # Initialize area selection
        self.area_selection = AreaSelection(loglevel=loglevel)

    @property
    def AVAILABLE_FLDSTATS(self):
        """Return available field statistics."""
        return {"custom":   ['integral', 'areasum'],
                "standard": ['mean', 'std', 'max', 'min', 'sum']}

    def fldstat(self, data: xr.DataArray | xr.Dataset,
                stat: str = "mean",
                region: regionmask.Regions | None = None,
                region_sel: str | int | list | None = None,
                mask_kwargs: dict = {},
                lon_limits: list | None = None, lat_limits: list | None = None,
                dims: list | None = None,
                **kwargs):
        """
        Perform a weighted global average.
        If a subset of the data is provided, the average is performed only on the subset.

        Args:
            data (xr.DataArray or xarray.DataDataset):  the input data
            stat (str):  the statistic to compute, only supported is "mean"
            region (regionmask.Regions, optional): A regionmask Regions object defining a class regions.
            region_sel (str, int or list, optional): The region(s) to select by name or number from the region object.
            mask_kwargs (dict, optional): Additional keyword arguments passed to region.mask().
            lon_limits (list, optional):  the longitude limits of the subset
            lat_limits (list, optional):  the latitude limits of the subset
            dims (list, optional):  the dimensions to average over, if not provided, horizontal_dims are used
            **kwargs: additional arguments passed to fldstat

        Kwargs:
            - box_brd (bool,opt): choose if coordinates are comprised or not in area selection.
                                  Default is True

        Returns:
            The value of the averaged field
        """

        if stat not in [s for stats in self.AVAILABLE_FLDSTATS.values() for s in stats]:
            raise ValueError(f"Statistic {stat} not supported by AQUA FldStat(), only "
                             f"{[s for stats in self.AVAILABLE_FLDSTATS.values() for s in stats]} are supported.")

        if not isinstance(data, (xr.DataArray, xr.Dataset)):
            raise ValueError("Data must be an xarray DataArray or Dataset.")

        # if horizontal_dims is not provided, try to guess it
        if self.horizontal_dims is None:
            # please notice GridInspector always return a list of GridType objects
            data_gridtype = GridInspector(data).get_gridtype()
            if len(data_gridtype) > 1:
                raise ValueError("Multiple grid types found in the data, please provide horizontal_dims!")
            self.horizontal_dims = data_gridtype[0].horizontal_dims
            self.logger.debug('Horizontal dimensions guessed from data are %s', self.horizontal_dims)

        # Determine which dims to average over based on mean_type
        if not dims:
            dims = self.horizontal_dims
        else:
            if not isinstance(dims, list):
                raise ValueError("dims must be a list of dimension names.")
            # check if dims are in horizontal_dims
            for dim in dims:
                if dim not in self.horizontal_dims:
                    raise ValueError(f"Dimension {dim} not found in horizontal dimensions: {self.horizontal_dims}")

        # If area is not provided, return the raw mean
        if self.area is None:
            self.logger.warning("No area provided, no area-weighted stat can be provided.")
            # compact call, equivalent of "out = data.mean()"
            if stat in self.AVAILABLE_FLDSTATS["standard"]:
                self.logger.info("Computing unweighted %s on %s dimensions", stat, self.horizontal_dims)
                log_history(data, f"Unweighted {stat} computed on {self.horizontal_dims} dimensions")
                return getattr(data, stat)(dim=self.horizontal_dims)

        # align dimensions naming of area to match data
        self.area = self.align_area_dimensions(data)

        # align coordinates values of area to match data
        self.area = self.align_area_coordinates(data)
    
        if lon_limits is not None or lat_limits is not None or region is not None:
            self.logger.debug("Selecting area for field stat calculation.")
            data = self.area_selection.select_area(data, lon=lon_limits, lat=lat_limits,
                                                   region=region, region_sel=region_sel,
                                                   mask_kwargs=mask_kwargs,
                                                   to_180=False, **kwargs)

        # cleaning coordinates which have "multiple" coordinates in their own definition
        # grid_area = self._clean_spourious_coords(grid_area, name = "area")
        # data = self._clean_spourious_coords(data, name = "data")

        # compact call, equivalent of "out = weighted_data.mean()""
        self.logger.info("Computing area-weighted %s on %s dimensions", stat, dims)

        if stat == 'integral':
            out = self.integrate_over_area(data, self.area, dims)
        elif stat == 'areasum':
            out = self.sum_area(data, self.area, dims)
        elif stat in ['max', 'min']:
            # max/min are not supported by weighted arrays, use unweighted calculation
            out = getattr(data, stat)(dim=dims)
        else:
            weighted_data = data.weighted(weights=self.area.fillna(0))
            out = getattr(weighted_data, stat)(dim=dims)

        if self.grid_name is not None:
            log_history(out, f"From grid '{self.grid_name}'. Computed field stat '{stat}'")

        return out

    def select_area(self, data: xr.Dataset | xr.DataArray,
                    lon: list | None = None, lat: list | None = None,
                    box_brd: bool = True, drop: bool = False,
                    lat_name: str = "lat", lon_name: str = "lon",
                    region: regionmask.Regions | None = None,
                    region_sel: str | int | list | None = None,
                    mask_kwargs: dict = {},
                    default_coords: dict = {"lat_min": -90, "lat_max": 90,
                                            "lon_min": 0, "lon_max": 360},
                    to_180: bool = True) -> xr.Dataset | xr.DataArray:
        """
        Select a specific area from the dataset based on longitude and latitude ranges.
        Wrapper for AreaSelection.select_area method.
        """
        # TODO: The lat_name and lon_name are at the actual stage in the
        # select_area method arguments. However it is possible to foresee
        # that we may want to automatically detect the names
        return self.area_selection.select_area(data, lon=lon, lat=lat,
                                               box_brd=box_brd, drop=drop,
                                               lat_name=lat_name, lon_name=lon_name,
                                               region=region, region_sel=region_sel,
                                               mask_kwargs=mask_kwargs,
                                               default_coords=default_coords, to_180=to_180)

    def integrate_over_area(self, data: xr.Dataset | xr.DataArray,
                            areacell: xr.DataArray,
                            dims: list):
        """
        Compute the integral of the data over the area.

        Args:
            data (xr.DataArray or xr.Dataset): The data, used also for masking.
            areacell (xr.DataArray): The area cells.
            dims (list): Dimensions to sum over.

        Returns:
            xr.DataArray or xr.Dataset: The integral of the data over the area
        """
        area_weighted_data = data * areacell.where(data.notnull())

        # preserve attrs (e.g. AQUA_region) from areacell due to multiplication above, which has priority if keys overlap
        merged_attrs = {**data.attrs, **areacell.attrs}

        if 'units' in data.attrs and 'units' in areacell.attrs:
            merged_attrs['units'] = multiply_units(data.attrs['units'], areacell.attrs['units'])
        else:
            self.logger.warning(f"Data units: {data.attrs.get('units', 'None')}; "
                                f"Area units: {areacell.attrs.get('units', 'None')}, cannot multiply units using Metpy.")

        if 'long_name' in data.attrs:
            merged_attrs['long_name'] = f"Integrated {data.attrs['long_name']}"

        area_weighted_data.attrs.update(merged_attrs)

        area_weighted_integral = area_weighted_data.sum(skipna=True, min_count=1, dim=dims)

        return area_weighted_integral

    def sum_area(self,
                 data: xr.Dataset | xr.DataArray, 
                 areacell: xr.DataArray, 
                 dims: list):
        """
        Compute the sum of area cells where masked data is not null.

        This is useful for computing field such as sea ice extent, by summing
        the area of cells that contain data not null.

        Note: if data is not masked might return incorrect results. If irrelevant regions (e.g., low level or land
        in sea-ice data) are not masked beforehand, their area will be incorrectly included in the sum.

        Args:
            data (xr.DataArray or xr.Dataset): The data (check if pre-masking is needed in the considered variable)
            areacell (xr.DataArray): The area cells.
            dims (list): Dimensions to sum over.

        Returns:
            xr.DataArray or xr.Dataset: The sum of area cells
        """
        summed_area = areacell.where(data).sum(skipna=True, min_count=1, dim=dims)

        if self.grid_name is not None:
            log_history(summed_area, f"Area summed from {self.grid_name} grid")
        return summed_area

    def align_area_dimensions(self, data: xr.Dataset | xr.DataArray):
        """
        Align the area dimensions with the data dimensions.
        If the area and data have different number of horizontal dimensions, try to rename them.

        Args:
            data (xr.DataArray or xr.Dataset): The input data to align with the area.
        """

        # verify that horizontal dimensions area the same in the two datasets.
        # If not, try to rename them. Use gridtype to get the horizontal dimensions
        # TODO: "rgrid" is not a default dimension in smmregrid, it should be added.
        # please notice GridInspector always return a list of GridType objects
        area_gridtype = GridInspector(self.area, extra_dims={"horizontal": ["rgrid"]}).get_gridtype()
        area_horizontal_dims = area_gridtype[0].horizontal_dims
        self.logger.debug("Area horizontal dimensions are %s", area_horizontal_dims)

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

    def align_area_coordinates(self, data: xr.Dataset | xr.DataArray, decimals: int = 5):
        """
        Check if the coordinates of the area and data are aligned.
        If they are not aligned, try to flip the coordinates.

        Args:
            data (xr.DataArray or xr.Dataset): The input data to align with the area.
            decimals (int): Number of decimals to use for rounding when aligning coordinates.

        Returns:
            xr.DataArray or xr.Dataset: The area with aligned coordinates.
        """

        # area.coords should be only lon-lat
        for coord in self.area.coords:
            if coord in data.coords and coord != "time":
                area_coord = self.area[coord]
                data_coord = data[coord]

                # verify coordinates has the same sizes
                if area_coord.size != data_coord.size:
                    raise ValueError(f"{coord} has a mismatch in length!")

                # Fast check if coordinates are already aligned
                if np.array_equal(area_coord, data_coord):
                    continue

                # Fast check for reversed coordinates: use slicing
                if np.array_equal(area_coord[::-1], data_coord):
                    self.logger.warning("Reversing coordinate '%s' for alignment.", coord)
                    self.area = self.area.isel({coord: slice(None, None, -1)})
                    continue

                # Try alignment by rounding to specified decimals
                area_rounded = np.round(area_coord, decimals=decimals)
                data_rounded = np.round(data_coord, decimals=decimals)
                if np.array_equal(area_rounded, data_rounded):
                    self.logger.warning("Coordinate '%s' aligned by rounding to %d decimals.", coord, decimals)
                    # assign the rounded coordinates to the area (matching data's rounded values)
                    self.area = self.area.assign_coords({coord: data_coord})
                    continue

                raise ValueError(f"Mismatch in values for coordinate '{coord}' between data and areas.")

        return self.area
