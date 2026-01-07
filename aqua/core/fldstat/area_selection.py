import xarray as xr
import regionmask
from typeguard import typechecked
from aqua.core.logger import log_configure, log_history
from aqua.core.util import check_coordinates, to_list

# set default options for xarray
xr.set_options(keep_attrs=True)

DEFAULT_COORDS = {"lat_min": -90, "lat_max": 90,
                  "lon_min": 0, "lon_max": 360}


class AreaSelection:
    """Class to select an area from an xarray Dataset."""

    def __init__(self, loglevel: str = "WARNING"):
        """
        Initialize the AreaSelection.

        Args:
            loglevel (str, optional): The logging level. Default is "WARNING".
        """
        self.logger = log_configure(
            log_level=loglevel, log_name="AreaSelection"
        )

    @typechecked
    def select_area(
        self,
        data: xr.Dataset | xr.DataArray,
        lon: list | None = None,
        lat: list | None = None,
        box_brd: bool = True,
        drop: bool = False,
        lat_name: str = "lat",
        lon_name: str = "lon",
        region: regionmask.Regions | None = None,
        region_sel: str | int | list | None = None,
        mask_kwargs: dict = {},
        default_coords: dict | None = None,
        to_180: bool = True,
    ) -> xr.Dataset | xr.DataArray:
        """
        Select a specific area from the dataset based on longitude and
        latitude ranges.

        Args:
            data (xr.Dataset or xr.DataArray): The input dataset or data array.
            lon (list, optional): The longitude range to select.
            lat (list, optional): The latitude range to select.
            box_brd (bool, optional): Whether to include box boundaries.
                Default is True.
            drop (bool, optional): Whether to drop non-selected data.
                Default is False.
            lat_name (str, optional): Name of latitude coordinate.
                Default is "lat".
            lon_name (str, optional): Name of longitude coordinate.
                Default is "lon".
            region (regionmask.Regions, optional): A regionmask Regions object defining a class regions.
            region_sel (str, int or list, optional): The region(s) to select by name or number from the region object.
            mask_kwargs (dict, optional): Additional keyword arguments passed to region.mask().
            default_coords (dict, optional): Default coordinate ranges.
                Defaults to {"lat_min": -90, "lat_max": 90,
                "lon_min": 0, "lon_max": 360}.
            to_180 (bool, optional): Whether to convert longitude to
                [-180, 180] range. Default is False.

        Returns:
            xr.Dataset or None: The selected area dataset or None if no
            area is selected.
        """
        # By default we work with the AQUA data_model but we keep the
        # flexibility to adapt to other data models.
        if lat_name not in data.coords or lon_name not in data.coords:
            raise KeyError(
                f"Latitude or Longitude coordinates not found. "
                f"Expected '{lat_name}' and '{lon_name}'."
            )

        # Case1: Regionmask selection
        if region is not None:
            self.logger.info("A region was provided for area selection, lon/lat selection will be ignored.")

            if region_sel is None:
                raise ValueError("`region_sel` must be specified when using region argument.")

            mask = region.mask(data[lon_name], data[lat_name], **mask_kwargs)

            # Normalize input to list
            region_sel = to_list(region_sel)

            # Convert region names to numbers if necessary
            region_numbers = [
                region.map_keys(name) if isinstance(name, str) else name
                for name in region_sel
            ]

            # Combine masks for selected regions
            reg_mask = xr.zeros_like(mask, dtype=bool)
            for rn in region_numbers:
                reg_mask = reg_mask | (mask == rn)

            reg_mask = reg_mask.fillna(False)  # handle NaNs from regionmask

            selected = data.where(reg_mask, drop=drop)

            region_sel = [
                region.names[rs] if isinstance(rs, int) else rs
                for rs in region_sel
            ]
            region_str = ", ".join([str(rs) for rs in region_sel])

            selected = log_history(selected, f"Regionmask selection: {region_str}")

            return selected

        # 2. Coordinate-based selection

        # If both lon and lat are None, no selection is needed
        if lon is None and lat is None:
            return data

        default_coords = default_coords or DEFAULT_COORDS

        lon, lat = check_coordinates(lon, lat, default_coords)

        # Detect whether the requested longitude selection crosses the
        # Greenwich meridian in the 0..360 system (e.g. lon becomes
        # [280, 30]). In that case we convert the final selected
        # longitudes to [-180, 180] and sort them so the selection is
        # contiguous for plotting.
        crossing_greenwich = (
            default_coords.get("lon_min") == 0
            and default_coords.get("lon_max") == 360
            and lon[0] > lon[1]
        )

        # Building the mask
        lat_condition = (
            (data[lat_name] >= lat[0]) & (data[lat_name] <= lat[1])
            if box_brd
            else (data[lat_name] > lat[0]) & (data[lat_name] < lat[1])
        )

        lon_condition = self._lon_condition(
            data, lon_name=lon_name, lon0=lon[0], lon1=lon[1],
            box_brd=box_brd, default_coords=default_coords
        )

        # Apply the selection on data
        selected = data.where(lat_condition & lon_condition, drop=drop)

        # If the selection crosses Greenwich in a 0..360 coordinate
        # system, optionally convert the selected lon coordinates to
        # [-180,180] and sort them so the chosen region is contiguous
        # for plotting. Use xarray operations where possible to stay
        # dask-friendly.
        if to_180 and crossing_greenwich and lon_name in selected.coords:
            lon_da = selected[lon_name]
            # Use xarray.where to compute conversion lazily
            lon_conv = xr.where(lon_da > 180, lon_da - 360, lon_da)

            selected = selected.assign_coords({lon_name: lon_conv})
            # sort longitudes so they are ascending (-80 .. 30)
            selected = selected.sortby(lon_name)

        selected = log_history(
            selected, f"Area selection: lat={lat}, lon={lon}"
        )

        return selected

    def _lon_condition(
        self,
        data,
        lon_name: str,
        lon0: float,
        lon1: float,
        box_brd: bool = True,
        default_coords: dict | None = None,
    ):
        """
    Build longitude selection condition. Support selections that
    cross the coordinate wrap (e.g. across Greenwich).

        Args:
            data: The dataset containing the longitude values.
            lon_name: The name of the longitude variable in the dataset.
            lon0: The first longitude value.
            lon1: The second longitude value.
            box_brd: Whether to include the boundaries in the selection.
            default_coords: The default coordinate system boundaries.

        Returns:
            A boolean mask for selecting the appropriate longitude values.
        """
        default_coords = default_coords or DEFAULT_COORDS

        lon = data[lon_name]

        # Normal case
        if lon0 <= lon1:
            return (
                (lon >= lon0) & (lon <= lon1) if box_brd
                else (lon > lon0) & (lon < lon1)
            )
        else:
            # Across Greenwich
            return (
                (lon >= lon0) & (lon <= default_coords["lon_max"])
            ) | (
                (lon >= default_coords["lon_min"]) & (lon <= lon1)
            ) if box_brd else (
                (lon > lon0) & (lon <= default_coords["lon_max"])
            ) | (
                (lon >= default_coords["lon_min"]) & (lon < lon1)
            )
