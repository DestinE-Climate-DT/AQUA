import xarray as xr
from aqua.logger import log_configure, log_history
from aqua.util import lon_to_360, lon_to_180

# set default options for xarray
xr.set_options(keep_attrs=True)



class AreaSelection:
    """Class to select an area from an xarray Dataset."""

    def __init__(self, loglevel: str = "WARNING"):
        """
        Initialize the AreaSelection.

        Args:
            loglevel (str, optional): The logging level. Default is "WARNING".
        """
        self.logger = log_configure(log_level=loglevel, log_name="AreaSelection")

    def select_area(self, data: xr.Dataset | xr.DataArray,
                    lon: list | None = None, lat: list | None = None,
                    box_brd: bool = True, drop: bool = False,
                    lat_name: str = "lat", lon_name: str = "lon",
                    default_coords: dict = {"lat_min": -90, "lat_max": 90,
                                            "lon_min": 0, "lon_max": 360}) -> xr.Dataset | xr.DataArray:
        """
        Select a specific area from the dataset based on longitude and latitude ranges.

        Args:
            data (xr.Dataset or xr.DataArray): The input dataset or data array.
            lon (list, optional): The longitude range to select.
            lat (list, optional): The latitude range to select.
            box_brd (bool, optional): Whether to include the box boundaries. Default is True.
            drop (bool, optional): Whether to drop the non-selected data. Default is False.
            lat_name (str, optional): The name of the latitude coordinate. Default is "lat".
            lon_name (str, optional): The name of the longitude coordinate. Default is "lon".
            default_coords (dict, optional): The default coordinate ranges. Default is {"lat_min": -90, "lat_max": 90,
                                               "lon_min": 0, "lon_max": 360}).

        Returns:
            xr.Dataset or None: The selected area dataset or None if no area is selected.
        """
        # By default we work with the AQUA data_model but we keep the
        # flexibility to adapt to other data models.
        if lat_name not in data.coords or lon_name not in data.coords:
            raise KeyError(f"Latitude or Longitude coordinates not found. "
                           f"Expected '{lat_name}' and '{lon_name}'.")

        # If both lon and lat are None, no selection is needed
        if lon is None and lat is None:
            return data

        lon, lat = self._check_coordinates(lon, lat, default_coords)

        # Building the mask
        lat_condition = (data[lat_name] >= lat[0]) & (data[lat_name] <= lat[1]) if box_brd \
            else (data[lat_name] > lat[0]) & (data[lat_name] < lat[1])
        lon_condition = self._lon_condition(data, lon_name=lon_name, lon0=lon[0], lon1=lon[1],
                                            box_brd=box_brd, default_coords=default_coords)

        # Apply the selection on data
        selected = data.where(lat_condition & lon_condition, drop=drop)
        selected = log_history(selected, f"Area selection: lat={lat}, lon={lon}")

        return selected

    def _check_coordinates(self, lon: list | None, lat: list | None,
                          default_coords: dict) -> tuple[list, list]:
        """
        Validate and normalize latitude/longitude ranges.

        Returns:
            tuple: (lon_range, lat_range) with values mapped to default system.
        """
        self.logger.debug("Input coordinates: lat=%s, lon=%s", lat, lon)

        # --- Latitude ---
        # Populate with maximum extent if no Latitude is provided
        if lat is None:
            lat = [default_coords["lat_min"], default_coords["lat_max"]]
        # Swap if values are inverted
        elif lat[0] > lat[1]:
            self.logger.warning(f"Latitude: {lat[0]} > {lat[1]}, swapping values!")
            lat = [lat[1], lat[0]]
        # Check that values are within the maximum range
        if lat[0] < default_coords["lat_min"] or lat[1] > default_coords["lat_max"]:
            raise ValueError(f"Latitude must be within {default_coords['lat_min']} and {default_coords['lat_max']}")
        # If the two latitudes are equal raise an error
        if lat[0] == lat[1]:
            raise ValueError(f"Both latitude values are equal: {lat[0]}, please provide a valid range.")

        # --- Longitude ---
        # Populate with maximum extent if no Longitude is provided
        if lon is None:
            lon = [default_coords["lon_min"], default_coords["lon_max"]]
        else:
            # Normalize according to coordinate system
            if default_coords["lon_min"] == 0 and default_coords["lon_max"] == 360:
                lon = [lon_to_360(l) for l in lon]
            elif default_coords["lon_min"] == -180 and default_coords["lon_max"] == 180:
                lon = [lon_to_180(l) for l in lon]
            else:
                raise ValueError("Unsupported default longitude system.")
        # If the two longitudes are equal raise an error
        if lon[0] == lon[1]:
            raise ValueError(f"Longitude: {lon[0]} == {lon[1]}, please provide a valid range.")

        self.logger.debug("Normalized coordinates: lat=%s, lon=%s", lat, lon)
        return lon, lat

    def _lon_condition(self, data, lon_name: str, lon0: float, lon1: float,
                       box_brd: bool = True, default_coords: dict = {"lat_min": -90, "lat_max": 90,
                                                                     "lon_min": 0, "lon_max": 360}):
        """
        Build longitude selection condition, supporting across-max value ranges.

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
        lon = data[lon_name]

        # Normal case
        if lon0 <= lon1:
            return (lon >= lon0) & (lon <= lon1) if box_brd else (lon > lon0) & (lon < lon1)
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