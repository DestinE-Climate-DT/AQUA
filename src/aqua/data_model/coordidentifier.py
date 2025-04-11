"""
Module to identify the nature of coordinates of an Xarray object.
"""
import xarray as xr
import numpy as np
from metpy.units import units

from aqua.logger import log_configure

LATITUDE = ["latitude", "lat", "nav_lat"]
LONGITUDE = ["longitude", "lon", "nav_lon"]
TIME = ["time", "valid_time", "forecast_period", "time_counter"]
ISOBARIC = ["plev"]
DEPTH = ["depth", "zlev"]

# Define the target dimensionality (pressure)
pressure_dim = units.pascal.dimensionality
#meter_dim = units.meter.dimensionality

# Function to check if a unit is a pressure unit
def is_isobaric(unit):
    """Check if a unit is a pressure unit."""
    try:
        return units(unit).dimensionality == pressure_dim
    except Exception as e:
        return False

class CoordIdentifier():
    """
    Class to identify the nature of coordinates of an Xarray object.
    It aims at detecting the longitude, latitude, time and any other vertical
    by inspecting the attributes of the coordinates provided by the user.

    Args: 
        coords (xarray.Coordinates): The coordinates of Dataset to be analysed.
        loglevel (str): The log level to use. Default is 'WARNING'.
    """

    def __init__(self, coords: xr.Coordinates, loglevel='WARNING'):
        """
        Constructor of the CoordIdentifier class.
        """
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'CoordIdentifier')

        if not isinstance(coords, xr.Coordinates):
            raise TypeError("coords must be an Xarray Coordinates object.")
        self.coords = coords

        # internal name definition for the coordinates
        self.coord_dict = {
            "latitude": [],
            "longitude": [],
            "time": [],
            "isobaric": [],
            "depth": []
        }

    def identify_coords(self):
        """
        Identify the coordinates of the Xarray object.
        """

        # define a dictionary with the methods to identify the coordinates
        coord_methods = {
            "latitude": self._identify_latitude,
            "longitude": self._identify_longitude,
            "isobaric": self._identify_isobaric,
            "depth": self._identify_depth,
            "time": self._identify_time,
        }

        # loop on coordinates provided by the user
        for name, coord in self.coords.items():
            self.logger.debug("Identifying coordinate: %s", name)

            # use the methods to detect the coordinates and assign attributes accordingly
            for coord_name, identify_func in coord_methods.items():
                if identify_func(coord):
                    self.logger.debug(coord_name)
                    if coord_name == "time":
                        self.coord_dict[coord_name].append(self._get_time_attributes(coord))
                    else:
                        self.coord_dict[coord_name].append(self._get_attributes(coord, coord_name=coord_name))
                    continue # loop on the next coordinate

        # check if the coordinates are empty or double!
        self.coord_dict = self._clean_coord_dict()

        return self.coord_dict
    
    def _clean_coord_dict(self):
        """
        Clean the coordinate dictionary.
        Set to None the coordinates that are empty.
        If multiple coordinates are found, keep only the first one and log an error
        """
        for key, value in self.coord_dict.items():
            if len(value) == 0:
                self.coord_dict[key] = None
            elif len(value) == 1:
                self.coord_dict[key] = value[0]
            else:
                self.logger.warning("Multiple %s coordinates found: %s. Disabling data model check for this coordinate.",
                                     key, [x['name'] for x in value])
                self.coord_dict[key] = None
        return self.coord_dict
    
    def _get_time_attributes(self, coord):
        """
        Get the attributes of the time coordinates.

        Args:
            coord (xarray.Coordinates): The coordinate to define the attributes.

        Returns:
            dict: A dictionary containing the attributes of the coordinate.
        """
        return {'name': coord.name,
                'dims:': coord.dims,
                'units': coord.attrs.get('units'),
                'calendar': coord.attrs.get('calendar'),
                'bounds': coord.attrs.get('bounds')}
    
    def _get_attributes(self, coord, coord_name="longitude"):
        """
        Get the attributes of the coordinates.

        Args:
            coord (xarray.Coordinates): The coordinate to define the attributes.
            coord_type (str): The type of coordinate ("horizontal" or "vertical").

        Returns:
            dict: A dictionary containing the attributes of the coordinate.
        """
        coord_range = (coord.values.min(), coord.values.max())
        direction = None
        positive = None
        horizontal = ["longitude", "latitude"]
        vertical = ["depth", "isobaric"]

        if coord.ndim == 1 and coord_name in horizontal:
            direction = "increasing" if coord.values[-1] > coord.values[0] else "decreasing"

        if coord_name in vertical:
            positive = coord.attrs.get('positive')
            if not positive:
                if is_isobaric(coord.attrs.get('units')):
                    positive = "down"
                else:
                    positive = "down" if coord.values[0] > 0 else "up"

        attributes = {
            'name': coord.name,
            'dims': coord.dims,
            'units': coord.attrs.get('units'),
            'range': coord_range,
            'bounds': coord.attrs.get('bounds'),
        }

        if coord_name in horizontal:
            attributes['stored_direction'] = direction
        elif coord_name in vertical:
            attributes['positive'] = positive
        
        if coord_name == "longitude":
            attributes['convention'] = self._guess_longitude_range(coord)
            

        return attributes
    
    @staticmethod
    def _guess_longitude_range(longitude) -> str:
        """
        Guess if the longitude range is from 0 to 360 or from -180 to 180,
        ensuring the grid is global.
        """

        # Guess the longitude range
        if np.any(longitude.values < 0):
            return "centered"
        elif np.any(longitude.values > 180):
            return "positive"
        else:
            return "ambigous"

    @staticmethod
    def _identify_latitude(coord):
        """
        Identify the latitude coordinate of the Xarray object.
        """
        if coord.name in LATITUDE:
            return True
        if coord.attrs.get("standard_name") == "latitude":
            return True
        if coord.attrs.get("axis") == "Y":
            return True
        if coord.attrs.get("units") == "degrees_north":
            return True
        return False
    
    @staticmethod
    def _identify_longitude(coord):
        """
        Identify the longitude coordinate of the Xarray object.
        """
        if coord.name in LONGITUDE:
            return True
        if coord.attrs.get("standard_name") == "longitude":
            return True
        if coord.attrs.get("axis") == "X":
            return True
        if coord.attrs.get("units") == "degrees_east":
            return True
        return False
    
    @staticmethod
    def _identify_time(coord):
        """
        Identify the time coordinate of the Xarray object.
        """
        if coord.name in TIME:
            return True
        if coord.attrs.get("axis") == "T":
            return True
        if coord.attrs.get("standard_name") == "time":
            return True
        return False
    
    @staticmethod
    def _identify_isobaric(coord):
        """
        Identify the isobaric coordinate of the Xarray object.
        """
        if coord.name in ISOBARIC:
            return True
        if coord.attrs.get("standard_name") == "air_pressure":
            return True
        if is_isobaric(coord.attrs.get("units")):
            return True
        return False
    
    @staticmethod
    def _identify_depth(coord):
        """
        Identify the depth coordinate of the Xarray object.
        """
        if coord.name in DEPTH:
            return True
        if coord.attrs.get("standard_name") == "depth":
            return True
        if "depth" in coord.name:
            return True
        if "depth" in coord.attrs.get("long_name", ""):
            return True
        return False
    
    

