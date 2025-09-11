"""Module to transform coordinates of an Xarray object."""

import os
import xarray as xr
from metpy.units import units
from aqua.logger import log_configure, log_history
from aqua.util import load_yaml, ConfigPath
from .coordidentifier import CoordIdentifier
from pint.errors import DimensionalityError


# Function to get the conversion factor
def units_conversion_factor(from_unit_str, to_unit_str):
    """
    Get the conversion factor between two units.
    """
    from_unit = units(from_unit_str)
    to_unit = units(to_unit_str)
    try:
        return from_unit.to(to_unit).magnitude
    except DimensionalityError:
        return None



class CoordTransformer():
    """
    Class to transform coordinates of an Xarray object.
    It aims at transforming the coordinates provided by the user into
    a standard format.
    """

    def __init__(self, data, loglevel='WARNING'):
        """
        Constructor of the CoordTransator class.

        Args:
            data (xr.Dataset or xr.DataArray): Xarray Dataset or DataArray object.
            loglevel (str, optional): Log level. Defaults to 'WARNING'.
        """
        if not isinstance(data, (xr.Dataset, xr.DataArray)):
            raise TypeError("data must be an Xarray Dataset or DataArray object.")
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'CoordTransformer')

        self.data = data

        self.src_coords = CoordIdentifier(data.coords, loglevel=loglevel).identify_coords()
        self.tgt_coords = None
        self.gridtype = self._info_grid(data.coords)
        self.logger.info("Grid type: %s", self.gridtype)

    def load_data_model(self, name: str = "aqua"):
        """
        Load the default data model from the aqua.yaml file.

        Args:
            name (str): An installed data_model into aqua config, i.e. a YAML file

        Returns:
            dict: Target coordinates dictionary.
            str: Name of the target data model.
        """

        data_model_dir = os.path.join(ConfigPath().get_config_dir(), "data_model")
        data_model_file = os.path.join(data_model_dir, f"{name}.yaml")
        if not os.path.exists(data_model_file):
            raise FileNotFoundError(f"Data model file {data_model_file} not found.")
        self.logger.info("Loading data model from %s", data_model_file)
        data_yaml = load_yaml(data_model_file)
        return data_yaml

    def _info_grid(self, coords):
        """
        Identify the grid type of the Xarray object.
        To be used to check if the axis direction has to be reversed. 
        Args: 
            coords (xr.Coordinates): Coordinates of the Xarray object.
        Returns:
            str: The grid type of the Xarray object. 
            It can be Regular, Curvilinear or Unstructured.
        """

        lonname = self.src_coords.get('latitude')
        latname = self.src_coords.get('longitude')
        if lonname is None or latname is None:
            return "Unknown"

        lat = coords[latname.get('name')]
        lon = coords[lonname.get('name')]
        if lon.ndim == 2 and lat.ndim == 2:
            return "Curvilinear"
        if lon.dims != lat.dims:
            return "Regular"
        return "Unstructured"
    
    def transform_coords(self, name="aqua"):
        """
        Transform the coordinates of the Xarray object.

        Args:
            tgt_coords (dict, optional): Target coordinates dictionary. Defaults to None.

        Returns:
            xr.Dataset or xr.DataArray: The transformed dataset or dataarray.
        """

        if not isinstance(name, str):
            raise TypeError("name must be a string.")

        # multiple safety check
        self.logger.info("Target data model: %s", name)
        data_yaml = self.load_data_model(name)
        self.tgt_coords = data_yaml.get('data_model')
        outname = f"{data_yaml.get('name')} v{str(data_yaml.get('version'))}"

        if not isinstance(self.tgt_coords, dict):
            raise TypeError("tgt_coords must be a dictionary.")
        
        data = self.data
        for coord in self.tgt_coords:
            if coord in self.src_coords and self.src_coords[coord]:
                tgt_coord = self.tgt_coords[coord]
                src_coord = self.src_coords[coord]
                self.logger.info("Analysing coordinate: %s", coord)
                #self.logger.info("Transforming coordinate %s to %s", src_coord, tgt_coord)
                data = self.rename_coordinate(data, src_coord, tgt_coord)
                data = self.reverse_coordinate(data, src_coord, tgt_coord)
                data = self.convert_units(data, src_coord, tgt_coord)
                data = self.assign_attributes(data, tgt_coord)
            else:
                self.logger.info("Coordinate %s not found in source coordinates.", coord)

        log_history(data, f"Coordinates fixed toward {outname} datamodel")

        return data
    
    def rename_coordinate(self, data, src_coord, tgt_coord):
        """
        Rename coordinate if necessary.

        Args:
            data (xr.Dataset or xr.DataArray): The Xarray object.
            src_coord (dict): Source coordinate dictionary.
            tgt_coord (dict): Target coordinate dictionary.
        Returns:
            xr.Dataset or xr.DataArray: The Xarray object with renamed coordinate.
        """
        if src_coord['name'] != tgt_coord['name']:
            original_coords = list(data.coords)
            self.logger.info("Renaming coordinate %s to %s",
                            src_coord['name'], tgt_coord['name'])
            data = data.rename({src_coord['name']: tgt_coord['name']})
            log_history(data,
                        f"Renamed coordinate {src_coord['name']} to {tgt_coord['name']} by datamodel")

            # Ensure the AQUA dependent index is preserved
            if f"idx_{src_coord['name']}" in original_coords:
                index_name = f"idx_{src_coord['name']}"
                new_index_name = f"idx_{tgt_coord['name']}"
                self.logger.info("Renaming index %s to %s", index_name, new_index_name)
                data = data.rename({index_name: new_index_name})

            # unclear if this is fundamental
            # if tgt_coord['name'] in data.dims:
            #   self.logger.info("Preserving original dimension %s and index.", src_coord['name'])
            #   data = data.swap_dims({tgt_coord['name']: src_coord['name']})
            #   data = data.set_index({src_coord['name']: tgt_coord['name']})
                
            #data = data.rename({src_coord['name']: tgt_coord['name']})
            tgt_coord['bounds'] = f"{tgt_coord['name']}_bnds"
            data = self._rename_bounds(data, src_coord, tgt_coord)
        return data
    
    def _rename_bounds(self, data, src_coord, tgt_coord):
        """
        Rename bounds if necessary.

        Args:
            data (xr.Dataset or xr.DataArray): The Xarray object.
            src_coord (dict): Source coordinate dictionary.
            tgt_coord (dict): Target coordinate dictionary.
        
        Returns:
            xr.Dataset or xr.DataArray: The Xarray object with renamed bounds.
        """
        if src_coord['bounds'] is not None:
            if src_coord['bounds'] in data:
                self.logger.info("Renaming bounds %s to %s",
                                src_coord['bounds'], tgt_coord['bounds'])
                data = data.rename({src_coord['bounds']: tgt_coord['bounds']})
                data[tgt_coord['name']].attrs['bounds'] = tgt_coord['bounds']
            else:
                self.logger.info("Bounds %s not found in data.", src_coord['bounds'])
        return data

    def reverse_coordinate(self, data, src_coord, tgt_coord):
        """
        Reverse coordinate if necessary.

        Args:
            data (xr.Dataset or xr.DataArray): The Xarray object.
            src_coord (dict): Source coordinate dictionary.
            tgt_coord (dict): Target coordinate dictionary.
        
        Returns:
            xr.Dataset or xr.DataArray: The Xarray object with possibly reversed coordinate.
        """
        if 'stored_direction' not in tgt_coord:
            return data
        if tgt_coord['stored_direction'] not in ["increasing", "decreasing"]:
            raise ValueError(f"tgt direction must be 'increasing' or 'decreasing', not  {tgt_coord['stored_direction']}")
        if src_coord['stored_direction'] not in ["increasing", "decreasing"]:
            self.logger.warning("src direction is not 'increasing' or 'decreasing', but %s. Disabling reverse!", src_coord['stored_direction'])
            return data
        if src_coord['stored_direction'] != tgt_coord['stored_direction']:
            if self.gridtype == "Regular":
                self.logger.info("Reversing coordinate %s from %s to %s",
                                tgt_coord['name'], src_coord['stored_direction'], tgt_coord['stored_direction'])
                data = data.isel({tgt_coord['name']: slice(None, None, -1)})
                # add an attribute for regridder evalution
                data[tgt_coord['name']].attrs['flipped'] = 1
                log_history(data, f"Reversed coordinate {tgt_coord['name']} from {src_coord['stored_direction']} to {tgt_coord['stored_direction']} by datamodel")
            else:
                self.logger.info("Cannot reverse coordinate %s. Grid type is %s.",
                                    tgt_coord['name'], self.gridtype)
        return data
    
    
    def convert_units(self, data, src_coord, tgt_coord):
        """
        Convert units of the coordinate.
        """
        if 'units' not in tgt_coord:
            self.logger.debug("%s unit not found in target data model. Disabling unit conversion.", tgt_coord['name'])
            return data
        if 'units' not in src_coord:
            self.logger.warning("%s unit not found source data model. Disabling unit conversion.", src_coord['name'])
            return data
        if 'units' not in data[tgt_coord['name']].attrs:
            self.logger.warning("%s unit not found in data. Disabling unit conversion.", tgt_coord['name'])
            return data
        # fix for coordinates with spaced units that is not read by metpy
        if src_coord['units'].lower() in ['degree north', 'degree east']:
            src_coord['units'] = src_coord['units'].replace(" ", "_").lower()
        if src_coord['units'] != tgt_coord['units']:
            self.logger.info("Converting units of coordinate %s from %s to %s",
                            src_coord['name'], src_coord['units'], tgt_coord['units'])
            factor = units_conversion_factor(src_coord['units'], tgt_coord['units'])
            if factor is None:
                self.logger.warning(
                    "Incompatible unit conversion for coordinate %s: %s -> %s. Skipping conversion.",
                    src_coord['name'], src_coord['units'], tgt_coord['units']
                )
                return data
            if factor != 0:
                self.logger.info("Conversion factor is: %s ", factor)
                data = data.assign_coords({tgt_coord['name']: data[tgt_coord['name']]*factor})
                tgt_coord['bounds'] = f"{tgt_coord['name']}_bnds"
                data = self._convert_bounds(data, src_coord, tgt_coord, factor)
                log_history(data,
                            f"Converted units of coordinate {src_coord['name']} from {src_coord['units']} to {tgt_coord['units']} by datamodel")
            data[tgt_coord['name']].attrs['units'] = tgt_coord['units']
        return data
    
    def _convert_bounds(self, data, src_coord, tgt_coord, factor):
        """
        Convert units bounds of the coordinate.
        """
        if 'bounds' not in tgt_coord:
            return data
        if tgt_coord['bounds'] in data:
            self.logger.info("Converting bounds of coordinate %s from %s to %s",
                            src_coord['name'], src_coord['units'], tgt_coord['units'])
            data[tgt_coord['bounds']] = data[tgt_coord['bounds']]*factor
            data[tgt_coord['bounds']].attrs['units'] = tgt_coord['units']
        return data

    def assign_attributes(self, data, tgt_coord):
        """
        Assign attributes to the coordinate.
        """
        for key, value in tgt_coord.items():
            if key not in['name', 'units', 'positive', 'stored_direction', 'bounds']:
                if key not in data.coords[tgt_coord['name']].attrs:
                    self.logger.debug("Adding attribute %s to coordinate %s", key, tgt_coord['name'])
                    data.coords[tgt_coord['name']].attrs[key] = value
        return data
    

def counter_reverse_coordinate(data):
    """
    Flip back latitude if necessary
    """

    for coord in data.coords:
        if 'flipped' in data.coords[coord].attrs:
            data = data.isel({coord: slice(None, None, -1)})
            del data.coords[coord].attrs['flipped']
    return data