"""Class that uses the AQUA internal data model"""
import xarray as xr
from aqua.data_model import CoordTransformer
from aqua.logger import log_history, log_configure

DEFAULT_DATAMODEL = "aqua"

class FixerDataModel:
    """
    Class that uses the AQUA internal data model to apply to the data
    Further fixes to coordinates and dimensions can be applied.

    Args:
        fixes (dict): Dictionary containing the fixes to be applied.
        loglevel (int, optional): Log level for logging. Defaults to None.
    """ 

    def __init__(self, fixes=None, loglevel=None):

        self.fixes = fixes
        self.logger = log_configure(log_level=loglevel, log_name='FixerDataModel')
        self.loglevel = loglevel

    def apply_datamodel(self, data: xr.Dataset):
        """
        Apply fixes to the data model

        Arguments:
            data (xr.Dataset):  input dataset to process

        Returns:
            The processed input dataset
        """
        if self.fixes is None:
            return data
        
        datamodel = self.fixes.get("data_model", DEFAULT_DATAMODEL)
        if datamodel:
            data = CoordTransformer(data, loglevel=self.loglevel).transform_coords(name=datamodel)

        # Extra coordinate handling
        data = self._fix_dims(data)
        data = self._fix_coord(data)

        return data


    def fix_area(self, area: xr.DataArray):
        """
        Apply fixes to the area file

        Arguments:
            area (xr.DataArray):  area file to be fixed

        Returns:
            The fixed area file (xr.DataArray)
        """
        if self.fixes is None:  # No fixes available
            return area
        else:
            self.logger.debug("Applying fixes to area file")
            # This operation is a duplicate, rationalization with fixer method is needed
            #src_datamodel = self.fixes_dictionary["defaults"].get("src_datamodel", None)
            #src_datamodel = self.fixes.get("data_model", src_datamodel)

            #if src_datamodel:
            #    area = self.change_coord_datamodel(area, src_datamodel, self.dst_datamodel)
            area = CoordTransformer(area, loglevel=self.loglevel).transform_coords()

            return area

    def _fix_coord(self, data: xr.Dataset):
        """
        Other than the data_model we can apply other fixes to the coordinates
        reading them from the fixes file, in the coords section.
        Units override can also be specified.

        Arguments:
            data (xr.Dataset):  input dataset to process

        Returns:
            The processed input dataset
        """
        if self.fixes is None:
            return data

        coords_fix = self.fixes.get("coords", None)

        if coords_fix:
            coords = list(coords_fix.keys())
            self.logger.debug("Coordinates to be checked: %s", coords)

            for coord in coords:
                src_coord = coords_fix[coord].get("source", None)
                tgt_units = coords_fix[coord].get("tgt_units", None)

                if src_coord:
                    if src_coord in data.coords:
                        data = data.rename({src_coord: coord})
                        self.logger.debug("Coordinate %s renamed to %s", src_coord, coord)
                        log_history(data[coord], f"Coordinate {src_coord} renamed to {coord} by fixer")
                    else:
                        self.logger.warning("Coordinate %s not found", src_coord)

                if tgt_units:
                    if coord in data.coords:
                        self.logger.debug("Coordinate %s units set to %s", coord, tgt_units)
                        self.logger.debug("Please notice that this is an override, no unit conversion has been applied")
                        data[coord].attrs['units'] = tgt_units
                        log_history(data[coord], f"Coordinate {coord} units set to {tgt_units} by fixer")
                    else:
                        self.logger.warning("Coordinate %s not found", coord)
        # By Maqsood
        # Addition only for EERIE IFS-FESOM 2D data where there is an extra coordinate of 'depth'
        # Somehow the fixers are not working and I am unable to drop the 'depth' in 2D data.
        # Following is a hardcoded fixer

        drop_coords = self.fixes.get("drop_coords", None)
        self.logger.info("Coordinates to be dropped %s", drop_coords) 
        if drop_coords:
            _coords = list(drop_coords.keys())
            self.logger.debug("Coordinates to be dropped: %s", _coords)
            
            for _coord in _coords:
                if _coord in data.coords: 
                    data = data.isel({_coord : 0}, drop=True)
 
        #if "depth" in data.coords and data.dims["depth"] == 1:   
        #    self.logger.info("Hard coded: 'Depth' coordinates to be dropped in 2D case")
        #    data =  data.isel(depth=0,drop=True) 

        return data
    
    def _fix_dims(self, data: xr.Dataset):
        """
        Other than the data_model we can apply other fixes to the dimensions
        reading them from the fixes file, in the dims section.

        Arguments:
            data (xr.Dataset):  input dataset to process

        Returns:
            The processed input dataset
        """
        if self.fixes is None:
            return data

        dims_fix = self.fixes.get("dims", None)

        if dims_fix:
            dims = list(dims_fix.keys())
            self.logger.debug("Dimensions to be checked: %s", dims)

            for dim in dims:
                src_dim = dims_fix[dim].get("source", None)

                if src_dim and src_dim in data.dims:
                    data = data.rename_dims({src_dim: dim})
                    self.logger.debug("Dimension %s renamed to %s", src_dim, dim)
                    log_history(data, f"Dimension {src_dim} renamed to {dim} by fixer")
                else:
                    self.logger.warning("Dimension %s not found", dim)

        return data
