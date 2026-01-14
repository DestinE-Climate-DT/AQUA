"""
Class for supplementary coordinate and dimension fixes beyond base data model.
Works BEFORE base data model transformation (DataModel/CoordTransformer).
"""
import xarray as xr
from aqua.core.logger import log_history, log_configure


class FixerDataModel:
    """
    Apply supplementary coordinate and dimension fixes beyond base data model.
    
    Works BEFORE base data model transformation by DataModel/CoordTransformer.
    Handles additional fixes specified in experiment-specific YAML files.
    
    This class handles:
    - Coordinate renaming (source → target)
    - Units override (without conversion)
    - Dimension renaming
    
    Args:
        fixes (dict): Dictionary containing the fixes from YAML file.
        loglevel (str): Log level for logging. Defaults to 'WARNING'.
        
    """

    def __init__(self, fixes=None, loglevel='WARNING'):
        self.fixes = fixes
        self.logger = log_configure(log_level=loglevel, log_name='FixerDataModel')
        self.loglevel = loglevel

    def apply(self, data: xr.Dataset) -> xr.Dataset:
        """
        Apply supplementary coordinate and dimension fixes.
        
        This method should be called AFTER DataModel.apply() has been run.
        It applies experiment-specific fixes that override or supplement
        the base data model transformations.

        Args:
            data (xr.Dataset): Dataset already processed by DataModel

        Returns:
            xr.Dataset: Dataset with supplementary fixes applied
        """
        if self.fixes is None:
            self.logger.debug("No supplementary fixes to apply")
            return data
        
        # Apply supplementary fixes from YAML
        data = self._fix_dims(data)
        data = self._fix_coord(data)

        return data

    def _fix_coord(self, data: xr.Dataset):
        """
        Apply supplementary coordinate fixes from fixes file.
        Handles coordinate renaming and units override.

        Args:
            data (xr.Dataset): Input dataset

        Returns:
            xr.Dataset: Dataset with fixed coordinates
        """
        coords_fix = self.fixes.get("coords", None)
        if not coords_fix:
            return data

        coords = list(coords_fix.keys())
        self.logger.debug("Supplementary coordinate fixes: %s", coords)

        for coord in coords:
            src_coord = coords_fix[coord].get("source", None)
            tgt_units = coords_fix[coord].get("tgt_units", None)

            if src_coord:
                if src_coord in data.coords:
                    data = data.rename({src_coord: coord})
                    self.logger.debug("Coordinate %s renamed to %s", src_coord, coord)
                    log_history(data[coord], f"Coordinate {src_coord} renamed to {coord} by FixerDataModel")
                else:
                    self.logger.warning("Coordinate %s not found", src_coord)

            if tgt_units:
                if coord in data.coords:
                    self.logger.debug("Coordinate %s units overridden to %s", coord, tgt_units)
                    self.logger.warning("Units override applied - no conversion performed")
                    data[coord].attrs['units'] = tgt_units
                    log_history(data[coord], f"Coordinate {coord} units set to {tgt_units} by FixerDataModel")
                else:
                    self.logger.warning("Coordinate %s not found", coord)

        return data
    
    def _fix_dims(self, data: xr.Dataset):
        """
        Apply supplementary dimension fixes from fixes file.

        Args:
            data (xr.Dataset): Input dataset

        Returns:
            xr.Dataset: Dataset with fixed dimensions
        """
        dims_fix = self.fixes.get("dims", None)
        if not dims_fix:
            return data

        dims = list(dims_fix.keys())
        self.logger.debug("Supplementary dimension fixes: %s", dims)

        for dim in dims:
            src_dim = dims_fix[dim].get("source", None)

            if src_dim and src_dim in data.dims:
                data = data.rename_dims({src_dim: dim})
                self.logger.debug("Dimension %s renamed to %s", src_dim, dim)
                log_history(data, f"Dimension {src_dim} renamed to {dim} by FixerDataModel")
            else:
                if src_dim:
                    self.logger.warning("Dimension %s not found", src_dim)

        return data
