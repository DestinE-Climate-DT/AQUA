"""Filename generation for regridding areas and weights."""

import os
import re

from smmregrid.util import check_gridfile

from aqua.core.default import DEFAULT_DIMENSION, DEFAULT_DIMENSION_MASK, DEFAULT_WEIGHTS_AREAS_PARAMETERS
from aqua.core.logger import log_configure

from .regridder_util import get_grid_path


class FilenameHandler:
    """Generates area and weights filenames for the Regridder, handling
    grid-based, catalog-based and path-based (data-derived) naming schemes."""

    def __init__(self, cfg_grid_dict: dict, src_grid_name: str = None, src_grid_path: dict = None, loglevel: str = "WARNING"):
        """
        Args:
            cfg_grid_dict (dict): Full AQUA grid configuration (with 'areas', 'weights', 'paths' blocks).
            src_grid_name (str, optional): Source grid name.
            src_grid_path (dict, optional): Source grid path dict, keyed by vertical dimension.
            loglevel (str): Logging level.
        """
        self.cfg_grid_dict = cfg_grid_dict or {}
        self.src_grid_name = src_grid_name
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name="FilenameHandler")

    def area_filename(self, tgt_grid_name, src_grid_path, regridder_metadata):
        """Generate the area filename (grid-based, catalog-based or path-based)."""
        area_dict = self.cfg_grid_dict.get("areas")

        if not area_dict:
            self.logger.warning("Areas block not found in the configuration file, using fallback naming scheme.")
            filename = (
                f"cell_area_{tgt_grid_name}.nc" if tgt_grid_name else self._fallback_filename(regridder_metadata, kind="area")
            )
        elif tgt_grid_name:
            filename = area_dict["template_grid"].format(grid=tgt_grid_name)
        elif check_gridfile(get_grid_path(src_grid_path)) != "xarray":
            filename = area_dict["template_grid"].format(grid=self.src_grid_name)
        else:
            filename = self._resolve_source_filename(area_dict, regridder_metadata, kind="area")

        filename = self._insert_metadata_params(filename, regridder_metadata)
        return self._prepend_path(filename, kind="areas")

    def weights_filename(self, tgt_grid_name, src_grid_path, regrid_method, mask_dim, regridder_metadata):
        """Generate the weights filename (grid-based, catalog-based or path-based)."""
        levname = mask_dim if mask_dim in [DEFAULT_DIMENSION, DEFAULT_DIMENSION_MASK] else f"3d-{mask_dim}"
        weights_dict = self.cfg_grid_dict.get("weights")

        if not weights_dict:
            self.logger.warning("Weights block not found in the configuration file, using fallback naming scheme.")
            filename = f"weights_{tgt_grid_name}_{regrid_method}_l{levname}.nc"
        elif check_gridfile(get_grid_path(src_grid_path)) != "xarray":
            filename = weights_dict["template_grid"].format(
                sourcegrid=self.src_grid_name, method=regrid_method, targetgrid=tgt_grid_name, level=levname
            )
        else:
            filename = self._resolve_source_filename(
                weights_dict, regridder_metadata, kind="weights", method=regrid_method, targetgrid=tgt_grid_name, level=levname
            )

        filename = self._insert_metadata_params(filename, regridder_metadata)
        return self._prepend_path(filename, kind="weights")

    def _resolve_source_filename(self, config_dict, regridder_metadata, kind="area", **extra_params):
        """Resolve filename for data-derived grids (catalog-based or path-based), with fallback."""
        if regridder_metadata and regridder_metadata.is_catalog_based():
            template = config_dict.get("template_default")
            if not template:
                raise ValueError(f"template_default missing in {kind} config")
            template_vars = regridder_metadata.to_template_dict()
            template_vars.update(extra_params)
            return template.format(**template_vars)

        if regridder_metadata and regridder_metadata.is_path_based():
            template_path = config_dict.get("template_path")
            if template_path:
                template_vars = {"path_id": regridder_metadata.get_path_identifier()}
                template_vars.update(extra_params)
                return template_path.format(**template_vars)
            self.logger.warning("No template_path in config, using fallback for %s", kind)
            return self._fallback_filename(regridder_metadata, kind, **extra_params)

        self.logger.warning("No valid regridder metadata, using fallback")
        return self._fallback_filename(regridder_metadata, kind, **extra_params)

    def _fallback_filename(self, regridder_metadata, kind="area", **extra_params):
        """Fallback filename when no template is available."""
        if kind == "area":
            if self.src_grid_name:
                return f"cell_area_{self.src_grid_name}.nc"
            if regridder_metadata and regridder_metadata.path:
                return f"cell_area_path_{regridder_metadata.get_path_identifier()}.nc"
            self.logger.warning("No metadata or src_grid_name available, using 'cell_area_unknown.nc'.")
            return "cell_area_unknown.nc"

        method = extra_params.get("method", "unknown")
        targetgrid = extra_params.get("targetgrid", "unknown")
        level = extra_params.get("level", "2d")

        if regridder_metadata and regridder_metadata.path:
            return f"weights_path_{regridder_metadata.get_path_identifier()}_{method}_{targetgrid}_l{level}.nc"
        if self.src_grid_name:
            return f"weights_{self.src_grid_name}_{method}_{targetgrid}_l{level}.nc"
        self.logger.warning("No metadata or src_grid_name available, using 'weights_unknown_...'.")
        return f"weights_unknown_{method}_{targetgrid}_l{level}.nc"

    def _insert_metadata_params(self, filename, regridder_metadata):
        """Insert extra params from DEFAULT_WEIGHTS_AREAS_PARAMETERS into filename."""
        if not regridder_metadata:
            return filename
        for param in DEFAULT_WEIGHTS_AREAS_PARAMETERS:
            value = getattr(regridder_metadata, param, None)
            if value is not None:
                filename = re.sub(r".nc$", f"_{param}{value}.nc", filename)
        return filename

    def _prepend_path(self, filename, kind="weights"):
        """Prepend configured output path to filename, creating it if needed."""
        paths = self.cfg_grid_dict.get("paths")
        if not paths:
            self.logger.warning("Paths block not found in the configuration file, using present directory.")
            return filename
        path = paths.get(kind)
        if not path:
            self.logger.warning("%s block not found in the paths block, using present directory.", kind)
            return filename
        if not os.path.exists(path):
            self.logger.warning("%s path in %s does not exist: creating!", kind, path)
            os.makedirs(path, exist_ok=True)
        return os.path.join(path, filename)
