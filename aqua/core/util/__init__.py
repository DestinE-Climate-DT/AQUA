"""Utilities module"""
from importlib import import_module

from .cli_util import template_parse_arguments
from .eccodes import get_eccodes_attr
from .metadata import RegridderMetadata
from .io_util import files_exist, create_folder, file_is_complete
from .io_util import update_metadata
from .realizations import format_realization, get_realizations, DEFAULT_REALIZATION
from .sci_util import lon_to_180, lon_to_360, check_coordinates
from .sci_util import select_season, merge_attrs
from .string import generate_random_string, strlist_to_phrase, lat_to_phrase
from .string import clean_filename, extract_literal_and_numeric, unit_to_latex
from .util import expand_env_vars, extract_attrs, get_arg, to_list, check_attrs, set_attrs
from .yaml import load_yaml, dump_yaml, load_multi_yaml
from .time import check_chunk_completeness, check_seasonal_chunk_completeness
from .time import frequency_string_to_pandas, pandas_freq_to_string, pandas_freq_to_offset
from .time import time_to_string, int_month_name, xarray_to_pandas_freq
from .time import fix_calendar
from aqua.core.default import DEFAULT_TIME_UNIT

# Heavy helpers (matplotlib, cartopy, metpy, kerchunk) are imported on first access (PEP 562);
# catalog_entry is lazy as well, since it imports aqua.core.configurer which imports this package
_LAZY_IMPORTS = {
    **dict.fromkeys(["replace_intake_vars", "replace_urlpath_jinja", "replace_urlpath_wildcard"], ".catalog_entry"),
    **dict.fromkeys(["add_cyclic_lon", "plot_box", "minmax_maps",
                     "evaluate_colorbar_limits", "cbar_get_label", "set_map_title",
                     "coord_names", "ticks_round", "set_ticks", "generate_colorbar_ticks",
                     "apply_circular_window",
                     "get_nside", "get_npix", "healpix_resample",
                     "prettify_levels", "get_decimals"], ".graphics"),
    "get_projection": ".projections",
    **dict.fromkeys(["multiply_units", "normalize_units", "convert_units", "convert_data_units"], ".units"),
    "create_zarr_reference": ".zarr",
}


def __getattr__(name):
    if name in _LAZY_IMPORTS:
        value = getattr(import_module(_LAZY_IMPORTS[name], __name__), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(_LAZY_IMPORTS))


__all__ = ['replace_intake_vars', 'replace_urlpath_jinja', 'replace_urlpath_wildcard',
           'template_parse_arguments',
           'get_eccodes_attr',
           'RegridderMetadata',
           'add_cyclic_lon', 'plot_box', 'minmax_maps',
           'evaluate_colorbar_limits', 'cbar_get_label', 'set_map_title',
           'coord_names', 'ticks_round', 'set_ticks', 'generate_colorbar_ticks',
           'apply_circular_window',
           'get_nside', 'get_npix', 'healpix_resample',
           'prettify_levels', 'get_decimals',
           'files_exist', 'create_folder', 'file_is_complete',
           'update_metadata',
           'get_projection',
           'format_realization', 'get_realizations', 'DEFAULT_REALIZATION',
           'lon_to_180', 'lon_to_360', 'check_coordinates',
           'select_season', 'merge_attrs',
           'generate_random_string', 'strlist_to_phrase', 'lat_to_phrase',
           'clean_filename', 'extract_literal_and_numeric', 'unit_to_latex',
           'multiply_units', 'normalize_units', 'convert_units', 'convert_data_units',
           'expand_env_vars', 'extract_attrs', 'get_arg', 'to_list', 'check_attr', 'set_attrs',
           'load_yaml', 'dump_yaml', 'load_multi_yaml', 'pandas_freq_to_offset',
           'check_chunk_completeness', 'frequency_string_to_pandas', 'pandas_freq_to_string',
           'time_to_string', 'int_month_name',  'xarray_to_pandas_freq', 'check_seasonal_chunk_completeness',
           'fix_calendar', 'DEFAULT_TIME_UNIT',
           'create_zarr_reference',
           ]
