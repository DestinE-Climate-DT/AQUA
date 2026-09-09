"""Utilities module"""

from .catalog_entry import replace_intake_vars, replace_urlpath_jinja, replace_urlpath_wildcard
from .cli_util import template_parse_arguments
from .eccodes import get_eccodes_attr
from .graphics import (
           add_cyclic_lon,
           apply_circular_window,
           cbar_get_label,
           coord_names,
           evaluate_colorbar_limits,
           generate_colorbar_ticks,
           get_decimals,
           get_npix,
           get_nside,
           healpix_resample,
           minmax_maps,
           plot_box,
           prettify_levels,
           set_map_title,
           set_ticks,
           ticks_round,
)
from .io_util import create_folder, file_is_complete, files_exist, update_metadata
from .projections import get_projection
from .realizations import DEFAULT_REALIZATION, format_realization, get_realizations
from .sci_util import check_coordinates, find_vert_coord, lon_to_180, lon_to_360, merge_attrs, select_season
from .string import (
           clean_filename,
           extract_literal_and_numeric,
           generate_random_string,
           lat_to_phrase,
           strlist_to_phrase,
           unit_to_latex,
)
from .time import (
           check_chunk_completeness,
           check_seasonal_chunk_completeness,
           default_time_unit,
           fix_calendar,
           frequency_string_to_pandas,
           int_month_name,
           pandas_freq_to_offset,
           pandas_freq_to_string,
           time_to_string,
           xarray_to_pandas_freq,
)
from .units import convert_data_units, convert_units, multiply_units, normalize_units
from .util import expand_env_vars, extract_attrs, get_arg, to_list
from .yaml import dump_yaml, load_multi_yaml, load_yaml
from .zarr import create_zarr_reference

__all__ = ['replace_intake_vars', 'replace_urlpath_jinja', 'replace_urlpath_wildcard',
           'template_parse_arguments',
           'get_eccodes_attr',
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
           'select_season', 'merge_attrs', 'find_vert_coord',
           'generate_random_string', 'strlist_to_phrase', 'lat_to_phrase',
           'clean_filename', 'extract_literal_and_numeric', 'unit_to_latex',
           'multiply_units', 'normalize_units', 'convert_units', 'convert_data_units',
           'expand_env_vars', 'extract_attrs', 'get_arg', 'to_list',
           'load_yaml', 'dump_yaml', 'load_multi_yaml',
           'check_chunk_completeness', 'frequency_string_to_pandas', 'pandas_freq_to_string',
           'time_to_string', 'int_month_name',  'xarray_to_pandas_freq', 'pandas_freq_to_offset',
           'check_seasonal_chunk_completeness',
           'fix_calendar', 'default_time_unit',
           'create_zarr_reference',
           ]
