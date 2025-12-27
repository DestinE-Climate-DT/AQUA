"""Utilities module"""

from .catalog_entry import replace_intake_vars, replace_urlpath_jinja, replace_urlpath_wildcard
from .cli_util import template_parse_arguments
from .eccodes import get_eccodes_attr
from .graphics import add_cyclic_lon, plot_box, minmax_maps
from .graphics import evaluate_colorbar_limits, cbar_get_label, set_map_title
from .graphics import coord_names, ticks_round, set_ticks, generate_colorbar_ticks
from .graphics import apply_circular_window
from .graphics import get_nside, get_npix, healpix_resample
from .io_util import files_exist, create_folder, file_is_complete
from .io_util import add_pdf_metadata, add_png_metadata, update_metadata
from .projections import get_projection
from .realizations import format_realization, get_realizations, DEFAULT_REALIZATION
from .sci_util import lon_to_180, lon_to_360, check_coordinates
from .sci_util import select_season, merge_attrs, find_vert_coord
from .string import generate_random_string, strlist_to_phrase, lat_to_phrase
from .string import clean_filename, extract_literal_and_numeric, unit_to_latex
from .units import multiply_units, normalize_units, convert_units, convert_data_units
from .util import expand_env_vars, extract_attrs, get_arg, to_list, username
from .yaml import load_yaml, dump_yaml, load_multi_yaml
from .time import check_chunk_completeness, frequency_string_to_pandas, pandas_freq_to_string
from .time import time_to_string, int_month_name, xarray_to_pandas_freq, check_seasonal_chunk_completeness
from .zarr import create_zarr_reference

__all__ = ['replace_intake_vars', 'replace_urlpath_jinja', 'replace_urlpath_wildcard', 
           'template_parse_arguments',
           'get_eccodes_attr',
           'add_cyclic_lon', 'plot_box', 'minmax_maps',
           'evaluate_colorbar_limits', 'cbar_get_label', 'set_map_title',
           'coord_names', 'ticks_round', 'set_ticks', 'generate_colorbar_ticks',
           'apply_circular_window',
           'get_nside', 'get_npix', 'healpix_resample',
           'files_exist', 'create_folder', 'file_is_complete',
           'add_pdf_metadata', 'add_png_metadata', 'update_metadata',
           'get_projection',
           'format_realization', 'get_realizations', 'DEFAULT_REALIZATION',
           'lon_to_180', 'lon_to_360', 'check_coordinates',
           'select_season', 'merge_attrs', 'find_vert_coord',
           'generate_random_string', 'strlist_to_phrase', 'lat_to_phrase', 
           'clean_filename', 'extract_literal_and_numeric', 'unit_to_latex',
           'multiply_units', 'normalize_units', 'convert_units', 'convert_data_units',
           'expand_env_vars', 'extract_attrs', 'get_arg', 'to_list','username',
           'load_yaml', 'dump_yaml', 'load_multi_yaml',
           'check_chunk_completeness', 'frequency_string_to_pandas', 'pandas_freq_to_string',
           'time_to_string', 'int_month_name',  'xarray_to_pandas_freq', 'check_seasonal_chunk_completeness',
           'create_zarr_reference',
           ]
