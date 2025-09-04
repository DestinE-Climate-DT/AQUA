"""Utilities module"""

from .catalog_entry import replace_intake_vars, replace_urlpath_jinja, replace_urlpath_wildcard
from .config import ConfigPath
from .eccodes import get_eccodes_attr
from .units import normalize_units, convert_units, convert_data_units
from .graphics import add_cyclic_lon, plot_box, minmax_maps
from .graphics import evaluate_colorbar_limits, cbar_get_label, set_map_title
from .graphics import coord_names, ticks_round, set_ticks, generate_colorbar_ticks
from .graphics import apply_circular_window
from .graphics import get_nside, get_npix, healpix_resample
from .projections import get_projection
from .realizations import format_realization, DEFAULT_REALIZATION
from .sci_util import area_selection, check_coordinates, select_season, merge_attrs
from .string import strlist_to_phrase, lat_to_phrase
from .util import generate_random_string, get_arg, create_folder, to_list
from .util import file_is_complete, files_exist, find_vert_coord
from .util import extract_literal_and_numeric, extract_attrs, add_pdf_metadata, add_png_metadata
from .util import open_image, username, update_metadata
from .yaml import load_yaml, dump_yaml, load_multi_yaml
from .time import check_chunk_completeness, frequency_string_to_pandas, pandas_freq_to_string
from .time import time_to_string, int_month_name, xarray_to_pandas_freq
from .zarr import create_zarr_reference

__all__ = ['replace_intake_vars', 'replace_urlpath_jinja', 'replace_urlpath_wildcard',
           'ConfigPath',
           'get_eccodes_attr',
           'normalize_units', 'convert_units', 'convert_data_units',
           'add_cyclic_lon', 'plot_box', 'minmax_maps',
           'evaluate_colorbar_limits', 'cbar_get_label', 'set_map_title',
           'coord_names', 'ticks_round', 'set_ticks', 'generate_colorbar_ticks',
           'apply_circular_window',
           'get_nside', 'get_npix', 'healpix_resample',
           'get_projection',
           'format_realization', 'DEFAULT_REALIZATION',
           'area_selection', 'check_coordinates', 'select_season', 'merge_attrs',
           'strlist_to_phrase', 'lat_to_phrase',
           'generate_random_string', 'get_arg', 'create_folder', 'to_list',
           'file_is_complete', 'files_exist', 'find_vert_coord',
           'extract_literal_and_numeric', 'extract_attrs', 'add_pdf_metadata', 'add_png_metadata',
           'open_image', 'username', 'update_metadata',
           'load_yaml', 'dump_yaml', 'load_multi_yaml',
           'check_chunk_completeness', 'frequency_string_to_pandas', 'pandas_freq_to_string',
           'time_to_string', 'int_month_name',  'xarray_to_pandas_freq',
           'create_zarr_reference',
           ]
