"""
This module contains the CLI for the GridBuilder.
"""

import argparse
from aqua import Reader
from aqua import GridBuilder
from aqua.core.util import load_yaml, get_arg


def builder_parser(parser=None):

    """
    Parse command line arguments for the builder CLI.
    """
    if parser is None:
        parser = argparse.ArgumentParser(description='AQUA grids builder CLI')
    parser.add_argument('-c', '--config', type=str, 
                        help='YAML configuration file for the builder [default: None]')
    parser.add_argument('--catalog', type=str, help='Catalog for the Reader')
    parser.add_argument('-m', '--model', type=str,
                        help='Model name (e.g. "ECMWF", "ERA5")')
    parser.add_argument('-e', '--exp', type=str,
                        help='Experiment name (e.g. "historical", "future")')
    parser.add_argument('-s', '--source', type=str,
                        help='Data source (e.g. "reanalysis", "forecast")')
    parser.add_argument('-l', '--loglevel', type=str, default=None,
                        help='Log level [default: WARNING]')
    parser.add_argument('--rebuild', action='store_true',
                        help='Rebuild the grid even if it already exists')
    parser.add_argument('--vert_coord', type=str,
                        help='Vertical coordinate name for 3D grids [default: None]')
    parser.add_argument('--version', type=int,
                        help='Version number for the grid file [default: None]')
    parser.add_argument('--outdir', type=str, default='.',
                        help='Output directory for the grid file [default: current directory]')
    parser.add_argument('--original', type=str,
                        help='Original resolution of the grid [default: None]')
    parser.add_argument('--modelname', type=str,
                        help='alternative name for the model for grid naming [default: None]')
    parser.add_argument('--gridname', type=str,
                        help='alternative name for the grid for grid naming [default: None]. Required for Curvilinear and Unstructured grids.')
    parser.add_argument('--fix', action='store_true',
                        help='Fix the original source [default: False]')
    parser.add_argument('--verify', action='store_true', default=False,
                        help='Verify the grid file after creation [default: False]')
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Create the grid entry in the grid file [default: False]')

    return parser

def builder_execute(args):
    """
    Execute the builder CLI with the provided arguments or configuration file.
    """
    config = {}
    reader_config = {}
    builder_config = {}
    if hasattr(args, 'config') and args.config:
        config = load_yaml(args.config)
        reader_config = config.get('reader', {})
        builder_config = config.get('builder', {})

    # Use get_arg to merge CLI args and config file values
    catalog = get_arg(args, 'catalog', reader_config.get('catalog'))
    model = get_arg(args, 'model', reader_config.get('model'))
    exp = get_arg(args, 'exp', reader_config.get('exp'))
    source = get_arg(args, 'source', reader_config.get('source'))
    fix = get_arg(args, 'fix', reader_config.get('fix', False))
    loglevel = get_arg(args, 'loglevel', builder_config.get('loglevel', 'WARNING'))
    outdir = get_arg(args, 'outdir', builder_config.get('outdir', '.'))
    original_resolution = get_arg(args, 'original', builder_config.get('original'))
    modelname = get_arg(args, 'modelname', builder_config.get('modelname'))
    gridname = get_arg(args, 'gridname', builder_config.get('gridname'))
    rebuild = get_arg(args, 'rebuild', builder_config.get('rebuild', False))
    version = get_arg(args, 'version', builder_config.get('version'))
    verify = get_arg(args, 'verify', builder_config.get('verify', False))
    create_yaml = get_arg(args, 'yaml', builder_config.get('yaml', False))
    vert_coord = get_arg(args, 'vert_coord', builder_config.get('vert_coord'))

    # Ensure required arguments are present
    if model is None:
        raise ValueError("Model must be specified via --model or in the config file")
    if exp is None:
        raise ValueError("Experiment must be specified via --exp or in the config file")
    if source is None:
        raise ValueError("Source must be specified via --source or in the config file")

    # retrieve the data
    reader = Reader(catalog=catalog, model=model, exp=exp, source=source, loglevel=loglevel, areas=False, fix=fix)
    data = reader.retrieve()

    # Create GridBuilder instance
    grid_builder = GridBuilder(
        loglevel=loglevel,
        outdir=outdir, 
        original_resolution=original_resolution,
        model_name=modelname,
        grid_name=gridname,
        vert_coord=vert_coord
    )

    # Build the grid
    grid_builder.build(
        data, rebuild=rebuild,
        version=version, verify=verify,
        create_yaml=create_yaml)
    