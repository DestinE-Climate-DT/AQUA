import argparse
from aqua.regridder.builder import GridBuilder


def builder_parser(parser=None):

    """
    Parse command line arguments for the builder CLI.
    """
    if parser is None:
        parser = argparse.ArgumentParser(description='AQUA grids builder CLI')
    parser.add_argument('-m', '--model', type=str, required=True,
                        help='Model name (e.g. "ECMWF", "ERA5")')
    parser.add_argument('-e', '--exp', type=str, required=True,
                        help='Experiment name (e.g. "historical", "future")')
    parser.add_argument('-s', '--source', type=str, required=True,
                        help='Data source (e.g. "reanalysis", "forecast")')
    parser.add_argument('-l', '--loglevel', type=str, default='WARNING',
                        help='Log level [default: WARNING]')


    return parser

def builder_execute(args):
    """
    Execute the builder CLI with the provided arguments.
    """

    # Create GridBuilder instance
    grid_builder = GridBuilder(
        model=args.model, exp=args.exp, 
        source=args.source, loglevel=args.loglevel
    )

    # Build the grid
    grid_builder.build()
    