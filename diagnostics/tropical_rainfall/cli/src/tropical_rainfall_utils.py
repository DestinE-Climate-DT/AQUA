import argparse
from aqua.util import load_yaml

def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Trop. Rainfall CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')
    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
    # This arguments will override the configuration file if provided
    parser.add_argument('--model', type=str, help='model name',
                        required=False)
    parser.add_argument('--exp', type=str, help='experiment name',
                        required=False)
    parser.add_argument('--source', type=str, help='source name',
                        required=False)
    parser.add_argument('--regrid', type=str, help='regrid value',
                        required=False)
    parser.add_argument('--freq', type=str, help='frequency',
                        required=False)
    parser.add_argument('--outputdir', type=str, help='output directory',
                        required=False)
    parser.add_argument('--bufferdir', type=str, help='buffer directory',
                        required=False)
    parser.add_argument('--xmax', type=int, help='maximum value on horizontal axe',
                        required=False)
    parser.add_argument('--nproc', type=int, required=False,
                        help='the number of processes to run in parallel',
                        default=4)
    return parser.parse_args(args)

def validate_arguments(args):
    """
    Validate the types of command line arguments.

    Args:
        args: Parsed arguments from argparse.

    Raises:
        TypeError: If any argument is not of the expected type.
    """
    if args.config and not isinstance(args.config, str):
        raise TypeError("Config file path must be a string.")
    if args.loglevel and not isinstance(args.loglevel, str):
        raise TypeError("Log level must be a string.")
    if args.model and not isinstance(args.model, str):
        raise TypeError("Model name must be a string.")
    if args.exp and not isinstance(args.exp, str):
        raise TypeError("Experiment name must be a string.")
    if args.source and not isinstance(args.source, str):
        raise TypeError("Source name must be a string.")
    if args.regrid and not isinstance(args.regrid, str):
        raise TypeError("Regrid value must be a string.")
    if args.freq and not isinstance(args.freq, str):
        raise TypeError("Frequency value must be a string.")
    if args.outputdir and not isinstance(args.outputdir, str):
        raise TypeError("Output directory must be a string.")
    if args.bufferdir and not isinstance(args.bufferdir, str):
        raise TypeError("Buffer directory must be a string.")
    if args.xmax and not isinstance(args.xmax, int):
        raise TypeError("Xmax must be an integer.")
    if args.nproc and not isinstance(args.nproc, int):
        raise TypeError("The number of processes (nproc) must be an integer.")

def load_configuration(file_path):
    """Load and return the YAML configuration."""
    config = load_yaml(file_path)
    return config

def adjust_year_range_based_on_dataset(full_dataset, start_year=None, final_year=None):
    """
    Adjusts the start and end years for processing based on the dataset's time range and optional user inputs.
    """
    # Extract the first and last year from the dataset's time dimension
    try:
        first_year_in_dataset = full_dataset['time'].dt.year.values[0]
        last_year_in_dataset = full_dataset['time'].dt.year.values[-1]
    except AttributeError:
        raise ValueError("The dataset must have a 'time' dimension with datetime64 data.")

    # Adjust start_year based on the dataset's range or user input
    start_year = first_year_in_dataset if start_year is None else max(start_year, first_year_in_dataset)

    # Adjust final_year based on the dataset's range or user input
    final_year = last_year_in_dataset if final_year is None else min(final_year, last_year_in_dataset)

    return start_year, final_year