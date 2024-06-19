import argparse
from tropical_rainfall import __version__ as version
from tropical_rainfall import __path__ as pypath

def parse_arguments():
    """Parse arguments for Tropical Rainfall console"""

    parser = argparse.ArgumentParser(prog='tropical_rainfall', description='Tropical Rainfall command line tool')
    subparsers = parser.add_subparsers(dest='command', help='Available Tropical Rainfall commands')

    parser.add_argument('--version', action='version',
                        version=f'%(prog)s v{version}', help="show Tropical Rainfall version number and exit.")
    parser.add_argument('--path', action='version', version=f'{pypath[0]}',
                        help="show Tropical Rainfall installation path and exit")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Increase verbosity of the output to INFO log level')
    parser.add_argument('-vv', '--very_verbose', action='store_true',
                        help='Increase verbosity of the output to DEBUG log level')

    init_parser = subparsers.add_parser("init", description='Initialize Tropical Rainfall configuration')
    init_parser.add_argument('-p', '--path', type=str, metavar="TROPICAL_RAINFALL_TARGET_PATH",
                             help='Path where to install Tropical Rainfall. Default is $HOME/.tropical_rainfall')
    init_parser.add_argument('-c', '--config', type=str, metavar="CONFIG_FILE_PATH",
                             help='Path to the custom configuration file')

    add_config_parser = subparsers.add_parser("add_config", description='Add a configuration file to Tropical Rainfall')
    add_config_parser.add_argument('config_file_path', metavar="CONFIG_FILE_PATH", type=str,
                                   help="Path to the configuration file")

    run_tr_parser = subparsers.add_parser("run_cli", description='Run Tropical Rainfall CLI')
    run_tr_parser.add_argument('--nproc', type=int, default=1, help="Number of processes to use for analysis")
    run_tr_parser.add_argument('--config_file', type=str, help="Path to the CLI configuration file")

    test_parser = subparsers.add_parser("test", description='Run tests for the Tropical Rainfall package')
    test_parser.add_argument('test_folder', nargs='?', default='tests', help="Folder containing the tests (default: 'tests')")

    parser_dict = {
        'main': parser
    }

    return parser_dict
