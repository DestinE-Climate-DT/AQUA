import sys
import os
from aqua.logger import log_configure
from aqua.util import get_arg
from src.tropical_rainfall_utils import parse_arguments, validate_arguments, load_configuration
from src.tropical_rainfall_cli_class import Tropical_Rainfall_CLI

# Initialize logger
logger = log_configure(log_name="Trop. Rainfall CLI", log_level='INFO')

def load_config(args):
    """Load the configuration file."""
    homedir = os.environ.get('HOME')
    config_filename = os.path.join(homedir, '.aqua', 'diagnostics', 'tropical_rainfall', 'cli', 'cli_config_trop_rainfall.yml')

    # Load the configuration
    config_path = get_arg(args, 'config', config_filename)
    try:
        config = load_configuration(config_path)
        logger.info(f"Configuration successfully loaded from {config_path}")
    except FileNotFoundError:
        logger.error(f"Configuration file not found at {config_filename}")
        sys.exit(2)
    except Exception as e:
        logger.error(f"An error occurred while loading configuration: {e}")
        sys.exit(3)

    return config

def main():
    """Main function to orchestrate the tropical rainfall CLI operations."""
    # Parse and validate arguments
    args = parse_arguments(sys.argv[1:])
    validate_arguments(args)

    # Load configuration
    config = load_config(args)

    # Create the CLI object and run operations
    trop_rainfall_cli = Tropical_Rainfall_CLI(config, args)

    try:
        trop_rainfall_cli.calculate_histogram_by_months()
        trop_rainfall_cli.plot_histograms()
        trop_rainfall_cli.average_profiles()
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")
        sys.exit(4)

if __name__ == '__main__':
    main()
