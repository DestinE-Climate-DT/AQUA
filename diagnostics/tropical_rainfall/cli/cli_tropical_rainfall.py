import sys
import os
from aqua.util import get_arg
from src.tropical_rainfall_utils import parse_arguments, validate_arguments, load_configuration
from src.tropical_rainfall_cli_class import Tropical_Rainfall_CLI
from tropical_rainfall import __path__ as pypath

def main():
    """Main function to orchestrate the tropical rainfall CLI operations."""
    try:
        # Parse command line arguments
        args = parse_arguments(sys.argv[1:])

        # Validate the parsed arguments
        validate_arguments(args)

        # Determine the configuration file path
        config_file = get_arg(args, 'config', os.path.join(pypath[0], 'cli', 'cli_config_trop_rainfall.yml'))

        # Load the configuration from the file
        config = load_configuration(config_file)

        # Initialize the Tropical Rainfall CLI with the loaded configuration and arguments
        trop_rainfall_cli = Tropical_Rainfall_CLI(config, args)

        # Uncomment and call the desired methods
        trop_rainfall_cli.calculate_histogram_by_months()
        trop_rainfall_cli.plot_histograms()
        trop_rainfall_cli.average_profiles()

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
