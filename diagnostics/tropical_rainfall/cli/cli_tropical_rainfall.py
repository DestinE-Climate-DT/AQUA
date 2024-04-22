import sys
import os
from aqua.util import get_arg
aqua_path = os.getenv('AQUA')  # This will return None if 'AQUA' is not set
from src.tropical_rainfall_utils import parse_arguments, validate_arguments, load_configuration
from src.tropical_rainfall_cli_class import Tropical_Rainfall_CLI

def main():
    """Main function to orchestrate the tropical rainfall CLI operations."""

    args = parse_arguments(sys.argv[1:])
    validate_arguments(args)

    config = load_configuration(get_arg(args, 'config',
                                        f'{aqua_path}/diagnostics/tropical_rainfall/cli/cli_config_trop_rainfall.yml'))

    trop_rainfall_cli = Tropical_Rainfall_CLI(config, args)
    trop_rainfall_cli.calculate_histogram_by_months()
    trop_rainfall_cli.plot_histograms()
    trop_rainfall_cli.daily_variability()
    trop_rainfall_cli.plot_daily_variability()
    trop_rainfall_cli.average_profiles()
    

if __name__ == '__main__':
    main()
