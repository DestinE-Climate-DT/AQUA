import argparse
import os
import sys

AQUA = os.environ.get("AQUA")
if AQUA is not None:
    replicability_path = os.path.join(AQUA, 'diagnostics/replicability')
    sys.path.insert(0, replicability_path)
    print(f"Attached replicability from this path {replicability_path}")
else:
    print("AQUA environment variable is not defined. Going to use default replicability package!")

from replicability import ks_test
from aqua.util import load_yaml, get_arg, create_folder
from aqua.logger import log_configure


class replicabilityCLI:
    def __init__(self, args):
        self.args = args
        self.loglevel = {}
        self.config = {}
        self.data = {}
    def get_arg(self, arg, default):
        """
        Support function to get arguments

        Args:
            args: the arguments
            arg: the argument to get
            default: the default value

        Returns:
            The argument value or the default value
        """

        res = getattr(self.args, arg)
        if not res:
            res = default
        return res

    def get_value_with_default(self, dictionary, key, default_value):
        try:
            return dictionary[key]
        except KeyError:
            return default_value

    def replicability_config_process(self, file):
        self.replicability_config_dict = load_yaml(file)

        self.logger.debug(f"Configuration file: {self.replicability_config_dict}")

        self.config["ensemble1"] = self.replicability_config_dict.get('ensemble1')
        self.config["ensemble1_PI_dir"] = self.replicability_config_dict.get('ensemble1_PI_dir')
        self.config["ensemble2"] = self.replicability_config_dict.get('ensemble2')
        self.config["ensemble2_PI_dir"] = self.replicability_config_dict.get('ensemble2_PI_dir')
        self.config["members"] = self.replicability_config_dict.get('members')
        self.config["outputdir"] = self.replicability_config_dict.get('outputdir')



    def run_diagnostic(self):

        self.loglevel = self.get_arg('loglevel', 'INFO')
        self.logger = log_configure(log_name='replicability CLI', log_level= self.loglevel)

        # Change the current directory to the one of the CLI so that relative paths work
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        if os.getcwd() != dname:
            os.chdir(dname)
            self.logger.info(f'Moving from current directory to {dname} to run!')

        self.logger.info("Running replicability diagnostic...")

        # Read configuration file
        file = self.get_arg('config', 'config.yaml')
        self.logger.info('Reading configuration yaml file..')

        self.replicability_config_process(file)
        
        ks_test_diag = ks_test(self.config)
        ks_test_diag.plot()



def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='replicability CLI')

    parser.add_argument('--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    # This arguments will override the configuration file is provided
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--outputdir', type=str,
                        help='Output directory')

    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    replicability_cli = replicabilityCLI(args)
    replicability_cli.run_diagnostic()