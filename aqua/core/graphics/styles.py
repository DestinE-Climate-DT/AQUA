"""Class to load the choosen style for graphical utilities."""
import os
import matplotlib.pyplot as plt
from aqua.core.logger import log_configure
from aqua.core.configurer import ConfigPath
from aqua.core.util import load_yaml


class ConfigStyle(ConfigPath):
    """Class to load the choosen style for graphical utilities."""
    def __init__(self,
                 style: str = None,
                 filename: str = 'config-aqua.yaml',
                 configdir: str = None,
                 loglevel: str = 'WARNING'):
        """Initialize the class.

        Args:
            style (str): name of the style to load.
                     If not provided, it will be read from the configuration file.
            filename (str): name of the configuration file. Default is 'config-aqua.yaml'.
            configdir (str): path to the configuration directory.
                            If not provided, it is determined by the `get_config_dir` method.
            loglevel (str): logging level. Default is 'WARNING'
        """

        # Initialize the ConfigPath class
        super().__init__(configdir=configdir, filename=filename, loglevel=loglevel)
        self.logger = log_configure(log_level=loglevel, log_name="ConfigStyle")

        if style is not None:
            self.style = style
        else:  # Read the style from the configuration file
            configfile = load_yaml(self.config_file)
            if 'options' in configfile:
                self.style = configfile['options'].get('style', 'aqua')
            else:
                self.style = 'aqua'

        style_dir = os.path.join(self.configdir, 'styles')
        filename = self.style + '.mplstyle'
        self.style_file = os.path.join(style_dir, filename)
        self.logger.debug("Style file: %s", self.style_file)
        self.load_style()

    def load_style(self):
        """Load the choosen style."""
        try:
            plt.style.use(self.style_file)
            self.logger.debug("Setting style %s from file %s", self.style, self.style_file)
        except OSError:
            plt.style.use(self.style)
            self.logger.debug("Setting matplotlib style %s", self.style)