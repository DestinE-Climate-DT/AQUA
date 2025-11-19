"""Helpers to resolve AQUA configuration directories and files."""
import os

class ConfigLocator:
    """
    Helper to resolve AQUA configuration directories/files.
    """

    def __init__(self, filename='config-aqua.yaml', configdir=None, logger=None):
        self.filename = filename
        self._configdir = configdir
        self.logger = logger

    @property
    def configdir(self):
        if self._configdir is None:
            self._configdir = self._discover_config_dir()
        return self._configdir

    @property
    def config_file(self):
        return os.path.join(self.configdir, self.filename)

    def _discover_config_dir(self):
        """
        Search for the configuration directory in a list of predefined directories.
        """
        configdirs = []

        aquaconfigdir = os.environ.get('AQUA_CONFIG')
        if aquaconfigdir:
            configdirs.append(aquaconfigdir)

        homedir = os.environ.get('HOME')
        if homedir:
            configdirs.append(os.path.join(homedir, '.aqua'))

        for configdir in configdirs:
            if os.path.exists(os.path.join(configdir, self.filename)):
                if self.logger:
                    self.logger.debug('AQUA installation found in %s', configdir)
                return configdir

        raise FileNotFoundError(f"No config file {self.filename} found in {configdirs}")

