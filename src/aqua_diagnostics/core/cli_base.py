"""
Base class for diagnostic CLI to centralize common operations.
"""
from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args


class DiagnosticCLI:
    """
    Base class to centralize common CLI initialization operations.

    
    Usage:
        cli = DiagnosticCLI(
            args=args,
            diagnostic_name='timeseries',
            config='config_timeseries_atm.yaml'
        )
        cli.prepare()
        cli.open_dask_cluster()
        
        # Access prepared attributes
        logger = cli.logger
        config_dict = cli.config_dict
        outputdir = cli.outputdir
        ...
        
        # At the end
        cli.close_dask_cluster()
    """

    def __init__(self, args, diagnostic_name, default_config, log_name=None):
        """
        Initialize the CLI handler.
        
        Args:
            args: Parsed command-line arguments
            diagnostic_name (str): Name of the diagnostic (e.g., 'timeseries', 'seaice')
            default_config (str): Default config file name
            log_name (str, optional): Logger name. Defaults to '{diagnostic_name} CLI'
        """
        self.args = args
        self.diagnostic_name = diagnostic_name
        self.default_config = default_config
        self.log_name = log_name or f"{diagnostic_name.capitalize()} CLI"
        
        # Attributes populated by prepare()
        self.loglevel = None
        self.logger = None
        self.client = None
        self.cluster = None
        self.private_cluster = None
        self.config_dict = None
        self.regrid = None
        self.realization = None
        self.reader_kwargs = None
        self.outputdir = None
        self.rebuild = None
        self.save_pdf = None
        self.save_png = None
        self.save_netcdf = None
        self.dpi = None
        self.create_catalog_entry = None  # Default behavior; can be overridden in prepare()
        
    def prepare(self, **overrides):
        """
        Execute common setup operations (excluding cluster management).
        
        This method:
        1. Sets up logging
        2. Loads and merges config
        3. Extracts common options (regrid, realization, output settings)
        
        Optional keyword arguments can be passed to override options extracted
        from configuration. Overrides are applied after extraction so they
        take precedence.
        
        Returns:
            self: For method chaining
        """
        self._setup_logging()
        self._load_config()
        self._extract_options()

        # option to override arguments
        if overrides:
            for key, value in overrides.items():
                setattr(self, key, value)

        return self
    
    def _setup_logging(self):
        """Setup logger."""
        self.loglevel = get_arg(self.args, 'loglevel', 'WARNING')
        self.logger = log_configure(log_level=self.loglevel, log_name=self.log_name)
        self.logger.info("Running %s diagnostic with AQUA version %s", self.diagnostic_name, aqua_version)

    def _load_config(self):
        """Load diagnostic config and merge with CLI args."""
        self.config_dict = load_diagnostic_config(
            diagnostic=self.diagnostic_name,
            config=self.args.config,
            default_config=self.default_config,
            loglevel=self.loglevel
        )
        self.config_dict = merge_config_args(
            config=self.config_dict,
            args=self.args,
            loglevel=self.loglevel
        )

    def _extract_options(self):
        """Extract common options from config and args."""
        # Regrid option
        self.regrid = get_arg(self.args, 'regrid', None)
        if self.regrid:
            self.logger.info("Regrid option is set to %s", self.regrid)

        # Realization option and reader_kwargs
        self.realization = get_arg(self.args, 'realization', None)
        if self.realization:
            self.logger.info("Realization option is set to: %s", self.realization)
            self.reader_kwargs = {'realization': self.realization}
        else:
            # Fallback to config if present
            self.reader_kwargs = self.config_dict.get('datasets', [{}])[0].get('reader_kwargs') or {}

        # Output options
        output_config = self.config_dict.get('output', {})
        self.outputdir = output_config.get('outputdir', './')
        self.rebuild = output_config.get('rebuild', True)
        self.save_pdf = output_config.get('save_pdf', True)
        self.save_png = output_config.get('save_png', True)
        self.save_netcdf = output_config.get('save_netcdf', True)
        self.dpi = output_config.get('dpi', 300)
        self.create_catalog_entry = output_config.get('create_catalog_entry', False)

    def dataset_args(self, dataset):
        """
        Helper to extract dataset arguments for diagnostics.
        """

        return {'catalog': dataset['catalog'], 'model': dataset['model'],
                'exp': dataset['exp'], 'source': dataset['source'],
                'regrid': dataset.get('regrid', self.regrid),
                'startdate': dataset.get('startdate', None),
                'enddate': dataset.get('enddate', None)}

    def open_dask_cluster(self):
        """
        Open dask cluster if requested via CLI arguments.
        
        Returns:
            self: For method chaining
        """
        cluster_arg = get_arg(self.args, 'cluster', None)
        nworkers = get_arg(self.args, 'nworkers', None)

        self.client, self.cluster, self.private_cluster = open_cluster(
            nworkers=nworkers,
            cluster=cluster_arg,
            loglevel=self.loglevel
        )
        return self

    def close_dask_cluster(self):
        """
        Close the dask cluster if it was opened.
        """
        if self.client or self.cluster:
            close_cluster(
                client=self.client,
                cluster=self.cluster,
                private_cluster=self.private_cluster,
                loglevel=self.loglevel
            )
            self.logger.info("%s diagnostic completed.", self.diagnostic_name)
