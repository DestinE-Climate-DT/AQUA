#!/usr/bin/env python3
"""
AQUA analysis module for running diagnostics and handling configurations.
"""

import logging
import os
import shutil
import subprocess
import sys
import tempfile
from importlib import resources as pypath

from dask.distributed import LocalCluster

from aqua.core.configurer import ConfigPath
from aqua.core.logger import log_configure
from aqua.core.util import create_folder, dump_yaml, format_realization, get_arg, load_yaml, to_list


class Analysis:
    """Structured class for running AQUA diagnostic collections and managing configurations."""

    def __init__(self, config_file_path=None, loglevel="WARNING"):
        """
        Initialize the analysis instance.

        Args:
            config_file_path (str): Path to the AQUA analysis configuration file.
            loglevel (str): Logging level for the analysis instance.
        """
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name="AquaAnalysis")

        self.aqua_configdir = ConfigPath().configdir

        if config_file_path is None:
            self.config_file_path = os.path.join(self.aqua_configdir, "analysis/config.aqua-analysis.yaml")
        else:
            self.config_file_path = os.path.expandvars(config_file_path)

        self.config = None
        self.aqua_core_path = None
        self.aqua_diagnostics_path = None

        # experiment kind dictionary
        self.exp_kind_dict = None

        # default
        self.model = None
        self.exp = None
        self.source = None
        self.catalog = None
        self.source_oce = None
        self.startdate = None
        self.enddate = None
        self.realization = None
        self.regrid = False
        self.output_dir = None

        # dask
        self.serial = False
        self.cluster = None
        self.cluster_address = None

    def get_config(self):
        """Load the configuration file and return the config dictionary."""

        if not os.path.exists(self.config_file_path):
            self.logger.error("Config file %s not found.", self.config_file_path)
            sys.exit(1)

        self.config = load_yaml(self.config_file_path)
        self.logger.info("AQUA analysis config loaded: %s", self.config_file_path)
        return self.config

    def check_model_exp_source(self):
        """Check that the required model-exp-source triplet is provided and log the configuration."""
        if not all([self.model, self.exp, self.source]):
            self.logger.error("Model, experiment, and source must be specified either in config or as command-line arguments.")
            sys.exit(1)
        else:
            self.logger.info(
                "Requested experiment: Model = %s, Experiment = %s, Source = %s. Source_oce = %s",
                self.model,
                self.exp,
                self.source,
                self.source_oce,
            )

    def set_catalog_model_exp_source(self, args, config):
        """
        Get catalog, model, experiment, and source (and source_oce) from command-line arguments configuration dictionary
        and set them into class attributes. Catalog is determined automatically if not provided.
        Guards against missing required model-exp-source and logs the requested configuration.

        Args:
            args (argparse.Namespace): Parsed command-line arguments.
            config (dict): Job configuration dictionary loaded from YAML.
        """

        self.model = get_arg(args, "model", None, config=config)
        self.exp = get_arg(args, "exp", None, config=config)
        self.source = get_arg(args, "source", None, config=config)
        self.source_oce = get_arg(args, "source_oce", None, config=config)

        # guard
        self.check_model_exp_source()

        # catalog
        self.catalog = get_arg(args, "catalog", None, config=config)
        if self.catalog:
            self.logger.info("Requested catalog: %s", self.catalog)
        else:
            cat, _ = ConfigPath().browse_catalogs(self.model, self.exp, self.source)
            if cat:
                self.catalog = cat[0]
                self.logger.info("Automatically determined catalog: %s", self.catalog)
            else:
                self.logger.error(
                    "Model = %s, Experiment = %s, Source = %s triplet not found in any installed catalog.",
                    self.model,
                    self.exp,
                    self.source,
                )
                sys.exit(1)

        # guard
        self.check_catalog()

    def check_catalog(self):
        """Check that the catalog is set and log the configuration."""
        if not self.catalog:
            self.logger.error("Catalog must be specified either in config or as command-line argument.")
            sys.exit(1)
        else:
            self.logger.info("Using catalog: %s", self.catalog)

    def set_startdate_enddate(self, args, config):
        """
        Get startdate and enddate from command-line arguments and set them into class attributes.
        If not provided, they will remain None.

        Args:
            args (argparse.Namespace): Parsed command-line arguments.
            config (dict): Job configuration dictionary loaded from YAML.
        """

        self.startdate = get_arg(args, "startdate", None, config=config)
        self.enddate = get_arg(args, "enddate", None, config=config)

        if self.startdate and self.enddate:
            self.logger.info("Requested time range: %s to %s", self.startdate, self.enddate)
        else:
            self.logger.info("No time range specified; using all available data.")

    def set_regrid_option(self, args, config):
        """Get regrid option from from command-line arguments and set them into class attribute.
        If not provided, they will remain be set to False to disable regrid.

        Args:
            args (argparse.Namespace): Parsed command-line arguments.
            config (dict): Job configuration dictionary loaded from YAML.
        """

        self.regrid = get_arg(args, "regrid", "False", config=config)
        if not self.regrid or (isinstance(self.regrid, str) and self.regrid.lower() == "false"):
            self.regrid = None

    def set_realization(self, args, config):
        """Get realization from command-line arguments and set them into class attribute.
        If not provided, they will remain be formatted to r1.

        Args:
            args (argparse.Namespace): Parsed command-line arguments.
            config (dict): Job configuration dictionary loaded from YAML.
        """

        realization = get_arg(args, "realization", None, config=config)
        self.realization = format_realization(realization)
        self.check_realization()
        self.logger.info("Input realization formatted to: %s", realization)

    def set_serial_or_parallel(self, args):
        """Get serial option from command-line arguments and set them into class attribute.
        If not provided, they will remain be set to False to enable parallel execution.

        Args:
            args (argparse.Namespace): Parsed command-line arguments.
            config (dict): Job configuration dictionary loaded from YAML.
        """

        self.serial = get_arg(args, "serial", False)
        if self.serial:
            self.logger.info("Serial execution enabled; dask cluster will not be used.")
            if args.nworkers or args.nthreads:
                self.logger.warning("Serial execution selected, ignoring worker/thread settings.")
        else:
            self.logger.info("Parallel execution enabled; dask cluster will be used if configured.")

    def check_realization(self):
        """Check that realization is set and log the configuration."""
        if not self.realization:
            self.logger.error("Realization must be specified either in config or as command-line argument.")
            sys.exit(1)
        else:
            self.logger.info("Using realization: %s", self.realization)

    def set_output_directory(self, args, config):
        """Get output directory from command-line arguments and set them into class attribute.
        If not provided, it will be set to ./output.

        Args:
            args (argparse.Namespace): Parsed command-line arguments.
            config (dict): Job configuration dictionary loaded from YAML.
        """

        outputdir = os.path.expandvars(get_arg(args, "outputdir", "./output", config=config))
        self.check_model_exp_source()  # ensure model, exp, source are set before setting output directory
        self.check_realization()  # ensure realization is set before setting output directory
        self.check_catalog()  # ensure catalog is set before setting output directory
        self.output_dir = os.path.join(outputdir, self.catalog, self.model, self.exp, self.realization)
        create_folder(self.output_dir, loglevel=self.loglevel)
        self.logger.debug("Output directory set to: %s", self.output_dir)

    def get_aqua_paths(self):
        """Get and cache AQUA core and diagnostics paths.

        Returns:
            tuple: (aqua_core_path, aqua_diagnostics_path, aqua_configdir)
        """

        self.aqua_core_path = str(pypath.files("aqua.core"))
        try:
            self.aqua_diagnostics_path = str(pypath.files("aqua.diagnostics"))
        except ModuleNotFoundError:
            self.aqua_diagnostics_path = ""
            self.logger.error("aqua.diagnostics package not found; AQUA_DIAGNOSTICS will be empty.")

        self.logger.debug("AQUA core path: %s", self.aqua_core_path)
        self.logger.debug("AQUA diagnostics path: %s", self.aqua_diagnostics_path)

        return self.aqua_core_path, self.aqua_diagnostics_path, self.aqua_configdir

    def run_command(self, cmd: str, log_file: str) -> int:
        """
        Run a system command and capture the exit code, redirecting output to the specified log file.

        Args:
            cmd (str): Command to run.
            log_file (str): Path to the log file to capture the command's output.

        Returns:
            int: The exit code of the command.
        """
        try:
            log_file = os.path.expandvars(log_file)
            log_dir = os.path.dirname(log_file)
            create_folder(log_dir)

            with open(log_file, "w", encoding="utf-8") as log:
                process = subprocess.run(cmd, shell=True, stdout=log, stderr=log, text=True, check=False)
                return process.returncode
        except (OSError, subprocess.SubprocessError) as e:
            self.logger.error("Error running command %s: %s", cmd, e)
            raise

    def run_diagnostic_tool(
        self,
        collection: str,
        tool: str,
        script_path: str,
        extra_args: str,
        logfile: str = "aqua-diagnostic-tool.log",
    ):
        """
        Run the diagnostic tool script with specified arguments.

        Args:
            collection (str): Name of the diagnostic collection.
            tool (str): Name of the diagnostic tool to use.
            script_path (str): Path to the diagnostic tool script.
            extra_args (str): Additional arguments for the script.
            logfile (str): Path to the logfile for capturing the command output.
        """

        self.logger.info("Running tool %s for diagnostic collection %s", tool, collection)
        cmd = f"python {script_path} {extra_args} -l {self.loglevel}"
        self.logger.debug("Command: %s", cmd)

        result = self.run_command(cmd, logfile)

        if result != 0:
            self.logger.error("Tool %s for diagnostic collection %s failed with exit code %s", tool, collection, result)
        else:
            self.logger.info("Tool %s for diagnostic collection %s completed successfully.", tool, collection)

    def run_setup_checker(
        self,
        script_path: str = None,
        logfile: str = None,
    ) -> int:
        """
        Run the setup checker script and return the exit code.

        Args:
            script_path (str): Path to the setup checker script.
            output_dir (str): Output directory.
            logfile (str): Path to the logfile for capturing the command output.

        Returns:
            int: The exit code of the command.
        """
        # self gueessing
        if script_path is None:
            script_path = os.path.join(self.aqua_core_path, "analysis", "cli_checker.py")
        if logfile is None:
            logfile = os.path.expandvars(f"{self.output_dir}/setup_checker.log")

        self.logger.info("Running setup checker")
        extra_args = self.build_extra_args(regrid=self.regrid, catalog=self.catalog, realization=self.realization)
        cmd = (
            f"python {script_path} --model {self.model} --exp {self.exp} --source {self.source} --yaml {self.output_dir}"
            f"{extra_args} -l {self.loglevel}"
        )
        self.logger.debug("Command: %s", cmd)

        result = self.run_command(cmd, logfile)
        if result == 1:
            self.logger.critical("Setup checker failed, exiting.")
            sys.exit(1)
        elif result == 0:
            self.logger.info("Setup checker completed successfully.")
            return 0
        else:
            self.logger.error("Setup checker returned exit code %s, check the log %s for more information.", result, logfile)
            return result

    def run_diagnostic_collection(
        self,
        collection: str,
        cli: dict = None,
        diag_config=None,
    ):
        """
        Run the diagnostic collection and log the output, handling parallel processing if required.

        Args:
            collection (str): Name of the diagnostic collection.
            regrid (str): Regrid option.
            cli (dict): CLI definitions for the tools.
            diag_config (dict): Configuration dictionary loaded from YAML.
        """
        if cli is None:
            cli = {}

        # Internal naming scheme:
        # collection: the name of the wrapper metadiagnostic, e.g. atmosphere2d, climate_metrics, etc.
        # tool: the name of the individual command-line tool being run, e.g. biases, ecmean, etc.

        # run individual tools in serial mode
        for tool, tool_config in diag_config.items():
            self.logger.info("Configuring tool %s for diagnostic collection %s", tool, collection)

            cli_path = cli.get(tool)
            if cli_path is None:
                self.logger.error("CLI path for tool '%s' not found, skipping.", tool)
                continue

            if not os.path.exists(cli_path):
                self.logger.error("Script for tool '%s' not found at path: %s, skipping", tool, cli_path)
                continue

            outname = f"{self.output_dir}/{tool_config.get('outname', collection)}"
            extra_args = tool_config.get("extra", "")

            # Build conditional arguments
            if self.regrid:
                extra_args += f" --regrid {self.regrid}"

            if not self.serial:
                tool_nworkers = tool_config.get("nworkers")
                tool_nthreads = tool_config.get("nthreads")
                if tool_nworkers is not None:
                    extra_args += f" --nworkers {tool_nworkers}"
                if tool_nthreads is not None:
                    extra_args += f" --nthreads {tool_nthreads}"

            # This is needed for ECmean which uses multiprocessing
            if self.cluster_address and not tool_config.get("nocluster", False):
                extra_args += f" --cluster {self.cluster_address}"

            # Add standard arguments using helper function
            extra_args += self.build_extra_args(
                catalog=self.catalog,
                realization=self.realization,
                startdate=self.startdate,
                enddate=self.enddate,
            )

            # pass source_oce only if allowed by the diagnostic config file
            if tool_config.get("source_oce", False) and self.source_oce:
                extra_args += f" --source_oce {self.source_oce}"

            cfgs = to_list(tool_config.get("config"))
            if not cfgs:
                self.logger.error("Config for tool '%s' not found, skipping.", tool)
                continue

            # update cfgs with experiment kind templating if exp_kind_dict is provided
            if self.exp_kind_dict:
                cfgs = self.configure_template_configs(cfgs)

            for i, cfg in enumerate(cfgs, start=1):
                args = (
                    f"--model {self.model} --exp {self.exp} --source {self.source} --outputdir {outname}"
                    f" {extra_args} --config {cfg}"
                )
                if len(cfgs) == 1:
                    logfile = f"{self.output_dir}/{collection}-{tool}.log"
                else:
                    logfile = f"{self.output_dir}/{collection}-{tool}-{i}.log"

                self.run_diagnostic_tool(
                    collection=collection,
                    tool=tool,
                    script_path=cli_path,
                    extra_args=args,
                    logfile=logfile,
                )

            # remove temporary rendered config files created when using experiment kind templating
            if self.exp_kind_dict:
                temp_cfg_dir = os.path.dirname(cfgs[0])
                shutil.rmtree(temp_cfg_dir, ignore_errors=True)
                self.logger.debug("Removed temporary config directory: %s", temp_cfg_dir)

    def configure_experiment_kind(self, exp_kind, exp_kind_file):
        """
        Configure the experiment kind based on the provided kind and configuration file.

        Args:
            exp_kind (str): The experiment kind to configure (e.g "historical").
            exp_kind_file (str): YAML file containing kind configurations (e.g. "startdate").
        """
        if exp_kind is None:
            return None

        if not os.path.exists(exp_kind_file):
            raise FileNotFoundError(f"Experiment kind config file '{exp_kind_file}' not found.")

        self.logger.info("Configuring experiment kind: %s using config file: %s", exp_kind, exp_kind_file)
        complete_dictionary = load_yaml(exp_kind_file)

        if exp_kind not in complete_dictionary:
            self.logger.error("Experiment kind '%s' not found in config file '%s'. Default selected", exp_kind, exp_kind_file)
            if "default" not in complete_dictionary:
                raise KeyError(
                    f"Default experiment kind not found in config file '{exp_kind_file}'. Please ensure it is defined."
                )
        self.exp_kind_dict = complete_dictionary.get(exp_kind, "default")

    def configure_template_configs(self, cfgs):
        """
        Run jinja templating on the config files based on the experiment kind dictionary.
        Then dump them into a temporary folder and return the list of new config paths.

        Args:
            cfgs (list): List of config file paths to render.

        Returns:
            list: List of paths to the rendered config files in a temporary directory.
        """
        temp_dir = tempfile.mkdtemp(prefix="aqua_analysis_configs_")
        self.logger.debug("Temporary config directory: %s", temp_dir)

        new_cfg_paths = []
        for cfg in cfgs:
            rendered_cfg = load_yaml(cfg, definitions=self.exp_kind_dict, strict=True)
            new_cfg_path = os.path.join(temp_dir, os.path.basename(cfg))
            dump_yaml(new_cfg_path, rendered_cfg)
            self.logger.info("Rendered config saved to: %s", new_cfg_path)
            new_cfg_paths.append(new_cfg_path)

        return new_cfg_paths

    def configure_dask_cluster(self, args, cluster_config):
        """
        Configure a global dask cluster based on the provided arguments and cluster configuration.

        Args:
            args (argparse.Namespace): Parsed command-line arguments.
            cluster_config (dict): Cluster configuration dictionary loaded from YAML.
        """

        nthreads = get_arg(args, "nthreads", 2, config=cluster_config, key="threads")
        nworkers = get_arg(args, "nworkers", 32, config=cluster_config, key="workers")
        mem_limit = cluster_config.get("memory_limit", "3.1GiB")
        self.logger.debug(
            "Cluster configuration - nthreads: %d, nworkers: %d, memory_limit: %s", nthreads, nworkers, mem_limit
        )

        # silence_logs to avoids excessive logging (see https://github.com/dask/dask/issues/9888)
        self.cluster = LocalCluster(
            threads_per_worker=nthreads, n_workers=nworkers, memory_limit=mem_limit, silence_logs=logging.ERROR
        )
        self.cluster_address = self.cluster.scheduler_address
        self.logger.info(
            "Initialized global dask cluster %s providing %d workers.", self.cluster_address, len(self.cluster.workers)
        )

        # set DASK timeouts if not already set in the environment
        if "DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT" not in os.environ:
            connect_timeout = cluster_config.get("connect_timeout", None)
            if connect_timeout:
                # Increase timeout (certainly needed on LUMI, possibly useful elsewhere too).
                os.environ["DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT"] = f"{connect_timeout}s"
                self.logger.debug(
                    "Set DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT to %s",
                    os.environ["DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT"],
                )
        if "DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP" not in os.environ:
            tcp_timeout = cluster_config.get("tcp_timeout", None)
            if tcp_timeout:
                os.environ["DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP"] = f"{tcp_timeout}s"  # optional, might be good
                self.logger.debug(
                    "Set DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP to %s", os.environ["DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP"]
                )

    def close_dask_cluster(self):
        """Close the dask cluster if it was created."""
        if self.cluster:
            self.logger.info("Closing dask cluster %s.", self.cluster_address)
            self.cluster.close()
            self.cluster = None

    @staticmethod
    def build_extra_args(**kwargs):
        """Build command line arguments from key-value pairs, skipping None values."""
        args = ""
        for flag, value in kwargs.items():
            if value is not None:
                args += f" --{flag} {value}"
        return args
