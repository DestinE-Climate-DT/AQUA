#!/usr/bin/env python3
"""
AQUA analysis module for running diagnostics and handling configurations.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from importlib import resources as pypath

from aqua.core.configurer import ConfigPath
from aqua.core.util import create_folder, dump_yaml, load_yaml, to_list


class Analysis:
    """Structured class for running AQUA diagnostic collections and managing configurations."""

    def __init__(self, logger, config_file_path):
        """
        Initialize the analysis instance.

        Args:
            logger: Logger instance for logging messages.
            config_file_path (str): Path to the AQUA analysis configuration file.
        """
        self.logger = logger
        self.config_file_path = config_file_path

        # Cached state
        self._config = None
        self._aqua_core_path = None
        self._aqua_diagnostics_path = None
        self._aqua_configdir = None

    def _load_config(self):
        """Load and cache the AQUA analysis configuration."""
        if self._config is not None:
            return self._config

        if not os.path.exists(self.config_file_path):
            self.logger.error(f"Config file {self.config_file_path} not found.")
            sys.exit(1)

        self._config = load_yaml(self.config_file_path)
        self.logger.info(f"AQUA analysis config loaded: {self.config_file_path}")
        return self._config

    def get_config(self):
        """Get the loaded AQUA analysis configuration."""
        return self._load_config()

    def _get_aqua_paths(self):
        """Get and cache AQUA core and diagnostics paths."""
        if self._aqua_core_path is not None:
            return self._aqua_core_path, self._aqua_diagnostics_path, self._aqua_configdir

        self._aqua_core_path = str(pypath.files("aqua.core"))
        try:
            self._aqua_diagnostics_path = str(pypath.files("aqua.diagnostics"))
        except ModuleNotFoundError:
            self._aqua_diagnostics_path = ""
            self.logger.error("aqua.diagnostics package not found; AQUA_DIAGNOSTICS will be empty.")

        self._aqua_configdir = ConfigPath().configdir

        self.logger.debug(f"AQUA core path: {self._aqua_core_path}")
        self.logger.debug(f"AQUA diagnostics path: {self._aqua_diagnostics_path}")
        self.logger.debug(f"AQUA config dir: {self._aqua_configdir}")

        return self._aqua_core_path, self._aqua_diagnostics_path, self._aqua_configdir

    def get_aqua_paths(self):
        """Get the cached AQUA core and diagnostics paths."""
        return self._get_aqua_paths()

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
            self.logger.error(f"Error running command {cmd}: {e}")
            raise

    def run_diagnostic_tool(
        self,
        collection: str,
        tool: str,
        script_path: str,
        extra_args: str,
        loglevel: str = "INFO",
        logfile: str = "aqua-diagnostic-tool.log",
    ):
        """
        Run the diagnostic tool script with specified arguments.

        Args:
            collection (str): Name of the diagnostic collection.
            tool (str): Name of the diagnostic tool to use.
            script_path (str): Path to the diagnostic tool script.
            extra_args (str): Additional arguments for the script.
            loglevel (str): Log level to use.
            logfile (str): Path to the logfile for capturing the command output.
        """
        try:
            logfile = os.path.expandvars(logfile)
            create_folder(os.path.dirname(logfile))

            cmd = f"python {script_path} {extra_args} -l {loglevel} > {logfile} 2>&1"
            self.logger.info(f"Running tool {tool} for diagnostic collection {collection}")
            self.logger.debug(f"Command: {cmd}")

            process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)

            if process.returncode != 0:
                self.logger.error(f"Error running tool {tool} for diagnostic collection {collection}: {process.stderr}")
            else:
                self.logger.info(f"Tool {tool} for diagnostic collection {collection} completed successfully.")
        except (OSError, subprocess.SubprocessError) as e:
            self.logger.error(f"Failed to run tool {tool} for diagnostic collection {collection}: {e}")

    def _build_extra_args(self, **kwargs):
        """Build command line arguments from key-value pairs, skipping None values."""
        args = ""
        for flag, value in kwargs.items():
            if value is not None:
                args += f" --{flag} {value}"
        return args

    def run_diagnostic_collection(
        self,
        collection: str,
        serial: bool = False,
        regrid: str = None,
        cli: dict = None,
        diag_config=None,
        catalog=None,
        model="default_model",
        exp="default_exp",
        source="default_source",
        source_oce=None,
        startdate=None,
        enddate=None,
        realization=None,
        output_dir="./output",
        loglevel="INFO",
        cluster=None,
        exp_kind_dict=None,
    ):
        """
        Run the diagnostic collection and log the output, handling parallel processing if required.

        Args:
            collection (str): Name of the diagnostic collection.
            serial (bool): Whether to run in serial mode. When False, the dask parallel execution will be used.
            regrid (str): Regrid option.
            cli (dict): CLI definitions for the tools.
            diag_config (dict): Configuration dictionary loaded from YAML.
            catalog (str): Catalog name.
            model (str): Model name.
            exp (str): Experiment name.
            source (str): Source name.
            source_oce (str): Extra source name for ocean data when both are needed.
            startdate (str): Start date (YYYY-MM-DD). Defaults to None.
            enddate (str): End date (YYYY-MM-DD). Defaults to None.
            realization (str): Realization name. Defaults to None.
            output_dir (str): Directory to save output.
            loglevel (str): Log level for the tool.
            cluster: Dask cluster scheduler address.
            exp_kind_dict: Dictionary containing experiment kind configurations, if applicable.
        """
        if cli is None:
            cli = {}

        # Internal naming scheme:
        # collection: the name of the wrapper metadiagnostic, e.g. atmosphere2d, climate_metrics, etc.
        # tool: the name of the individual command-line tool being run, e.g. biases, ecmean, etc.

        output_dir = os.path.expandvars(output_dir)
        create_folder(output_dir)

        # run individual tools in serial mode
        for tool, tool_config in diag_config.items():
            self.logger.info(f"Configuring tool {tool} for diagnostic collection {collection}")

            cli_path = cli.get(tool)
            if cli_path is None:
                self.logger.error("CLI path for tool '%s' not found, skipping.", tool)
                continue

            if not os.path.exists(cli_path):
                self.logger.error("Script for tool '%s' not found at path: %s, skipping", tool, cli_path)
                continue

            outname = f"{output_dir}/{tool_config.get('outname', collection)}"
            extra_args = tool_config.get("extra", "")

            # Build conditional arguments
            if regrid:
                extra_args += f" --regrid {regrid}"

            if not serial:
                tool_nworkers = tool_config.get("nworkers")
                tool_nthreads = tool_config.get("nthreads")
                if tool_nworkers is not None:
                    extra_args += f" --nworkers {tool_nworkers}"
                if tool_nthreads is not None:
                    extra_args += f" --nthreads {tool_nthreads}"

            # This is needed for ECmean which uses multiprocessing
            if cluster and not tool_config.get("nocluster", False):
                extra_args += f" --cluster {cluster}"

            # Add standard arguments using helper function
            extra_args += self._build_extra_args(
                catalog=catalog,
                realization=realization,
                startdate=startdate,
                enddate=enddate,
            )

            # pass source_oce only if allowed by the diagnostic config file
            if tool_config.get("source_oce", False) and source_oce:
                extra_args += f" --source_oce {source_oce}"

            cfgs = to_list(tool_config.get("config"))
            if not cfgs:
                self.logger.error(f"Config for tool '{tool}' not found, skipping.")
                continue

            # update cfgs with experiment kind templating if exp_kind_dict is provided
            if exp_kind_dict:
                cfgs = self.configure_template_configs(cfgs, exp_kind_dict)

            for i, cfg in enumerate(cfgs, start=1):
                args = f"--model {model} --exp {exp} --source {source} --outputdir {outname} {extra_args} --config {cfg}"
                if len(cfgs) == 1:
                    logfile = f"{output_dir}/{collection}-{tool}.log"
                else:
                    logfile = f"{output_dir}/{collection}-{tool}-{i}.log"

                self.run_diagnostic_tool(
                    collection=collection,
                    tool=tool,
                    script_path=cli_path,
                    extra_args=args,
                    loglevel=loglevel,
                    logfile=logfile,
                )

            # remove temporary rendered config files created when using experiment kind templating
            if exp_kind_dict:
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

        self.logger.info(f"Configuring experiment kind: {exp_kind} using config file: {exp_kind_file}")
        complete_dictionary = load_yaml(exp_kind_file)

        if exp_kind not in complete_dictionary:
            self.logger.error(f"Experiment kind '{exp_kind}' not found in config file '{exp_kind_file}'. Default selected")
        return complete_dictionary.get(exp_kind, "default")

    def configure_template_configs(self, cfgs, exp_kind_dict):
        """
        Run jinja templating on the config files based on the experiment kind dictionary.
        Then dump them into a temporary folder and return the list of new config paths.

        Args:
            cfgs (list): List of config file paths to render.
            exp_kind_dict (dict): Dictionary of template variables for Jinja rendering.

        Returns:
            list: List of paths to the rendered config files in a temporary directory.
        """
        temp_dir = tempfile.mkdtemp(prefix="aqua_analysis_configs_")
        self.logger.debug("Temporary config directory: %s", temp_dir)

        new_cfg_paths = []
        for cfg in cfgs:
            rendered_cfg = load_yaml(cfg, definitions=exp_kind_dict, strict=True)
            new_cfg_path = os.path.join(temp_dir, os.path.basename(cfg))
            dump_yaml(new_cfg_path, rendered_cfg)
            self.logger.info("Rendered config saved to: %s", new_cfg_path)
            new_cfg_paths.append(new_cfg_path)

        return new_cfg_paths
