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

    def get_config(self):
        """Get the loaded AQUA analysis configuration."""

        if not os.path.exists(self.config_file_path):
            self.logger.error("Config file %s not found.", self.config_file_path)
            sys.exit(1)

        self.config = load_yaml(self.config_file_path)
        self.logger.info("AQUA analysis config loaded: %s", self.config_file_path)
        return self.config

    def set_catalog_model_exp_source(self, args, job_config):
        """Get catalog, model, experiment, and source from command-line arguments or job configuration."""

        self.model = get_arg(args, "model", None, config=job_config)
        self.exp = get_arg(args, "exp", None, config=job_config)
        self.source = get_arg(args, "source", None, config=job_config)
        self.source_oce = get_arg(args, "source_oce", None, config=job_config)

        # guard
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

        # catalog
        self.catalog = get_arg(args, "catalog", None, config=job_config)
        if self.catalog:
            self.logger.info("Requested catalog: %s", self.catalog)
        else:
            cat, _ = ConfigPath().browse_catalogs(self.model, self.exp, self.source)
            if cat:
                catalog = cat[0]
                self.logger.info("Automatically determined catalog: %s", catalog)
            else:
                self.logger.error(
                    "Model = %s, Experiment = %s, Source = %s triplet not found in any installed catalog.",
                    self.model,
                    self.exp,
                    self.source,
                )
                sys.exit(1)

    def set_startdate_enddate(self, args, job_config):
        """Get startdate and enddate from command-line arguments or job configuration."""
        self.startdate = get_arg(args, "startdate", None, config=job_config)
        self.enddate = get_arg(args, "enddate", None, config=job_config)

        if self.startdate and self.enddate:
            self.logger.info("Requested time range: %s to %s", self.startdate, self.enddate)
        else:
            self.logger.info("No time range specified; using all available data.")

    def set_regrid_option(self, args, job_config):
        """Get regrid option from command-line arguments or job configuration and set it to None if it is False."""

        self.regrid = get_arg(args, "regrid", "False", config=job_config)
        if self.regrid is False or self.regrid.lower() == "false":
            self.regrid = None

    def set_realization(self, args, job_config):
        """Get realization from command-line arguments or job configuration and format it."""

        realization = get_arg(args, "realization", None, config=job_config)
        self.realization = format_realization(realization)
        self.logger.info("Input realization formatted to: %s", realization)

    def get_aqua_paths(self):
        """Get and cache AQUA core and diagnostics paths."""

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
        script_path: str,
        output_dir: str,
        logfile: str = "setup_checker.log",
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
        self.logger.info("Running setup checker")
        extra_args = self.build_extra_args(regrid=self.regrid, catalog=self.catalog, realization=self.realization)
        cmd = (
            f"python {script_path} --model {self.model} --exp {self.exp} --source {self.source} --yaml {output_dir}"
            f"{extra_args} -l {self.loglevel}"
        )
        self.logger.debug("Command: %s", cmd)

        result = self.run_command(cmd, logfile)
        return result

    @staticmethod
    def build_extra_args(**kwargs):
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
        cli: dict = None,
        diag_config=None,
        output_dir="./output",
        cluster=None,
    ):
        """
        Run the diagnostic collection and log the output, handling parallel processing if required.

        Args:
            collection (str): Name of the diagnostic collection.
            serial (bool): Whether to run in serial mode. When False, the dask parallel execution will be used.
            regrid (str): Regrid option.
            cli (dict): CLI definitions for the tools.
            diag_config (dict): Configuration dictionary loaded from YAML.
            realization (str): Realization name. Defaults to None.
            output_dir (str): Directory to save output.
            cluster: Dask cluster scheduler address.
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
            self.logger.info("Configuring tool %s for diagnostic collection %s", tool, collection)

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
            if self.regrid:
                extra_args += f" --regrid {self.regrid}"

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
                    logfile = f"{output_dir}/{collection}-{tool}.log"
                else:
                    logfile = f"{output_dir}/{collection}-{tool}-{i}.log"

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
