#!/usr/bin/env python3
"""
AQUA analysis module for running diagnostics and handling configurations.
"""

import os
import sys
import subprocess
from importlib import resources as pypath

from aqua.core.util import create_folder, to_list
from aqua.core.configurer import ConfigPath


def run_command(cmd: str, log_file: str, logger=None) -> int:
    """
    Run a system command and capture the exit code, redirecting output to the specified log file.

    Args:
        cmd (str): Command to run.
        log_file (str): Path to the log file to capture the command's output.
        logger: Logger instance for logging messages.

    Returns:
        int: The exit code of the command.
    """
    try:
        log_file = os.path.expandvars(log_file)
        log_dir = os.path.dirname(log_file)
        create_folder(log_dir)

        with open(log_file, 'w', encoding='utf-8') as log:
            process = subprocess.run(
                cmd, shell=True, stdout=log, stderr=log, text=True, check=False
            )
            return process.returncode
    except (OSError, subprocess.SubprocessError) as e:
        if logger:
            logger.error(f"Error running command {cmd}: {e}")
        raise


def run_diagnostic(diagnostic: str, script_path: str, extra_args: str,
                   loglevel: str = 'INFO', logger=None, logfile: str = 'diagnostic.log'):
    """
    Run the diagnostic script with specified arguments.

    Args:
        diagnostic (str): Name of the diagnostic.
        script_path (str): Path to the diagnostic script.
        extra_args (str): Additional arguments for the script.
        loglevel (str): Log level to use.
        logger: Logger instance for logging messages.
        logfile (str): Path to the logfile for capturing the command output.
        cluster (str): Dask cluster address.
    """
    try:
        logfile = os.path.expandvars(logfile)
        create_folder(os.path.dirname(logfile))

        cmd = f"python {script_path} {extra_args} -l {loglevel} > {logfile} 2>&1"
        logger.info(f"Running diagnostic {diagnostic}")
        logger.debug(f"Command: {cmd}")

        process = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False
        )

        if process.returncode != 0:
            logger.error(f"Error running diagnostic {diagnostic}: {process.stderr}")
        else:
            logger.info(f"Diagnostic {diagnostic} completed successfully.")
    except (OSError, subprocess.SubprocessError) as e:
        logger.error(f"Failed to run diagnostic {diagnostic}: {e}")


def _build_extra_args(**kwargs):
    """Build command line arguments from key-value pairs, skipping None values."""
    args = ""
    for flag, value in kwargs.items():
        if value is not None:
            args += f" --{flag} {value}"
    return args


def run_diagnostic_func(diagnostic: str, parallel: bool = False,
                        regrid: str = None, cli: dict = {},
                        diag_config=None, catalog=None, model='default_model', exp='default_exp',
                        source='default_source', source_oce=None,
                        startdate=None, enddate=None, realization=None,
                        output_dir='./output', loglevel='INFO',
                        logger=None, cluster=None):
    """
    Run the diagnostic and log the output, handling parallel processing if required.

    Args:
        diagnostic (str): Name of the diagnostic to run.
        parallel (bool): Whether to run in parallel mode.
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
        loglevel (str): Log level for the diagnostic.
        logger: Logger instance for logging messages.
        cluster: Dask cluster scheduler address.
    """

    # Internal naming scheme:
    # diagnostic: the name of the wrapper metadiagnostic, e.g. atmosphere2d, climate_metrics, etc.
    # tool: the name of the individual command-line tool being run, e.g. biases, ecmean, etc.

    output_dir = os.path.expandvars(output_dir)
    create_folder(output_dir)

    # run individual tools in serial mode
    for tool, tool_config in diag_config.items():

        logger.info(f"Running tool: {tool} for diagnostic: {diagnostic}")
        
        cli_path = cli.get(tool)
        if cli_path is None:
            logger.error("CLI path for tool '%s' not found, skipping.", tool)
            continue
 
        if not os.path.exists(cli_path):
            logger.error("Script for tool '%s' not found at path: %s, skipping", tool, cli_path)
            continue

        outname = f"{output_dir}/{tool_config.get('outname', diagnostic)}"
        extra_args = tool_config.get('extra', "")

        # Build conditional arguments
        if regrid:
            extra_args += f" --regrid {regrid}"

        if parallel:
            nworkers = tool_config.get('nworkers')
            if nworkers is not None:
                extra_args += f" --nworkers {nworkers}"

        # This is needed for ECmean which uses multiprocessing
        if cluster and not tool_config.get('nocluster', False):
            extra_args += f" --cluster {cluster}"

        # Add standard arguments using helper function
        extra_args += _build_extra_args(
            catalog=catalog,
            realization=realization,
            startdate=startdate,
            enddate=enddate
        )

        if tool_config.get('source_oce', False) and source_oce:  # pass source_oce only if allowed by the diagnostic config file
            extra_args += f" --source_oce {source_oce}"

        cfgs = to_list(tool_config.get('config'))
        if not cfgs:
            logger.error(f"Config for tool '{tool}' not found, skipping.")
            continue

        for i, cfg in enumerate(cfgs, start=1):
            args = f"--model {model} --exp {exp} --source {source} --outputdir {outname} {extra_args} --config {cfg}"
            if len(cfgs) == 1:
                logfile = f"{output_dir}/{diagnostic}-{tool}.log"
            else:
                logfile = f"{output_dir}/{diagnostic}-{tool}-{i}.log"

            run_diagnostic(
                diagnostic=diagnostic,
                script_path=cli_path,
                extra_args=args,
                loglevel=loglevel,
                logger=logger,
                logfile=logfile
            )

def get_aqua_paths(*, args, logger):
    """
    Get both the AQUA path and the AQUA-analysis config path.

    Args:
        args: Command-line arguments.
        logger: Logger instance for logging messages.

    Returns:
        tuple: AQUA path and configuration path.
    """
    aqua_core_path = str(pypath.files('aqua.core'))
    aqua_diagnostics_path = str(pypath.files('aqua.diagnostics'))

    logger.debug(f"AQUA core path: {aqua_core_path}")
    logger.debug(f"AQUA diagnostics path: {aqua_diagnostics_path}")

    aqua_configdir = ConfigPath().configdir
    logger.debug(f"AQUA config dir: {aqua_configdir}")

    aqua_analysis_config_path = os.path.expandvars(args.config) if args.config and args.config.strip() else os.path.join(aqua_configdir, "analysis/config.aqua-analysis.yaml")
    if not os.path.exists(aqua_analysis_config_path):
        logger.error(f"Config file {aqua_analysis_config_path} not found.")
        sys.exit(1)
    logger.info(f"AQUA analysis config path: {aqua_analysis_config_path}")

    return aqua_core_path, aqua_diagnostics_path, aqua_configdir, aqua_analysis_config_path
