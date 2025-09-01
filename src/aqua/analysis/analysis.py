#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import subprocess
from aqua.util import create_folder, ConfigPath
from aqua import __path__ as pypath


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

        with open(log_file, 'w') as log:
            process = subprocess.run(cmd, shell=True, stdout=log, stderr=log, text=True)
            return process.returncode
    except Exception as e:
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

        process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if process.returncode != 0:
            logger.error(f"Error running diagnostic {diagnostic}: {process.stderr}")
        else:
            logger.info(f"Diagnostic {diagnostic} completed successfully.")
    except Exception as e:
        logger.error(f"Failed to run diagnostic {diagnostic}: {e}")


def run_diagnostic_func(diagnostic: str, parallel: bool = False, regrid: str = None,
                        config=None, catalog=None, model='default_model', exp='default_exp',
                        source='default_source', realization=None,
                        output_dir='./output', loglevel='INFO',
                        logger=None, aqua_path='', cluster=None):
    """
    Run the diagnostic and log the output, handling parallel processing if required.

    Args:
        diagnostic (str): Name of the diagnostic to run.
        parallel (bool): Whether to run in parallel mode.
        regrid (str): Regrid option.
        config (dict): Configuration dictionary loaded from YAML.
        catalog (str): Catalog name.
        model (str): Model name.
        exp (str): Experiment name.
        source (str): Source name.
        realization (str): Realization name. Defaults to None.
        output_dir (str): Directory to save output.
        loglevel (str): Log level for the diagnostic.
        logger: Logger instance for logging messages.
        aqua_path (str): AQUA path.
        cluster: Dask cluster scheduler address.
    """

    script_dir = config.get('job', {}).get("script_path_base")  # we were not using this key
    if not script_dir:
        script_dir=os.path.join(aqua_path, "diagnostics")

    diagnostic_config = config.get('diagnostics', {}).get(diagnostic)
    if diagnostic_config is None:
        logger.error(f"Diagnostic '{diagnostic}' not found in the configuration.")
        return

    output_dir = os.path.expandvars(output_dir)
    create_folder(output_dir)

    logfile = f"{output_dir}/{diagnostic}.log"

    extra_args = diagnostic_config.get('extra', "")
    cfg = diagnostic_config.get('config')
    if cfg:
        extra_args += f" --config {cfg}"

    if regrid:
        extra_args += f" --regrid {regrid}"

    if parallel:
        nworkers = diagnostic_config.get('nworkers')
        if nworkers is not None:
            extra_args += f" --nworkers {nworkers}"

    if cluster and not diagnostic_config.get('nocluster', False):  # This is needed for ECmean which uses multiprocessing
        extra_args += f" --cluster {cluster}"

    if catalog:
        extra_args += f" --catalog {catalog}"

    if realization:
        extra_args += f" --realization {realization}"

    outname = f"{output_dir}/{diagnostic_config.get('outname', diagnostic)}"
    args = f"--model {model} --exp {exp} --source {source} --outputdir {outname} {extra_args}"

    run_diagnostic(
        diagnostic=diagnostic,
        script_path=os.path.join(script_dir, diagnostic_config.get('script_path', f"{diagnostic}/cli/cli_{diagnostic}.py")),
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
    aqua_path = os.path.abspath(os.path.join(pypath[0], "..", ".."))
    logger.debug(f"AQUA path: {aqua_path}")

    aqua_configdir = ConfigPath().configdir
    logger.debug(f"AQUA config dir: {aqua_configdir}")

    aqua_analysis_config_path = os.path.expandvars(args.config) if args.config and args.config.strip() else os.path.join(aqua_configdir, "analysis/config.aqua-analysis.yaml")
    if not os.path.exists(aqua_analysis_config_path):
        logger.error(f"Config file {aqua_analysis_config_path} not found.")
        sys.exit(1)
    logger.info(f"AQUA analysis config path: {aqua_analysis_config_path}")

    return aqua_path, aqua_configdir, aqua_analysis_config_path