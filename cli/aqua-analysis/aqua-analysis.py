#!/usr/bin/env python3

from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import subprocess
import argparse
import logging
from dask.distributed import LocalCluster
from aqua.logger import log_configure
from aqua.util import load_yaml, create_folder, ConfigPath
from aqua import __path__ as pypath


def run_command(cmd: str, log_file: str = None, logger=None) -> int:
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
                        source='default_source', output_dir='./output', loglevel='INFO',
                        logger=None, aqua_path='', cluster=None):
    """
    Run the diagnostic and log the output, handling parallel processing if required.

    Args:
        diagnostic (str): Name of the diagnostic to run.
        parallel (bool): Whether to run in parallel mode.
        config (dict): Configuration dictionary loaded from YAML.
        catalog (str): Catalog name.
        model (str): Model name.
        exp (str): Experiment name.
        source (str): Source name.
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


def get_args():
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Run diagnostics for the AQUA project.")

    parser.add_argument("-m", "--model", type=str, help="Model (atmospheric and oceanic)")
    parser.add_argument("-e", "--exp", type=str, help="Experiment")
    parser.add_argument("-s", "--source", type=str, help="Source")
    parser.add_argument("-d", "--outputdir", type=str, help="Output directory")
    parser.add_argument("-f", "--config", type=str, default="$AQUA/cli/aqua-analysis/config.aqua-analysis.yaml",
                        help="Configuration file")
    parser.add_argument("-c", "--catalog", type=str, help="Catalog")
    parser.add_argument("--regrid", type=str, default="False",
                        help="Regrid option (Target grid/False). If False, no regridding will be performed.")
    parser.add_argument("--local_clusters", action="store_true",
                        help="Use separate local clusters instead of single global one")
    parser.add_argument("-p", "--parallel", action="store_true", help="Run diagnostics in parallel with a cluster")
    parser.add_argument("-t", "--threads", type=int, default=-1, help="Maximum number of threads")
    parser.add_argument("-l", "--loglevel", type=lambda s: s.upper(),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default=None, help="Log level")

    return parser.parse_args()


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
    logger.info(f"AQUA path: {aqua_path}")

    aqua_configdir = ConfigPath().configdir
    logger.info(f"AQUA config dir: {aqua_configdir}")

    aqua_analysis_config_path = os.path.expandvars(args.config) if args.config and args.config.strip() else os.path.join(aqua_path, "cli/aqua-analysis/config.aqua-analysis.yaml")
    if not os.path.exists(aqua_analysis_config_path):
        logger.error(f"Config file {aqua_analysis_config_path} not found.")
        sys.exit(1)
    logger.info(f"AQUA analysis config path: {aqua_analysis_config_path}")

    return aqua_path, aqua_configdir, aqua_analysis_config_path


def main():
    """
    Main entry point for running the diagnostics.
    """
    args = get_args()
    logger = log_configure('warning', 'AQUA Analysis')

    aqua_path, aqua_configdir, aqua_config_path = get_aqua_paths(args=args, logger=logger)

    config = load_yaml(aqua_config_path)
    loglevel = args.loglevel or config.get('job', {}).get('loglevel', "info")
    logger = log_configure(loglevel.lower(), 'AQUA Analysis')

    model = args.model or config.get('job', {}).get('model')
    exp = args.exp or config.get('job', {}).get('exp')
    source = args.source or config.get('job', {}).get('source', 'lra-r100-monthly')
    # We get regrid option and then we set it to None if it is False
    # This avoids to add the --regrid argument to the command line
    # if it is not needed
    regrid = args.regrid or config.get('job', {}).get('regrid', False)
    if regrid is False or regrid.lower() == 'false':
        regrid = None

    if not all([model, exp, source]):
        logger.error("Model, experiment, and source must be specified either in config or as command-line arguments.")
        sys.exit(1)
    else:
        logger.info(f"Requested experiment: Model = {model}, Experiment = {exp}, Source = {source}.")

    catalog = args.catalog or config.get('job', {}).get('catalog')
    if catalog:
        logger.info(f"Requested catalog: {catalog}")
    else:
        cat, _ = ConfigPath().browse_catalogs(model, exp, source)
        if cat:
            catalog = cat[0]
            logger.info(f"Automatically determined catalog: {catalog}")
        else:
            logger.error("Model, experiment, and source triplet not found in any installed catalog.")
            sys.exit(1)

    outputdir = os.path.expandvars(args.outputdir or config.get('job', {}).get('outputdir', './output'))
    max_threads = args.threads

    logger.debug(f"outputdir: {outputdir}")
    logger.debug(f"max_threads: {max_threads}")

    output_dir = f"{outputdir}/{catalog}/{model}/{exp}"
    output_dir = os.path.expandvars(output_dir)

    os.environ["OUTPUT"] = output_dir
    os.environ["AQUA"] = aqua_path
    os.environ["AQUA_CONFIG"] = aqua_configdir if 'AQUA_CONFIG' not in os.environ else os.environ["AQUA_CONFIG"]
    create_folder(output_dir)

    run_checker = config.get('job', {}).get('run_checker', False)
    if run_checker:
        logger.info("Running setup checker")
        checker_script = os.path.join(aqua_path, "src/aqua_diagnostics/cli/cli_checker.py")
        output_log_path = os.path.expandvars(f"{output_dir}/setup_checker.log")
        command = f"python {checker_script} --model {model} --exp {exp} --source {source} -l {loglevel} --yaml {output_dir}"
        if regrid:
            command += f" --regrid {regrid}"
        if catalog:
            command += f" --catalog {catalog}"
        logger.debug(f"Command: {command}")
        result = run_command(command, log_file=output_log_path, logger=logger)

        if result == 1:
            logger.critical("Setup checker failed, exiting.")
            sys.exit(1)
        elif result == 0:
            logger.info("Setup checker completed successfully.")
        else:
            logger.error(f"Setup checker returned exit code {result}, check the logs for more information.")

    diagnostics = config.get('diagnostics', {}).get('run')
    if not diagnostics:
        logger.error("No diagnostics found in configuration.")
        sys.exit(1)

    if args.parallel:
        if args.local_clusters:
            logger.info("Running diagnostics in parallel with separate local clusters.")
            cluster = None
            cluster_address = None
        else:
            nthreads = config.get('cluster', {}).get('threads', 2)
            nworkers = config.get('cluster', {}).get('workers', 64)
            mem_limit = config.get('cluster', {}).get('memory_limit', "3.1GiB")

            cluster = LocalCluster(threads_per_worker=nthreads, n_workers=nworkers, memory_limit=mem_limit,
                                   silence_logs=logging.ERROR)  # avoids excessive logging (see https://github.com/dask/dask/issues/9888)
            cluster_address = cluster.scheduler_address
            logger.info(f"Initialized global dask cluster {cluster_address} providing {len(cluster.workers)} workers.")
    else:
        logger.info("Running diagnostics without a dask cluster.")
        cluster = None
        cluster_address = None

    with ThreadPoolExecutor(max_workers=max_threads if max_threads > 0 else None) as executor:
        futures = []
        for diagnostic in diagnostics:
            futures.append(executor.submit(
                run_diagnostic_func,
                diagnostic=diagnostic,
                parallel=args.parallel,
                config=config,
                catalog=catalog,
                model=model,
                exp=exp,
                source=source,
                regrid=regrid,
                output_dir=output_dir,
                loglevel=loglevel,
                logger=logger,
                aqua_path=aqua_path,
                cluster=cluster_address
            ))

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                logger.error(f"Diagnostic raised an exception: {e}")

    if cluster:
        cluster.close()
        logger.info("Dask cluster closed.")

    logger.info("All diagnostics finished.")


if __name__ == "__main__":
    main()
