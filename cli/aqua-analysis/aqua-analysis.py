from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import subprocess
import argparse
import logging
from dask.distributed import LocalCluster
from aqua.logger import log_configure
from aqua.util import load_yaml, create_folder, ConfigPath

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

def run_diagnostic(diagnostic: str, script_path: str, extra_args: str, loglevel: str = 'INFO', logger=None, logfile: str = 'diagnostic.log'):
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

def run_diagnostic_func(diagnostic: str, parallel: bool = False, 
                        config=None, model='default_model', exp='default_exp', 
                        source='default_source', output_dir='./output', loglevel='INFO', 
                        logger=None, aqua_path='', cluster=None):
    """
    Run the diagnostic and log the output, handling parallel processing if required.

    Args:
        diagnostic (str): Name of the diagnostic to run.
        parallel (bool): Whether to run in parallel mode.
        config (dict): Configuration dictionary loaded from YAML.
        model (str): Model name.
        exp (str): Experiment name.
        source (str): Source name.
        output_dir (str): Directory to save output.
        loglevel (str): Log level for the diagnostic.
        logger: Logger instance for logging messages.
        aqua_path (str): AQUA path.
        cluster: Dask cluster scheduler address.
    """
    diagnostic_config = config.get('diagnostics', {}).get(diagnostic)
    if diagnostic_config is None:
        logger.error(f"Diagnostic '{diagnostic}' not found in the configuration.")
        return

    output_dir = os.path.expandvars(output_dir)
    create_folder(output_dir)

    logfile = f"{output_dir}/{diagnostic}.log"
    extra_args = diagnostic_config.get('extra', "")

    if parallel:
        nworkers = diagnostic_config.get('nworkers')
        if nworkers is not None:
            extra_args += f" --nworkers {nworkers}"

    if cluster and not diagnostic_config.get('nocluster', False):  # This is needed for ECmean which uses multiprocessing
        extra_args += f" --cluster {cluster}"  

    outname = f"{output_dir}/{diagnostic_config.get('outname', diagnostic)}"
    args = f"--model {model} --exp {exp} --source {source} --outputdir {outname} {extra_args}"

    run_diagnostic(
        diagnostic=diagnostic,
        script_path=os.path.join(aqua_path, "diagnostics", diagnostic_config.get('script_path', f"{diagnostic}/cli/cli_{diagnostic}.py")),
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

    parser.add_argument("-a", "--model_atm", type=str, help="Atmospheric model")
    parser.add_argument("-o", "--model_oce", type=str, help="Oceanic model")
    parser.add_argument("-m", "--model", type=str, help="Model (atmospheric and oceanic)")
    parser.add_argument("-e", "--exp", type=str, help="Experiment")
    parser.add_argument("-s", "--source", type=str, help="Source")
    parser.add_argument("-d", "--outputdir", type=str, help="Output directory")
    parser.add_argument("-f", "--config", type=str, help="Configuration file")
    parser.add_argument("-c", "--catalog", type=str, help="Catalog")
    parser.add_argument("--local_clusters", action="store_true", help="Use separate local clusters instead of single global one")
    parser.add_argument("-p", "--parallel", action="store_true", help="Run diagnostics in parallel with a cluster")
    parser.add_argument("-t", "--threads", type=int, default=-1, help="Maximum number of threads")
    parser.add_argument("-l", "--loglevel", type=lambda s: s.upper(),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default=None, help="Log level")

    return parser.parse_args()


def get_aqua_paths(*, args, logger):
    """
    Get both the AQUA path and the AQUA config path.

    Args:
        args: Command-line arguments.
        logger: Logger instance for logging messages.

    Returns:
        tuple: AQUA path and configuration path.
    """
    try:
        aqua_path = subprocess.run(["aqua", "--path"], stdout=subprocess.PIPE, text=True).stdout.strip()
        if not aqua_path:
            raise ValueError("AQUA path is empty.")
        aqua_path = os.path.abspath(os.path.join(aqua_path, "..", ".."))
        logger.info(f"AQUA path: {aqua_path}")

        configdir = ConfigPath(loglevel='warning').configdir

        # Expand the config path if provided, otherwise use the default path
        if args.config and args.config.strip():
            aqua_config_path = os.path.expandvars(args.config)
        else:
            aqua_config_path = os.path.join(configdir, "config.aqua-analysis.yaml")
        # Check if the config file exists
        if not os.path.exists(aqua_config_path):
            logger.error(f"Configuration file not found: {aqua_config_path}")
            sys.exit(1)

        logger.info(f"AQUA config path: {aqua_config_path}")
        return aqua_path, aqua_config_path
    except Exception as e:
        logger.error(f"Error getting AQUA path or config: {e}")
        sys.exit(1)


def main():
    """
    Main entry point for running the diagnostics.
    """
    args = get_args()
    logger = log_configure('warning', 'AQUA Analysis')

    aqua_path, aqua_config_path = get_aqua_paths(args=args, logger=logger)
    config = load_yaml(aqua_config_path)
    loglevel = args.loglevel or config.get('job', {}).get('loglevel', "info")
    logger = log_configure(loglevel.lower(), 'AQUA Analysis')

    catalog = args.catalog or config['models'][0]['catalog']
    model = args.model or config['models'][0]['model']
    exp = args.exp or config['models'][0]['exp']
    source = args.source or config['models'][0]['source']

    outputdir = os.path.expandvars(args.outputdir or config['job'].get('output').get('outputdir'))
    max_threads = args.threads

    logger.debug(f"outputdir: {outputdir}")
    logger.debug(f"max_threads: {max_threads}")
    logger.debug(f"catalog: {catalog}")

    diagnostics = config.get('diagnostics', {}).get('run')
    if not diagnostics:
        logger.error("No diagnostics found in configuration.")
        sys.exit(1)

    if not all([model, exp, source]):
        logger.error("Model, experiment, and source must be specified either in config or as command-line arguments.")
        sys.exit(1)
    else:
        logger.info(f"Successfully validated inputs: Model = {model}, Experiment = {exp}, Source = {source}.")

    output_dir = f"{outputdir}/{model}/{exp}"
    output_dir = os.path.expandvars(output_dir)
    os.environ["OUTPUT"] = output_dir
    os.environ["AQUA"] = aqua_path
    create_folder(output_dir)

    run_dummy = config.get('job', {}).get('run_dummy')
    logger.debug(f"run_dummy: {run_dummy}")
    if run_dummy:
        dummy_script = os.path.join(aqua_path, "diagnostics/dummy/cli/cli_dummy.py")
        output_log_path = os.path.expandvars(f"{output_dir}/setup_checker.log")
        command = f"python {dummy_script} --model_atm {model} --model_oce {model} --exp {exp} --source {source} -l {loglevel}"
        logger.info("Running setup checker")
        logger.debug(f"Command: {command}")
        result = run_command(command, log_file=output_log_path, logger=logger)

        if result == 1:
            logger.critical("Setup checker failed, exiting.")
            sys.exit(1)
        elif result == 2:
            logger.warning("Atmospheric model not found, it will be skipped.")
        elif result == 3:
            logger.warning("Oceanic model not found, it will be skipped.")
        else:
            logger.info("Setup checker completed successfully.")

    if args.parallel:
        if args.local_clusters:
            logger.info("Running diagnostics in parallel with separate local clusters.")
            cluster = None
            cluster_address = None
        else:
            nthreads = config.get('cluster', {}).get('threads', 2)
            nworkers = config.get('cluster', {}).get('workers', 64)
            mem_limit = config.get('cluster', {}).get('memory_limit', "3.1GiB")

            cluster = LocalCluster(threads_per_worker=nthreads, n_workers=nworkers, memory_limit=mem_limit, silence_logs=logging.ERROR)  # avoids excessive logging (see https://github.com/dask/dask/issues/9888)
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
                model=model,
                exp=exp,
                source=source,
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
