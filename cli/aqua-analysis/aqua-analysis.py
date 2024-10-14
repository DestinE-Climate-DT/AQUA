from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import subprocess
import argparse
from aqua.exceptions import NoDiagnosticError, NoDataError, SetupCheckerFailed
from aqua.logger import log_configure
from aqua.util import load_yaml, create_folder, ConfigPath
from aqua.cli import AquaConsole


def run_command(cmd: str, *, log_file: str = None, logger=None) -> int:
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
        os.makedirs(log_dir, exist_ok=True)

        with open(log_file, 'w') as log:
            process = subprocess.run(cmd, shell=True, stdout=log, stderr=log, text=True)
            return process.returncode
    except Exception as e:
        logger.error(f"Error running command {cmd}: {e}")
        raise


def run_diagnostic(diagnostic: str, *, script_path: str, extra_args: str, loglevel: str, logger, logfile: str):
    """
    Run the diagnostic script with specified arguments.

    Args:
        diagnostic (str): Name of the diagnostic.
        script_path (str): Path to the diagnostic script.
        extra_args (str): Additional arguments for the script.
        loglevel (str): Log level to use.
        logger: Logger instance for logging messages.
        logfile (str): Path to the logfile for capturing the command output.
    """
    try:
        cmd = f"python {script_path} {extra_args} -l {loglevel} > {logfile} 2>&1"
        logger.info(f"Running diagnostic {diagnostic}")
        logger.debug(f"Command: {cmd}")

        # process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # _, stderr = process.stdout, process.stderr

        # if process.returncode != 0:
        #     logger.error(f"Error running diagnostic {diagnostic}: {stderr}")
        # else:
        #     logger.info(f"Diagnostic {diagnostic} completed successfully.")
    except Exception as e:
        logger.error(f"Failed to run diagnostic {diagnostic}: {e}")


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
    parser.add_argument("-f", "--config", type=str, default="$AQUA/cli/aqua-analysis/config.aqua-analysis.yaml",
                        help="Configuration file")
    parser.add_argument("-d", "--outputdir", type=str, help="Output directory")
    parser.add_argument("-c", "--catalog", type=str, help="Catalog")
    parser.add_argument("-p", "--parallel", action="store_true", help="Run diagnostics in parallel")
    parser.add_argument("-t", "--threads", type=int, default=-1, help="Maximum number of threads")
    parser.add_argument("-l", "--loglevel", type=lambda s: s.upper(),
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default="WARNING", help="Log level")

    return parser.parse_args()


def get_aqua_paths(args, logger):
    """
    Get both the AQUA path and the AQUA config path.

    Args:
        args: Command-line arguments.
        logger: Logger instance for logging messages.

    Returns:
        tuple: AQUA path and configuration path.
    """
    try:
        aqua_path = os.path.join(AquaConsole().aquapath, "..")
        logger.debug(f"AQUA path: {aqua_path}")

        std_config_path = os.path.join(aqua_path, "cli/aqua-analysis/config.aqua-analysis.yaml")
        aqua_config = os.path.expandvars(args.config) if args.config and args.config.strip() else std_config_path

        if not os.path.exists(aqua_config):
            logger.error(f"Config file {aqua_config} not found.")
            raise FileNotFoundError(f"Config file {aqua_config} not found.")

        logger.info(f"AQUA analysis config file: {aqua_config}")
        return aqua_path, aqua_config
    except Exception as e:
        raise Exception(f"Error getting AQUA path or config: {e}")


def run_diagnostic_func(diagnostic: str = None, parallel: bool = False,
                        config : str = None, catalog : str = None,
                        model : str = None, exp : str = None, source : str = None,
                        output_dir : str = None, loglevel : str = 'WARNING', logger = None,
                        aqua_config_path : str = None, aqua_path : str = None):
    """
    Run the diagnostic and log the output, handling parallel processing if required.

    Args:
        diagnostic (str): Name of the diagnostic to run.
        parallel (bool): Whether to run in parallel mode.
        config (dict): Configuration dictionary loaded from YAML.
        catalog (str): Catalog name. Not used yet by all the diagnostics.
        model (str): Model name.
        exp (str): Experiment name.
        source (str): Source name.
        output_dir (str): Directory to save output.
        loglevel (str): Log level for the diagnostic.
        logger: Logger instance for logging messages.
        aqua_config_path (str): Path to the diagnostics configuration files.
        aqua_path (str): Path to the AQUA repository.
    """
    diagnostic_config = config.get('diagnostics', {}).get(diagnostic)
    logger.debug(f"Diagnostic config: {diagnostic_config}")
    if diagnostic_config is None:
        logger.error(f"Diagnostic '{diagnostic}' not found in the configuration.")
        return

    logfile = f"{output_dir}/{diagnostic}.log"
    config_file = diagnostic_config.get('config', None)
    extra_args = diagnostic_config.get('extra', "")

    if config_file is not None:
        if 'AQUA_CONFIG' in config_file:
            config_file = config_file.replace('AQUA_CONFIG', aqua_path)
            logger.debug(f"Config file: {config_file}")
        extra_args += f" --config {config_file}"

    # Add parallel processing logic
    if parallel:
        nworkers = diagnostic_config.get('nworkers')
        if nworkers is not None:
            extra_args += f" --nworkers {nworkers}"

    outname = f"{output_dir}/{diagnostic_config.get('outname', diagnostic)}"
    args = f"--model {model} --exp {exp} --source {source} --outputdir {outname} {extra_args}"

    run_diagnostic(
        diagnostic=diagnostic,
        script_path=os.path.join(aqua_path, diagnostic_config.get('script_path', f"{diagnostic}/cli/cli_{diagnostic}.py")),
        extra_args=args,
        loglevel=loglevel,
        logger=logger,
        logfile=logfile
    )


def get_catalog(model: str, exp: str, source: str, catalog: str, logger):
    """Based on the model, experiment, and source, get the catalog name."""
    if catalog is None:
        logger.warning("Catalog not specified, guessing it")
        matched, failed = ConfigPath().browse_catalogs(model=model, exp=exp, source=source)
        if not matched:
            for _, value in failed.items():
                logger.error(value)
            raise NoDataError('Cannot find the triplet in any catalog. Check logger error for hints on possible typos')

        if len(matched) > 1:
            logger.warning('Multiple triplets found in %s, setting %s as the default', matched, matched[0])
        catalog = matched[0]

    return catalog


def main():
    """
    Main entry point for running the diagnostics.
    """
    args = get_args()
    logger = log_configure('WARNING', 'AQUA Analysis')

    aqua_path, aqua_config_path = get_aqua_paths(args=args, logger=logger)  # Get the AQUA path here
    os.environ['AQUA'] = aqua_path
    config = load_yaml(aqua_config_path)
    loglevel = args.loglevel or config.get('job', {}).get('loglevel', "WARNING")
    logger = log_configure(loglevel, 'AQUA Analysis')

    model = args.model or config.get('job', {}).get('model')
    exp = args.exp or config.get('job', {}).get('exp')
    source = args.source or config.get('job', {}).get('source')
    outputdir = args.outputdir or config.get('job', {}).get('outputdir', './output')
    max_threads = args.threads
    catalog = args.catalog or config.get('job', {}).get('catalog')

    if not all([model, exp, source]):
        logger.error("Model, experiment, and source must be specified either in config or as command-line arguments.")
        raise NoDataError("Model, experiment, and source must be specified.")
    else:
        logger.info(f"Successfully validated inputs: Model = {model}, Experiment = {exp}, Source = {source}.")

    catalog = get_catalog(model, exp, source, catalog, logger)

    logger.debug(f"outputdir: {outputdir}")
    logger.debug(f"max_threads: {max_threads}")
    logger.debug(f"catalog: {catalog}")

    diagnostics = config.get('diagnostics', {}).get('run')
    if not diagnostics:
        raise NoDiagnosticError("No diagnostics selected.")

    output_dir = os.path.join(outputdir, catalog, model, exp)
    logger.debug(f"output_dir: {output_dir}")
    create_folder(output_dir, loglevel=loglevel)

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
            raise SetupCheckerFailed("Setup checker failed.")
        elif result == 2:
            logger.critical("Atmospheric model not found, it will be skipped.")
        elif result == 3:
            logger.critical("Oceanic model not found, it will be skipped.")
        else:
            logger.info("Setup checker completed successfully.")

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
                aqua_config_path=aqua_config_path
            ))

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                logger.error(f"Diagnostic raised an exception: {e}")

    logger.info("All diagnostics finished successfully.")


if __name__ == "__main__":
    main()
