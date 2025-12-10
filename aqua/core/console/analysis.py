"""AQUA analysis command line interface."""

import os
import sys
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dask.distributed import LocalCluster
from aqua.core.analysis import run_diagnostic_func, run_command, get_aqua_paths
from aqua.core.util import load_yaml, create_folder, format_realization
from aqua.core.configurer import ConfigPath
from aqua.core.util import expand_env_vars
from aqua.core.logger import log_configure
from importlib import resources as pypath


def analysis_parser(parser=None):
    """
    Parser for the AQUA analysis command line interface.
    
    Args:
        parser (argparse.ArgumentParser, optional): An existing parser to extend. If None,
            a new parser will be created.
    
    Returns:
        argparse.ArgumentParser: The configured argument parser.
    """
    if parser is None:
        parser = argparse.ArgumentParser(description="Run AQUA diagnostics.")

    parser.add_argument("-m", "--model", type=str, help="Model (atmospheric and oceanic)")
    parser.add_argument("-e", "--exp", type=str, help="Experiment")
    parser.add_argument("-s", "--source", type=str, help="Source")
    parser.add_argument("--source_oce", type=str, help="Extra source for oceanic data when --source is used for atmospheric data and both are needed")
    parser.add_argument("--realization", type=str, help="Realization (default: None)")
    parser.add_argument("-d", "--outputdir", type=str, help="Output directory")
    parser.add_argument("-f", "--config", type=str, help="Configuration file")
    parser.add_argument("-c", "--catalog", type=str, help="Catalog")
    parser.add_argument("--regrid", type=str, default="False",
                        help="Regrid option (Target grid/False). If False, no regridding will be performed.")
    parser.add_argument("--local_clusters", action="store_true",
                        help="Use separate local clusters instead of single global one")
    parser.add_argument("-p", "--parallel", action="store_true", help="Run diagnostics in parallel with a cluster")
    parser.add_argument("-t", "--threads", type=int, default=-1, help="Maximum number of threads")
    parser.add_argument("--startdate", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--enddate", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("-l", "--loglevel", type=str.upper,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default="INFO", help="Log level")

    return parser

def analysis_execute(args):
    """
    Executing the AQUA analysis by parsing the arguments and configuring the machinery
    """
    loglevel = args.loglevel
    logger = log_configure(loglevel, 'AQUA Analysis')

    aqua_core_path, aqua_diagnostics_path, aqua_configdir, aqua_config_path = get_aqua_paths(args=args, logger=logger)

    config = load_yaml(aqua_config_path)
    loglevel = args.loglevel or config.get('job', {}).get('loglevel', "info")
    logger = log_configure(log_level=loglevel.lower(), log_name='AQUA Analysis')

    model = args.model or config.get('job', {}).get('model')
    exp = args.exp or config.get('job', {}).get('exp')
    source = args.source or config.get('job', {}).get('source', 'lra-r100-monthly')
    source_oce = args.source_oce or config.get('job', {}).get('source_oce')
    realization = args.realization if args.realization else config.get('job', {}).get('realization')

    # startdate and enddate
    startdate = args.startdate or config.get('job', {}).get('startdate')
    enddate = args.enddate or config.get('job', {}).get('enddate')
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
        logger.info(
            "Requested experiment: Model = %s, Experiment = %s, Source = %s. Source_oce = %s",
            model, exp, source, source_oce
        )

    catalog = args.catalog or config.get('job', {}).get('catalog')
    if catalog:
        logger.info("Requested catalog: %s", catalog)
    else:
        cat, _ = ConfigPath().browse_catalogs(model, exp, source)
        if cat:
            catalog = cat[0]
            logger.info("Automatically determined catalog: %s", catalog)
        else:
            logger.error(
                "Model = %s, Experiment = %s, Source = %s triplet not found in any installed catalog.",
                model, exp, source
            )
            sys.exit(1)

    outputdir = os.path.expandvars(args.outputdir or config.get('job', {}).get('outputdir', './output'))
    max_threads = args.threads

    logger.debug("outputdir: %s", outputdir)
    logger.debug("max_threads: %d", max_threads)

    # Format the realization string by prepending 'r' if it is a digit or setting a default `r1`.
    realization = format_realization(realization)
    logger.info("Input realization formatted to: %s", realization)

    output_dir = os.path.join(outputdir, catalog, model, exp, realization)
    output_dir = os.path.expandvars(output_dir)

    # Set Dask timeouts if not already defined in the environment
    if "DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT" not in os.environ:
        connect_timeout = config.get('cluster', {}).get('connect_timeout', None)
        if connect_timeout:
            os.environ["DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT"] = f"{connect_timeout}s"  # increase timeout (certainly needed on lumi, possibly good anyway)
    if "DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP" not in os.environ:
        tcp_timeout = config.get('cluster', {}).get('tcp_timeout', None)
        if tcp_timeout:
            os.environ["DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP"] = f"{tcp_timeout}s"  # optional, might be good

    os.environ["OUTPUT"] = output_dir
    os.environ["AQUA_CORE"] = aqua_core_path
    os.environ["AQUA_DIAGNOSTICS"] = aqua_diagnostics_path
    os.environ["AQUA_CONFIG"] = aqua_configdir if 'AQUA_CONFIG' not in os.environ else os.environ["AQUA_CONFIG"]
    create_folder(output_dir, loglevel=loglevel)

    # expand the environment variables in the entire config
    config = expand_env_vars(config)

    run_checker = config.get('job', {}).get('run_checker', False)
    if run_checker:
        logger.info("Running setup checker")
        checker_script_path = os.path.join(pypath.files('aqua.core'), 'analysis', 'cli_checker.py')
        output_log_path = os.path.expandvars(f"{output_dir}/setup_checker.log")
        command = f"python {checker_script_path} --model {model} --exp {exp} --source {source} -l {loglevel} --yaml {output_dir}"
        if regrid:
            command += f" --regrid {regrid}"
        if catalog:
            command += f" --catalog {catalog}"
        if realization:
            command += f" --realization {realization}"
        logger.debug("Command: %s", command)
        result = run_command(command, log_file=output_log_path, logger=logger)

        if result == 1:
            logger.critical("Setup checker failed, exiting.")
            sys.exit(1)
        elif result == 0:
            logger.info("Setup checker completed successfully.")
        else:
            logger.error("Setup checker returned exit code %s, check the logs for more information.", result)

    run = config.get('run', [])
    if not run:
        logger.error("No run block found in configuration.")
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

            # silence_logs to avoids excessive logging (see https://github.com/dask/dask/issues/9888)
            cluster = LocalCluster(
                threads_per_worker=nthreads,
                n_workers=nworkers,
                memory_limit=mem_limit,
                silence_logs=logging.ERROR
            )
            cluster_address = cluster.scheduler_address
            logger.info("Initialized global dask cluster %s providing %d workers.", cluster_address, len(cluster.workers))
    else:
        logger.info("Running diagnostics without a dask cluster.")
        cluster = None
        cluster_address = None

    # read cli definitions and prepend script path
    cli = config.get('cli', {})
    script_dir = config.get('job', {}).get("script_path_base")  # we were not using this key
    if script_dir:
        for diag in cli:
            cli[diag] = os.path.join(script_dir, cli[diag])

    # Internal naming scheme:
    # diagnostic: the name of the wrapper metadiagnostic, e.g. atmosphere2d, climate_metrics, etc.
    # tool: the name of the individual command-line tool being run, e.g. biases, ecmean, etc.
    for diag_group in run:

        with ThreadPoolExecutor(max_workers=max_threads if max_threads > 0 else None) as executor:
            futures = []
            for diagnostic in diag_group:

                logger.info("Starting diagnostic: %s", diagnostic)
                diag_config = config.get('diagnostics', {}).get(diagnostic)
                if diag_config is None:
                    logger.error("Diagnostic '%s' not found in the configuration, skipping.", diagnostic)
                    continue

                futures.append(executor.submit(
                    run_diagnostic_func,
                    diagnostic=diagnostic,
                    parallel=args.parallel,
                    diag_config=diag_config,
                    cli=cli,
                    catalog=catalog,
                    model=model,
                    exp=exp,
                    source=source,
                    source_oce=source_oce,
                    realization=realization,
                    startdate=startdate,
                    enddate=enddate,
                    regrid=regrid,
                    output_dir=output_dir,
                    loglevel=loglevel,
                    logger=logger,
                    cluster=cluster_address
                ))

            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    logger.error("Diagnostic raised an exception: %s", e)

    if cluster:
        cluster.close()
        logger.info("Dask cluster closed.")

    logger.info("All diagnostics finished.")

if __name__ == "__main__":
    args = analysis_parser().parse_args(sys.argv[1:])
    analysis_execute(args)
