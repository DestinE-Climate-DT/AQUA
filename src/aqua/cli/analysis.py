import os
import sys
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dask.distributed import LocalCluster
from aqua.analysis import run_diagnostic_func, run_command, get_aqua_paths
from aqua.util import load_yaml, create_folder, ConfigPath, format_realization
from aqua.logger import log_configure


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
    parser.add_argument("--realization", type=str, default=None, help="Realization (default: None)")
    parser.add_argument("-d", "--outputdir", type=str, help="Output directory")
    parser.add_argument("-f", "--config", type=str, required=False, default=None,
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

    return parser

def analysis_execute(args):
    """
    Executing the AQUA analysis by parsing the arguments and configuring the machinery
    """
    loglevel = args.loglevel
    logger = log_configure(loglevel, 'AQUA Analysis')

    aqua_path, aqua_configdir, aqua_config_path = get_aqua_paths(args=args, logger=logger)

    config = load_yaml(aqua_config_path)
    loglevel = args.loglevel or config.get('job', {}).get('loglevel', "info")
    logger = log_configure(loglevel.lower(), 'AQUA Analysis')

    model = args.model or config.get('job', {}).get('model')
    exp = args.exp or config.get('job', {}).get('exp')
    source = args.source or config.get('job', {}).get('source', 'lra-r100-monthly')
    realization = args.realization if args.realization else config.get('job', {}).get('realization', None)
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
            logger.error(f"Model = {model}, Experiment = {exp}, Source = {source} triplet not found in any installed catalog.")
            sys.exit(1)

    outputdir = os.path.expandvars(args.outputdir or config.get('job', {}).get('outputdir', './output'))
    max_threads = args.threads

    logger.debug(f"outputdir: {outputdir}")
    logger.debug(f"max_threads: {max_threads}")

    realization_folder = format_realization(realization)
    output_dir = f"{outputdir}/{catalog}/{model}/{exp}/{realization_folder}"
    output_dir = os.path.expandvars(output_dir)

    os.environ["OUTPUT"] = output_dir
    os.environ["AQUA"] = aqua_path
    os.environ["AQUA_CONFIG"] = aqua_configdir if 'AQUA_CONFIG' not in os.environ else os.environ["AQUA_CONFIG"]
    create_folder(output_dir, loglevel=loglevel)

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
        if realization:
            command += f" --realization {realization}"
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
                realization=realization,
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
    args = analysis_parser().parse_args(sys.argv[1:])
    analysis_execute(args)
