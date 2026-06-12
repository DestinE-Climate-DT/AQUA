"""AQUA analysis command line interface."""

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib import resources as pypath

from dask.distributed import LocalCluster

from aqua.core.analysis import Analysis
from aqua.core.configurer import ConfigPath
from aqua.core.logger import log_configure
from aqua.core.util import create_folder, expand_env_vars, format_realization, get_arg


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
    # fmt: off
    # sources
    parser.add_argument("-c", "--catalog", type=str, help="Catalog")
    parser.add_argument("-m", "--model", type=str, help="Model (atmospheric and oceanic)")
    parser.add_argument("-e", "--exp", type=str, help="Experiment")
    parser.add_argument("-s", "--source", type=str, help="Source")
    parser.add_argument("--source_oce", type=str,
        help="Extra source for oceanic data when --source is used for atmospheric data and both are needed")
    parser.add_argument("--realization", type=str, help="Realization (default: None)")

    # default options for diagnostics
    parser.add_argument("--startdate", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--enddate", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--regrid", type=str, default="False",
                        help="Regrid option (Target grid/False). If False, no regridding will be performed.")

    # configuration
    parser.add_argument("-o", "--outputdir", type=str, help="Output directory")
    parser.add_argument("--config", type=str, help="Configuration file")
    parser.add_argument("-k", "--kind", type=str, help="Experiment kind to be run (e.g. historical, scenario, etc.)")

    # computation
    parser.add_argument("--serial", action="store_true", help="Disable dask cluster parallel execution")
    parser.add_argument("--nworkers", type=int, default=None,
                        help="Number of workers to use in the cluster (overrides config file)")
    parser.add_argument("--nthreads", type=int, default=None,
                        help="Number of threads per worker to use in the cluster (overrides config file)")
    parser.add_argument("--nmaxprocesses", type=int, default=-1,
                        help="Maximum number of processes to use in the ThreadPoolExecutor. Default==-1 (no limit)")

    # logger
    parser.add_argument("-l", "--loglevel", type=str.upper,
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        default="INFO", help="Log level")
    # fmt: on
    return parser


def analysis_execute(args):
    """
    Executing the AQUA analysis by parsing the arguments and configuring the machinery
    """
    loglevel = args.loglevel
    logger = log_configure(loglevel, "AQUA Analysis")

    # Initialize analyzer
    analyzer = Analysis(config_file_path=args.config, loglevel=loglevel)

    # Load config and get AQUA paths
    config = analyzer.get_config()
    aqua_core_path, aqua_diagnostics_path, aqua_configdir = analyzer.get_aqua_paths()

    # extract default
    job_config = config.get("job", {})
    cluster_config = config.get("cluster", {})

    # basic configuration
    model = get_arg(args, "model", None, config=job_config)
    exp = get_arg(args, "exp", None, config=job_config)
    source = get_arg(args, "source", None, config=job_config)
    source_oce = get_arg(args, "source_oce", None, config=job_config)

    # guard
    if not all([model, exp, source]):
        logger.error("Model, experiment, and source must be specified either in config or as command-line arguments.")
        sys.exit(1)
    else:
        logger.info(
            "Requested experiment: Model = %s, Experiment = %s, Source = %s. Source_oce = %s", model, exp, source, source_oce
        )

    # realizaiton, startdate and enddate
    startdate = get_arg(args, "startdate", None, config=job_config)
    enddate = get_arg(args, "enddate", None, config=job_config)
    realization = get_arg(args, "realization", None, config=job_config)
    # We get regrid option and then we set it to None if it is False
    # This avoids to add the --regrid argument to the command line
    # if it is not needed
    regrid = get_arg(args, "regrid", "False", config=job_config)
    if regrid is False or regrid.lower() == "false":
        regrid = None

    # catalog
    catalog = get_arg(args, "catalog", None, config=job_config)
    if catalog:
        logger.info("Requested catalog: %s", catalog)
    else:
        cat, _ = ConfigPath().browse_catalogs(model, exp, source)
        if cat:
            catalog = cat[0]
            logger.info("Automatically determined catalog: %s", catalog)
        else:
            logger.error(
                "Model = %s, Experiment = %s, Source = %s triplet not found in any installed catalog.", model, exp, source
            )
            sys.exit(1)

    # output directory and maximum parallel processes for the ThreadPoolExecutor
    outputdir = os.path.expandvars(args.outputdir or config.get("job", {}).get("outputdir", "./output"))
    nmaxprocesses = args.nmaxprocesses if args.nmaxprocesses > 0 else None
    logger.debug("outputdir: %s", outputdir)
    logger.debug("nmaxprocesses: %d", nmaxprocesses)

    # Format the realization string by prepending 'r' if it is a digit or setting a default `r1`.
    realization = format_realization(realization)
    logger.info("Input realization formatted to: %s", realization)

    output_dir = os.path.join(outputdir, catalog, model, exp, realization)
    output_dir = os.path.expandvars(output_dir)
    create_folder(output_dir, loglevel=loglevel)
    logger.debug("Output directory set to: %s", output_dir)

    # Set Dask timeouts if not already defined in the environment
    if "DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT" not in os.environ:
        connect_timeout = cluster_config.get("connect_timeout", None)
        if connect_timeout:
            # Increase timeout (certainly needed on LUMI, possibly useful elsewhere too).
            os.environ["DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT"] = f"{connect_timeout}s"
    if "DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP" not in os.environ:
        tcp_timeout = cluster_config.get("tcp_timeout", None)
        if tcp_timeout:
            os.environ["DASK_DISTRIBUTED__COMM__TIMEOUTS__TCP"] = f"{tcp_timeout}s"  # optional, might be good

    os.environ["OUTPUT"] = output_dir
    os.environ["AQUA_CORE"] = aqua_core_path
    os.environ["AQUA_DIAGNOSTICS"] = aqua_diagnostics_path
    os.environ["AQUA_CONFIG"] = aqua_configdir if "AQUA_CONFIG" not in os.environ else os.environ["AQUA_CONFIG"]

    # expand the environment variables in the entire config, extract again job and cluster config in case they were modified
    config = expand_env_vars(config)
    job_config = config.get("job", {})
    cluster_config = config.get("cluster", {})

    # read the experiment kind
    exp_kind_file = job_config.get("experiment_kind")
    analyzer.configure_experiment_kind(args.kind, exp_kind_file)

    # cli checker setup and run
    run_checker = job_config.get("run_checker", False)
    if run_checker:
        logger.info("Running setup checker")
        checker_script_path = os.path.join(pypath.files("aqua.core"), "analysis", "cli_checker.py")
        output_log_path = os.path.expandvars(f"{output_dir}/setup_checker.log")
        extra_args = analyzer.build_extra_args(regrid=regrid, catalog=catalog, realization=realization)
        command = (
            f"python {checker_script_path} --model {model} --exp {exp}"
            f"--source {source} -l {loglevel} --yaml {output_dir} {extra_args}"
        )
        # use the analsis class to build the command
        logger.debug("Command: %s", command)
        result = analyzer.run_command(command, log_file=output_log_path)

        if result == 1:
            logger.critical("Setup checker failed, exiting.")
            sys.exit(1)
        elif result == 0:
            logger.info("Setup checker completed successfully.")
        else:
            logger.error("Setup checker returned exit code %s, check the logs for more information.", result)

    # running or not
    run = config.get("run", [])
    if not run:
        logger.error("No run block found in configuration.")
        sys.exit(1)

    # confiuration of dask cluster
    if args.serial:
        if args.nworkers or args.nthreads:
            logger.warning("Serial execution selected, ignoring worker/thread settings.")
        logger.info("Running diagnostic collections without a dask cluster.")
        cluster, cluster_address = None, None
    else:
        nthreads = get_arg(args, "nthreads", 2, config=cluster_config, key="threads")
        nworkers = get_arg(args, "nworkers", 32, config=cluster_config, key="workers")
        mem_limit = cluster_config.get("memory_limit", "3.1GiB")
        logger.debug("Cluster configuration - nthreads: %d, nworkers: %d, memory_limit: %s", nthreads, nworkers, mem_limit)

        # silence_logs to avoids excessive logging (see https://github.com/dask/dask/issues/9888)
        cluster = LocalCluster(
            threads_per_worker=nthreads, n_workers=nworkers, memory_limit=mem_limit, silence_logs=logging.ERROR
        )
        cluster_address = cluster.scheduler_address
        logger.info("Initialized global dask cluster %s providing %d workers.", cluster_address, len(cluster.workers))

    # read cli definitions and prepend script path
    cli = config.get("cli", {})
    script_dir = job_config.get("script_path_base")  # we were not using this key
    if script_dir:
        for diag in cli:
            cli[diag] = os.path.join(script_dir, cli[diag])

    # Internal naming scheme:
    # collection: the name of the wrapper metadiagnostic, e.g. atmosphere2d, climate_metrics, etc.
    # tool: the name of the individual command-line tool being run, e.g. biases, ecmean, etc.
    for collections in run:
        with ThreadPoolExecutor(max_workers=nmaxprocesses) as executor:
            futures = []
            for collection in collections:
                logger.info("Starting diagnostic collection: %s", collection)
                diag_config = config.get("diagnostics", {}).get(collection)
                if diag_config is None:
                    logger.error("Diagnostic collection '%s' not found in the configuration, skipping.", collection)
                    continue

                futures.append(
                    executor.submit(
                        analyzer.run_diagnostic_collection,
                        collection=collection,
                        serial=args.serial,
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
                        cluster=cluster_address,
                    )
                )

            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    logger.error("Diagnostic collection raised an exception: %s", e)

    if cluster:
        cluster.close()
        logger.info("Dask cluster closed.")

    logger.info("All diagnostic collections finished.")


if __name__ == "__main__":
    args = analysis_parser().parse_args(sys.argv[1:])
    analysis_execute(args)
