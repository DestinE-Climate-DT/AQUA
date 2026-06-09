#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA regridding tool to create low resolution archive.
Make use of aqua.Drop class to perform the regridding.
Functionality can be controlled through CLI options and
a configuration yaml file.
"""

import argparse
import sys

from aqua import Drop
from aqua import __version__ as version
from aqua.core.util import get_arg, load_yaml, to_list


def drop_parser(parser=None):
    """
    Parse command line arguments for the DROP CLI

    Args:
        Optional part to be extended with DROP options

    Important: defaults are controlled by the _cfg approach below, not set in the parser
    """

    if parser is None:
        parser = argparse.ArgumentParser(description="AQUA DROP")
    # fmt: off
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-f', '--fix', action="store_true",
                        help='fixer on existing data')
    parser.add_argument('-w', '--workers', type=str,
                        help='number of dask workers')
    parser.add_argument('-d', '--definitive', action="store_true",
                        help='definitive run with files creation')
    parser.add_argument('-o', '--overwrite', action="store_true",
                        help='overwrite existing output')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')
    parser.add_argument('--monitoring', action="store_true",
                        help='enable the dask performance monitoring. Will run a single chunk')
    parser.add_argument('--catalog-entry', type=str, choices=['yes', 'no', 'only'],
                        help="Catalog entry behaviour [default: yes, or options.catalog_entry from config]: "
                             "'yes' writes data and creates catalog; "
                             "'no' writes data but skips catalog creation; "
                             "'only' skips data writing and only creates/updates the catalog entry.")
    parser.add_argument('--catalog', type=str,
                        help='catalog to be processed. Use with coherence with --model, -exp and --source')
    parser.add_argument('-m', '--model', type=str,
                        help='model to be processed. Use with coherence with --exp and --source')
    parser.add_argument('-e', '--exp', type=str,
                        help='experiment to be processed. Use with coherence with --source and --model')
    parser.add_argument('-s', '--source', type=str,
                        help='source to be processed. Use with coherence with --exp and --var')
    parser.add_argument('-v', '--var', type=str,
                        help='var to be processed. Use with coherence with --source')
    parser.add_argument('--no-validate', action="store_true",
                        help='skip pre-run integrity check on existing output files (speeds up startup)')
    parser.add_argument('--rebuild', action="store_true", help="Rebuild Reader areas and weights")
    parser.add_argument('--realization', type=str,
                        help='realization to be processed. Use with coherence with --model, --exp and --source')
    parser.add_argument('--stat', type=str,
                        help="statistic to be computed. Can be one of ['mean', 'std', 'max', 'min', 'sum', 'histogram'].")
    parser.add_argument('--frequency', type=str,
                        help="Frequency of the DROP output. Can be anything in the AQUA frequency.")
    parser.add_argument('--resolution', type=str,
                        help="Resolution of the DROP output. Can be anything in the AQUA resolution.")
    parser.add_argument('--startdate', type=str,
                        help="Start date to subset the data. Format YYYY-MM-DD")
    parser.add_argument('--enddate', type=str,
                        help="End date to subset the data. Format YYYY-MM-DD")
    parser.add_argument('--engine', type=str,
                        help="Engine to be used for GSV retrieval: 'polytope' or 'fdb'. Defaults to 'fdb'.")
    parser.add_argument('--driver', type=str, choices=['netcdf', 'zarr', 'icechunk'],
                        help='Output format for DROP files [default: netcdf, or options.driver from config]: '
                             'netcdf, zarr or icechunk '
                             '(icechunk is preliminary and does not support catalog integration).')
    parser.add_argument('--outdir', type=str,
                        help='Output directory. Required when running without a config file.')
    parser.add_argument('--tmpdir', type=str,
                        help='Temporary directory. Required when running without a config file.')
    # fmt: on
    return parser


def _cfg(config, section, key, default=None):
    """Read a value from a nested two-level config dict without raising on missing keys.

    Args:
        config (dict): top-level configuration dictionary
        section (str): top-level key (e.g. 'target', 'options', 'paths')
        key (str): key inside the section
        default: value to return when the section or key is absent. Defaults to None.

    Returns:
        The value at ``config[section][key]``, or ``default`` if absent.
    """
    return config.get(section, {}).get(key, default)


def _validate_drop_args(config, outdir):
    """Validate mandatory arguments when no config file is loaded.

    Args:
        config (dict): configuration dictionary (may be empty when no config file was loaded)
        outdir (str or None): output directory resolved from CLI or config
    """
    errors = []
    if not outdir:
        errors.append("  --outdir   output directory")
    if not config.get("data"):
        errors.append("  --model, --exp, --source, --var   data selection")
    if errors:
        print("ERROR: the following mandatory arguments are missing (required when running without a config file):")
        for e in errors:
            print(e)
        sys.exit(1)


def drop_execute(args):
    """
    Executing the DROP by parsing the arguments and configuring the machinery
    """

    print("AQUA version is: " + version)

    file = get_arg(args, "config", "drop_config.yaml")
    explicit_config = bool(getattr(args, "config", None))

    # Load config - optional unless explicitly requested with --config
    try:
        print("Reading configuration yaml file..")
        config = load_yaml(file)
        config_loaded = True
    except FileNotFoundError:
        if explicit_config:
            raise
        print(f"Configuration file '{file}' not found, running in CLI-only mode.")
        config = {}
        config_loaded = False

    # paths: CLI args take priority, fall back to config file
    outdir = get_arg(args, "outdir", _cfg(config, "paths", "outdir"))
    tmpdir = get_arg(args, "tmpdir", _cfg(config, "paths", "tmpdir")) or outdir

    # main arguments, only from config file
    region = _cfg(config, "target", "region")

    # Command line arguments override config file
    # from target block
    catalog = get_arg(args, "catalog", _cfg(config, "target", "catalog"))
    stat = get_arg(args, "stat", _cfg(config, "target", "stat", "mean"))
    stat_kwargs = _cfg(config, "target", "stat_kwargs", {})
    frequency = get_arg(args, "frequency", _cfg(config, "target", "frequency"))
    resolution = get_arg(args, "resolution", _cfg(config, "target", "resolution"))
    startdate = get_arg(args, "startdate", _cfg(config, "target", "startdate"))
    enddate = get_arg(args, "enddate", _cfg(config, "target", "enddate"))

    # options
    engine = get_arg(args, "engine", _cfg(config, "options", "engine", "fdb"))
    loglevel = get_arg(args, "loglevel", _cfg(config, "options", "loglevel", "WARNING"))
    compact = _cfg(config, "options", "compact", "cdo")
    driver = get_arg(args, "driver", _cfg(config, "options", "driver", "netcdf"))

    # Other options, only from command line
    definitive = get_arg(args, "definitive", False)
    monitoring = get_arg(args, "monitoring", _cfg(config, "options", "performance_reporting", False))
    overwrite = get_arg(args, "overwrite", _cfg(config, "options", "overwrite", False))
    rebuild = get_arg(args, "rebuild", _cfg(config, "options", "rebuild", False))
    exclude_incomplete = _cfg(config, "options", "exclude_incomplete", True)
    no_validate = get_arg(args, "no_validate", False)
    catalog_entry = get_arg(args, "catalog_entry", _cfg(config, "options", "catalog_entry", "yes"))
    if catalog_entry == "only":
        print("--catalog-entry only: skipping data generation, updating catalog entry only.")
    fix = get_arg(args, "fix", True)

    default_workers = get_arg(args, "workers", 1)

    # When no config was loaded, synthesize config["data"] from CLI args
    if "data" not in config:
        model_arg = getattr(args, "model", None)
        exp_arg = getattr(args, "exp", None)
        source_arg = getattr(args, "source", None)
        var_arg = getattr(args, "var", None)
        if model_arg and exp_arg and source_arg and var_arg:
            config["data"] = {model_arg: {exp_arg: {source_arg: {"vars": var_arg}}}}

    # In CLI-only mode validate that all mandatory arguments are present
    if not config_loaded:
        _validate_drop_args(config, outdir)

    drop_cli(
        args=args,
        config=config,
        catalog=catalog,
        resolution=resolution,
        frequency=frequency,
        fix=fix,
        enddate=enddate,
        startdate=startdate,
        outdir=outdir,
        tmpdir=tmpdir,
        loglevel=loglevel,
        region=region,
        stat=stat,
        stat_kwargs=stat_kwargs,
        compact=compact,
        definitive=definitive,
        overwrite=overwrite,
        rebuild=rebuild,
        no_validate=no_validate,
        default_workers=default_workers,
        engine=engine,
        monitoring=monitoring,
        catalog_entry=catalog_entry,
        driver=driver,
        exclude_incomplete=exclude_incomplete,
    )


def drop_cli(
    args,
    config,
    catalog=None,
    resolution=None,
    frequency=None,
    fix=None,
    startdate=None,
    enddate=None,
    outdir=None,
    tmpdir=None,
    loglevel=None,
    region=None,
    stat="mean",
    stat_kwargs={},
    definitive=False,
    overwrite=False,
    rebuild=False,
    no_validate=False,
    monitoring=False,
    engine="fdb",
    default_workers=1,
    driver="netcdf",
    compact="cdo",
    catalog_entry="yes",
    exclude_incomplete=True,
):
    """
    Running the default DROP from CLI, looping on all the configuration model/exp/source/var combination
    Optional feature for each source can be defined as `zoom`, `workers` and `realizations`
    Options for dry run and overwriting, as well as monitoring and zarr creation, are available

    Args:
        args: argparse arguments
        config: configuration dictionary
        catalog: catalog to be processed
        resolution: resolution of the DROP output
        frequency: frequency of the DROP output
        fix: fixer option
        outdir: output directory
        tmpdir: temporary directory
        loglevel: log level
        region: region to be processed
        stat: statistic to be computed
        stat_kwargs: kwargs for the statistic function, used only if the function accepts kwargs (like histogram)
        definitive: bool flag to create definitive files
        overwrite: bool flag to overwrite existing files
        rebuild: bool flag to rebuild the areas and weights
        no_validate: bool flag to skip pre-run integrity check on existing output files
        default_workers: default number of workers
        monitoring: bool flag to enable the dask monitoring
        driver: output format driver
        compact: compaction method
        catalog_entry: catalog entry behaviour ('yes', 'no', 'only')
        exclude_incomplete: bool flag to exclude incomplete temporal chunks when averaging
    """

    models = to_list(get_arg(args, "model", config["data"]))
    for model in models:
        exps = to_list(get_arg(args, "exp", config["data"][model]))
        for exp in exps:
            # if you do require the entire catalog generator
            sources = to_list(get_arg(args, "source", config["data"][model][exp]))
            for source in sources:
                # get info on potential realizations from the configuration file or from the args of command line
                realizations = get_arg(args, "realization", config["data"][model][exp][source].get("realizations"))
                loop_realizations = to_list(realizations) if realizations is not None else [1]

                # get info on varlist and workers
                varnames = to_list(get_arg(args, "var", config["data"][model][exp][source]["vars"]))

                # get the number of workers for this specific configuration
                workers = config["data"][model][exp][source].get("workers", default_workers)

                # per-source overrides: resolution, frequency, stat fall back to global values
                src_resolution = config["data"][model][exp][source].get("resolution", resolution)
                src_frequency = config["data"][model][exp][source].get("frequency", frequency)
                src_stat = config["data"][model][exp][source].get("stat", stat)

                # loop in realizations
                for realization in loop_realizations:
                    # define realization as extra args only if this is found in the configuration file
                    extra_args = {"realization": realization} if realizations else {}
                    for varname in varnames:
                        # get the zoom level - this might need some tuning for extra kwargs
                        zoom = config["data"][model][exp][source].get("zoom", None)
                        if zoom is not None:
                            extra_args = {**extra_args, **{"zoom": zoom}}

                        # disabling rebuild if we are not in the first realization and first varname
                        if varname != varnames[0] or realization != loop_realizations[0]:
                            rebuild = False
                        # init the DROP
                        drop = Drop(
                            catalog=catalog,
                            model=model,
                            exp=exp,
                            source=source,
                            var=varname,
                            resolution=src_resolution,
                            startdate=startdate,
                            enddate=enddate,
                            frequency=src_frequency,
                            fix=fix,
                            outdir=outdir,
                            tmpdir=tmpdir,
                            nproc=workers,
                            loglevel=loglevel,
                            region=region,
                            stat=src_stat,
                            stat_kwargs=stat_kwargs,
                            definitive=definitive,
                            overwrite=overwrite,
                            rebuild=rebuild,
                            compact=compact,
                            performance_reporting=monitoring,
                            exclude_incomplete=exclude_incomplete,
                            output_format=driver,
                            engine=engine,
                            **extra_args,
                        )

                        if catalog_entry != "only":
                            # check that your DROP output is not already there (it will not work in streaming mode)
                            if not no_validate:
                                drop.check_integrity(varname)

                            # retrieve and generate
                            drop.retrieve()
                            drop.drop_generator()

            # create the catalog once the loop is over
            if catalog_entry != "no" and driver != "icechunk":
                drop.create_catalog_entry()
            elif driver == "icechunk":
                print("Skipping catalog entry creation: not supported for icechunk output format.")

    print("CLI DROP run completed. Have yourself a tasty pint of beer!")


# if you want to execute the script from terminal without the aqua entry point
if __name__ == "__main__":
    args = drop_parser().parse_args(sys.argv[1:])
    drop_execute(args)
