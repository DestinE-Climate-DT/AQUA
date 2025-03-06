#!/usr/bin/env python3
"""
Command-line interface for Timeseries diagnostic.

This CLI allows to run the Timeseries, SeasonalCycles and GregoryPlot
diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""
import argparse
import sys

from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args
from aqua.diagnostics.timeseries.util_cli import load_var_config
from aqua.diagnostics.timeseries import Timeseries, SeasonalCycles, Gregory


def parse_arguments(args):
    """Parse command-line arguments for Timeseries diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='Timeseries CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='Timeseries CLI')
    logger.info(f"Running Timeseries diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster, = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments,
    # overwriting the configuration file values with the command-line arguments.
    config_dict = load_diagnostic_config(diagnostic='timeseries', args=args,
                                         default_config='config_timeseries_atm.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    regrid = get_arg(args, 'regrid', None)

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild = config_dict['output'].get('rebuild', True)
    save_pdf = config_dict['output'].get('save_pdf', True)
    save_png = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300)

    # Timeseries diagnostic
    if 'timeseries' in config_dict['diagnostics']:
        if config_dict['diagnostics']['timeseries']['run']:
            logger.info("Timeseries diagnostic is enabled.")

            for var in config_dict['diagnostics']['timeseries'].get('variables', []):
                var_config, regions = load_var_config(config_dict, var)
                logger.info(f"Running Timeseries diagnostic for variable {var} with config {var_config} in regions {[region if region else 'global' for region in regions]}") # noqa
                for region in regions:
                    logger.info(f"Running Timeseries diagnostic in region {region if region else 'global'}")

                    init_args = {'region': region, 'loglevel': loglevel}
                    run_args = {'var': var, 'formula': False, 'long_name': var_config.get('long_name'),
                                'units': var_config.get('units'), 'standard_name': var_config.get('standard_name'),
                                'freq': var_config.get('freq'), 'outputdir': outputdir, 'rebuild': rebuild}

                    # Initialize a list of len from the number of datasets
                    ts = [None] * len(config_dict['datasets'])
                    for i, dataset in enumerate(config_dict['datasets']):
                        logger.info(f'Running dataset: {dataset}, variable: {var}')
                        dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                        'exp': dataset['exp'], 'source': dataset['source'],
                                        'regrid': dataset.get('regrid', regrid)}
                        ts[i] = Timeseries(**init_args, **dataset_args)
                        ts[i].run(**run_args)

                    # Reference datasets are evaluated on the maximum time range of the datasets
                    startdate = min([ts[i].startdate for i in range(len(ts))])
                    enddate = max([ts[i].enddate for i in range(len(ts))])

                    # Initialize a list of len from the number of references
                    if 'references' in config_dict:
                        ts_ref = [None] * len(config_dict['references'])
                        for i, reference in enumerate(config_dict['references']):
                            logger.info(f'Running reference: {reference}, variable: {var}')
                            reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                                              'exp': reference['exp'], 'source': reference['source'],
                                              'startdate': startdate, 'enddate': enddate,
                                              'std_startdate': reference.get('std_startdate'),
                                              'std_enddate': reference.get('std_enddate'),
                                              'regrid': reference.get('regrid', regrid)}
                            ts_ref[i] = Timeseries(**init_args, **reference_args)
                            ts_ref[i].run(**run_args)

            for var in config_dict['diagnostics']['timeseries'].get('formulae', []):
                var_config, regions = load_var_config(config_dict, var)
                logger.info(f"Running Timeseries diagnostic for variable {var} with config {var_config}")

                for region in regions:
                    logger.info(f"Running Timeseries diagnostic in region {region if region else 'global'}")

                    init_args = {'region': region, 'loglevel': loglevel}
                    run_args = {'var': var, 'formula': True, 'long_name': var_config.get('long_name'),
                                'units': var_config.get('units'), 'standard_name': var_config.get('standard_name'),
                                'freq': var_config.get('freq'), 'outputdir': outputdir, 'rebuild': rebuild}

                    # Initialize a list of len from the number of datasets
                    ts = [None] * len(config_dict['datasets'])
                    for i, dataset in enumerate(config_dict['datasets']):
                        logger.info(f'Running dataset: {dataset}, variable: {var}')
                        dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                        'exp': dataset['exp'], 'source': dataset['source'],
                                        'regrid': dataset.get('regrid', regrid)}
                        ts[i] = Timeseries(**init_args, **dataset_args)
                        ts[i].run(**run_args)

                    # Reference datasets are evaluated on the maximum time range of the datasets
                    startdate = min([ts[i].startdate for i in range(len(ts))])
                    enddate = max([ts[i].enddate for i in range(len(ts))])

                    # Initialize a list of len from the number of references
                    if 'references' in config_dict:
                        ts_ref = [None] * len(config_dict['references'])
                        for i, reference in enumerate(config_dict['references']):
                            logger.info(f'Running reference: {reference}, variable: {var}')
                            reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                                              'exp': reference['exp'], 'source': reference['source'],
                                              'startdate': startdate, 'enddate': enddate,
                                              'std_startdate': reference.get('std_startdate'),
                                              'std_enddate': reference.get('std_enddate'),
                                              'regrid': reference.get('regrid', regrid)}
                            ts_ref[i] = Timeseries(**init_args, **reference_args)
                            ts_ref[i].run(**run_args)

    # SeasonalCycles diagnostic
    if 'seasonalcycles' in config_dict['diagnostics']:
        if config_dict['diagnostics']['seasonalcycles']['run']:
            logger.info("SeasonalCycles diagnostic is enabled.")

            for var in config_dict['diagnostics']['seasonalcycles'].get('variables', []):
                var_config, regions = load_var_config(config_dict, var, diagnostic='seasonalcycles')
                logger.info(f"Running SeasonalCycles diagnostic for variable {var} with config {var_config}")

                for region in regions:
                    logger.info(f"Running SeasonalCycles diagnostic in region {region if region else 'global'}")

                    init_args = {'region': region, 'loglevel': loglevel}
                    run_args = {'var': var, 'formula': False, 'long_name': var_config.get('long_name'),
                                'units': var_config.get('units'), 'standard_name': var_config.get('standard_name'),
                                'outputdir': outputdir, 'rebuild': rebuild}

                    # Initialize a list of len from the number of datasets
                    sc = [None] * len(config_dict['datasets'])

                    for i, dataset in enumerate(config_dict['datasets']):
                        logger.info(f'Running dataset: {dataset}, variable: {var}')
                        dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                        'exp': dataset['exp'], 'source': dataset['source'],
                                        'regrid': dataset.get('regrid', regrid)}
                        sc[i] = SeasonalCycles(**init_args, **dataset_args)
                        sc[i].run(**run_args)

                    # Reference datasets are evaluated on the maximum time range of the datasets
                    startdate = min([sc[i].startdate for i in range(len(sc))])
                    enddate = max([sc[i].enddate for i in range(len(sc))])

                    # Initialize a list of len from the number of references
                    if 'references' in config_dict:
                        sc_ref = [None] * len(config_dict['references'])
                        for i, reference in enumerate(config_dict['references']):
                            logger.info(f'Running reference: {reference}, variable: {var}')
                            reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                                              'exp': reference['exp'], 'source': reference['source'],
                                              'startdate': startdate, 'enddate': enddate,
                                              'std_startdate': reference.get('std_startdate'),
                                              'std_enddate': reference.get('std_enddate'),
                                              'regrid': reference.get('regrid', regrid)}
                            sc_ref[i] = SeasonalCycles(**init_args, **reference_args)
                            sc_ref[i].run(**run_args)

    if 'gregory' in config_dict['diagnostics']:
        if config_dict['diagnostics']['gregory']['run']:
            logger.info("Gregory diagnostic is enabled.")

            freq = []
            if config_dict['diagnostics']['gregory'].get('monthly', False):
                freq.append('monthly')
            if config_dict['diagnostics']['gregory'].get('annual', False):
                freq.append('annual')
            run_args = {'freq': freq, 't2m_name': config_dict['diagnostics']['gregory'].get('t2m_name', '2t'),
                        'net_toa_name': config_dict['diagnostics']['gregory'].get('net_toa_name', 'tnlwrf + tnswrf'),
                        'exclude_incomplete': config_dict['diagnostics']['gregory'].get('exclude_incomplete', True),
                        'outputdir': outputdir, 'rebuild': rebuild}

            # Initialize a list of len from the number of datasets
            greg = [None] * len(config_dict['datasets'])
            model_args = {'t2m': True, 'net_toa': True, 'std': False}
            for i, dataset in enumerate(config_dict['datasets']):
                logger.info(f'Running dataset: {dataset}')
                dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                'exp': dataset['exp'], 'source': dataset['source'],
                                'regrid': dataset.get('regrid', regrid)}

                greg[i] = Gregory(loglevel=loglevel, **dataset_args)
                greg[i].run(**run_args, **model_args)

            if config_dict['diagnostics']['gregory']['std']:
                # t2m:
                dataset_args = {**config_dict['diagnostics']['gregory']['t2m_ref'],
                                'regrid': regrid}
                greg = Gregory(loglevel=loglevel, **dataset_args)
                greg.run(**run_args, t2m=True, net_toa=False, std=True)

                # net_toa:
                dataset_args = {**config_dict['diagnostics']['gregory']['net_toa_ref'],
                                'regrid': regrid}
                greg = Gregory(loglevel=loglevel, **dataset_args)
                greg.run(**run_args, t2m=False, net_toa=True, std=True)

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

    logger.info("Timeseries diagnostic completed.")
