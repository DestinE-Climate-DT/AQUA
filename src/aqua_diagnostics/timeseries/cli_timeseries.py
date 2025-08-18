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
from aqua.diagnostics.core import round_startdate, round_enddate
from aqua.diagnostics.timeseries.util_cli import load_var_config
from aqua.diagnostics.timeseries import Timeseries, SeasonalCycles, Gregory
from aqua.diagnostics.timeseries import PlotTimeseries, PlotSeasonalCycles, PlotGregory


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
    config_dict = load_diagnostic_config(diagnostic='timeseries', config=args.config,
                                         default_config='config_timeseries_atm.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    regrid = get_arg(args, 'regrid', None)
    logger.info(f"Regrid option is set to {regrid}")
    realization = get_arg(args, 'realization', None)
    if realization:
        reader_kwargs = {'realization': realization}
    else:
        reader_kwargs = {}

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

            diagnostic_name = config_dict['diagnostics']['timeseries'].get('diagnostic_name', 'timeseries')
            center_time = config_dict['diagnostics']['timeseries'].get('center_time', True)

            for var in config_dict['diagnostics']['timeseries'].get('variables', []):
                var_config, regions = load_var_config(config_dict, var)
                logger.info(f"Running Timeseries diagnostic for variable {var} with config {var_config} in regions {[region if region else 'global' for region in regions]}") # noqa
                for region in regions:
                    try:
                        logger.info(f"Running Timeseries diagnostic in region {region if region else 'global'}")

                        init_args = {'region': region, 'loglevel': loglevel, 'diagnostic_name': diagnostic_name}
                        run_args = {'var': var, 'formula': False, 'long_name': var_config.get('long_name'),
                                    'units': var_config.get('units'), 'standard_name': var_config.get('standard_name'),
                                    'freq': var_config.get('freq'), 'outputdir': outputdir, 'rebuild': rebuild,
                                    'center_time': center_time, 'reader_kwargs': reader_kwargs}

                        # Initialize a list of len from the number of datasets
                        ts = [None] * len(config_dict['datasets'])
                        for i, dataset in enumerate(config_dict['datasets']):
                            logger.info(f'Running dataset: {dataset}, variable: {var}')
                            dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                            'exp': dataset['exp'], 'source': dataset['source'],
                                            'regrid': regrid if regrid is not None else dataset.get('regrid', None)}
                            logger.debug(f"Dataset args: {dataset_args}")
                            ts[i] = Timeseries(**init_args, **dataset_args)
                            ts[i].run(**run_args)

                        # Reference datasets are evaluated on the maximum time range of the datasets
                        startdate = min([ts[i].startdate for i in range(len(ts))])
                        enddate = max([ts[i].enddate for i in range(len(ts))])
                        startdate = round_startdate(startdate)
                        enddate = round_enddate(enddate)
                        logger.info(f"Start date: {startdate}, End date: {enddate}")

                        # Initialize a list of len from the number of references
                        if 'references' in config_dict:
                            run_args.pop('reader_kwargs')  # Remove reader_kwargs from run_args for references
                            ts_ref = [None] * len(config_dict['references'])
                            for i, reference in enumerate(config_dict['references']):
                                logger.info(f'Running reference: {reference}, variable: {var}')
                                reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                                                'exp': reference['exp'], 'source': reference['source'],
                                                'startdate': startdate, 'enddate': enddate,
                                                'std_startdate': var_config.get('std_startdate'),
                                                'std_enddate': var_config.get('std_enddate'),
                                                'regrid': regrid if regrid is not None else reference.get('regrid', None)}
                                logger.warning(f"Reference args: {reference_args}")
                                ts_ref[i] = Timeseries(**init_args, **reference_args)
                                ts_ref[i].run(**run_args, std=True)

                        # Plot the timeseries
                        if save_pdf or save_png:
                            logger.info(f"Plotting Timeseries diagnostic for variable {var} in region {region if region else 'global'}") # noqa
                            plot_args = {'monthly_data': [ts[i].monthly for i in range(len(ts))],
                                        'annual_data': [ts[i].annual for i in range(len(ts))],
                                        'ref_monthly_data': [ts_ref[i].monthly for i in range(len(ts_ref))],
                                        'ref_annual_data': [ts_ref[i].annual for i in range(len(ts_ref))],
                                        'std_monthly_data': [ts_ref[i].std_monthly for i in range(len(ts_ref))],
                                        'std_annual_data': [ts_ref[i].std_annual for i in range(len(ts_ref))],
                                        'loglevel': loglevel}
                            plot_ts = PlotTimeseries(**plot_args)
                            data_label = plot_ts.set_data_labels()
                            ref_label = plot_ts.set_ref_label()
                            description = plot_ts.set_description()
                            title = plot_ts.set_title(var=var, units=var_config.get('units'))
                            fig, _ = plot_ts.plot_timeseries(data_labels=data_label, ref_label=ref_label, title=title)

                            if save_pdf:
                                plot_ts.save_plot(fig, var=var, description=description, region=region, outputdir=outputdir,
                                                dpi=dpi, rebuild=rebuild, format='pdf')
                            if save_png:
                                plot_ts.save_plot(fig, var=var, description=description, region=region, outputdir=outputdir,
                                                dpi=dpi, rebuild=rebuild, format='png')
                    except Exception as e:
                        logger.error(f"Error running Timeseries diagnostic for variable {var} in region {region if region else 'global'}: {e}")

            for var in config_dict['diagnostics']['timeseries'].get('formulae', []):
                var_config, regions = load_var_config(config_dict, var)
                logger.info(f"Running Timeseries diagnostic for variable {var} with config {var_config}")

                diagnostic_name = config_dict['diagnostics']['timeseries'].get('diagnostic_name', 'timeseries')
                center_time = config_dict['diagnostics']['timeseries'].get('center_time', True)

                for region in regions:
                    try:
                        logger.info(f"Running Timeseries diagnostic in region {region if region else 'global'}")

                        init_args = {'region': region, 'loglevel': loglevel, 'diagnostic_name': diagnostic_name}
                        run_args = {'var': var, 'formula': True, 'long_name': var_config.get('long_name'),
                                    'units': var_config.get('units'), 'standard_name': var_config.get('standard_name'),
                                    'freq': var_config.get('freq'), 'outputdir': outputdir, 'rebuild': rebuild,
                                    'center_time': center_time, 'reader_kwargs': reader_kwargs}

                        # Initialize a list of len from the number of datasets
                        ts = [None] * len(config_dict['datasets'])
                        for i, dataset in enumerate(config_dict['datasets']):
                            logger.info(f'Running dataset: {dataset}, variable: {var}')
                            dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                            'exp': dataset['exp'], 'source': dataset['source'],
                                            'regrid': regrid if regrid is not None else dataset.get('regrid', None)}
                            ts[i] = Timeseries(**init_args, **dataset_args)
                            ts[i].run(**run_args)

                        # Reference datasets are evaluated on the maximum time range of the datasets
                        startdate = min([ts[i].plt_startdate for i in range(len(ts))])
                        enddate = max([ts[i].plt_enddate for i in range(len(ts))])

                        # Initialize a list of len from the number of references
                        if 'references' in config_dict:
                            ts_ref = [None] * len(config_dict['references'])
                            run_args.pop('reader_kwargs')  # Remove reader_kwargs from run_args for references
                            for i, reference in enumerate(config_dict['references']):
                                logger.info(f'Running reference: {reference}, variable: {var}')
                                reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                                                'exp': reference['exp'], 'source': reference['source'],
                                                'startdate': startdate, 'enddate': enddate,
                                                'std_startdate': var_config.get('std_startdate'),
                                                'std_enddate': var_config.get('std_enddate'),
                                                'regrid': regrid if regrid is not None else reference.get('regrid', None)}
                                ts_ref[i] = Timeseries(**init_args, **reference_args)
                                ts_ref[i].run(**run_args, std=True)

                        # Plot the timeseries
                        if save_pdf or save_png:
                            logger.info(f"Plotting Timeseries diagnostic for variable {var} in region {region if region else 'global'}") # noqa
                            plot_args = {'monthly_data': [ts[i].monthly for i in range(len(ts))],
                                        'annual_data': [ts[i].annual for i in range(len(ts))],
                                        'ref_monthly_data': [ts_ref[i].monthly for i in range(len(ts_ref))],
                                        'ref_annual_data': [ts_ref[i].annual for i in range(len(ts_ref))],
                                        'std_monthly_data': [ts_ref[i].std_monthly for i in range(len(ts_ref))],
                                        'std_annual_data': [ts_ref[i].std_annual for i in range(len(ts_ref))],
                                        'diagnostic_name': diagnostic_name,
                                        'loglevel': loglevel}
                            plot_ts = PlotTimeseries(**plot_args)
                            data_label = plot_ts.set_data_labels()
                            ref_label = plot_ts.set_ref_label()
                            description = plot_ts.set_description()
                            title = plot_ts.set_title(var=var, units=var_config.get('units'))
                            fig, _ = plot_ts.plot_timeseries(data_labels=data_label, ref_label=ref_label, title=title)

                            if save_pdf:
                                plot_ts.save_plot(fig, var=var, description=description, region=region, outputdir=outputdir,
                                                dpi=dpi, rebuild=rebuild, format='pdf')
                            if save_png:
                                plot_ts.save_plot(fig, var=var, description=description, region=region, outputdir=outputdir,
                                                dpi=dpi, rebuild=rebuild, format='png')
                    except Exception as e:
                        logger.error(f"Error running Timeseries diagnostic for variable {var} in region {region if region else 'global'}: {e}")

    # SeasonalCycles diagnostic
    if 'seasonalcycles' in config_dict['diagnostics']:
        if config_dict['diagnostics']['seasonalcycles']['run']:
            logger.info("SeasonalCycles diagnostic is enabled.")

            diagnostic_name = config_dict['diagnostics']['seasonalcycles'].get('diagnostic_name', 'seasonalcycles')

            for var in config_dict['diagnostics']['seasonalcycles'].get('variables', []):
                try:
                    var_config, regions = load_var_config(config_dict, var, diagnostic='seasonalcycles')
                    logger.info(f"Running SeasonalCycles diagnostic for variable {var} with config {var_config}")

                    for region in regions:
                        logger.info(f"Running SeasonalCycles diagnostic in region {region if region else 'global'}")

                        init_args = {'region': region, 'loglevel': loglevel, 'diagnostic_name': diagnostic_name}
                        run_args = {'var': var, 'formula': False, 'long_name': var_config.get('long_name'),
                                    'units': var_config.get('units'), 'standard_name': var_config.get('standard_name'),
                                    'outputdir': outputdir, 'rebuild': rebuild, 'reader_kwargs': reader_kwargs}

                        # Initialize a list of len from the number of datasets
                        sc = [None] * len(config_dict['datasets'])

                        for i, dataset in enumerate(config_dict['datasets']):
                            logger.info(f'Running dataset: {dataset}, variable: {var}')
                            dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                            'exp': dataset['exp'], 'source': dataset['source'],
                                            'regrid': regrid if regrid is not None else dataset.get('regrid', None)}
                            sc[i] = SeasonalCycles(**init_args, **dataset_args)
                            sc[i].run(**run_args)

                        # Reference datasets are evaluated on the maximum time range of the datasets
                        startdate = min([sc[i].startdate for i in range(len(sc))])
                        enddate = max([sc[i].enddate for i in range(len(sc))])

                        # Initialize a list of len from the number of references
                        if 'references' in config_dict:
                            sc_ref = [None] * len(config_dict['references'])
                            run_args.pop('reader_kwargs')  # Remove reader_kwargs from run_args for references
                            for i, reference in enumerate(config_dict['references']):
                                logger.info(f'Running reference: {reference}, variable: {var}')
                                reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                                                'exp': reference['exp'], 'source': reference['source'],
                                                'startdate': startdate, 'enddate': enddate,
                                                'std_startdate': var_config.get('std_startdate'),
                                                'std_enddate': var_config.get('std_enddate'),
                                                'regrid': regrid if regrid is not None else reference.get('regrid', None)}
                                sc_ref[i] = SeasonalCycles(**init_args, **reference_args)
                                sc_ref[i].run(**run_args, std=True)

                        # Plot the seasonal cycles
                        if save_pdf or save_png:
                            logger.info(f"Plotting SeasonalCycles diagnostic for variable {var} in region {region if region else 'global'}") # noqa
                            plot_args = {'monthly_data': [sc[i].monthly for i in range(len(sc))],
                                        'ref_monthly_data': [sc_ref[i].monthly for i in range(len(sc_ref))],
                                        'std_monthly_data': [sc_ref[i].std_monthly for i in range(len(sc_ref))],
                                        'loglevel': loglevel, 'diagnostic_name': diagnostic_name}
                            plot_sc = PlotSeasonalCycles(**plot_args)
                            data_label = plot_sc.set_data_labels()
                            ref_label = plot_sc.set_ref_label()
                            description = plot_sc.set_description()
                            title = plot_sc.set_title(var=var, units=var_config.get('units'))
                            fig, _ = plot_sc.plot_seasonalcycles(data_labels=data_label, ref_label=ref_label, title=title)

                            if save_pdf:
                                plot_sc.save_plot(fig, var=var, description=description, region=region, outputdir=outputdir,
                                                dpi=dpi, rebuild=rebuild, format='pdf')
                            if save_png:
                                plot_sc.save_plot(fig, var=var, description=description, region=region, outputdir=outputdir,
                                                dpi=dpi, rebuild=rebuild, format='png')
                except Exception as e:
                    logger.error(f"Error running SeasonalCycles diagnostic for variable {var} in region {region if region else 'global'}: {e}")

    if 'gregory' in config_dict['diagnostics']:
        if config_dict['diagnostics']['gregory']['run']:
            logger.info("Gregory diagnostic is enabled.")

            diagnostic_name = config_dict['diagnostics']['gregory'].get('diagnostic_name', 'gregory')

            try:
                init_args = {'loglevel': loglevel, 'diagnostic_name': diagnostic_name}
                freq = []
                if config_dict['diagnostics']['gregory'].get('monthly', False):
                    freq.append('monthly')
                if config_dict['diagnostics']['gregory'].get('annual', False):
                    freq.append('annual')
                run_args = {'freq': freq, 't2m_name': config_dict['diagnostics']['gregory'].get('t2m_name', '2t'),
                            'net_toa_name': config_dict['diagnostics']['gregory'].get('net_toa_name', 'tnlwrf+tnswrf'),
                            'exclude_incomplete': config_dict['diagnostics']['gregory'].get('exclude_incomplete', True),
                            'outputdir': outputdir, 'rebuild': rebuild, 'reader_kwargs': reader_kwargs}

                # Initialize a list of len from the number of datasets
                greg = [None] * len(config_dict['datasets'])
                model_args = {'t2m': True, 'net_toa': True, 'std': False}
                for i, dataset in enumerate(config_dict['datasets']):
                    logger.info(f'Running dataset: {dataset}')
                    dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                    'exp': dataset['exp'], 'source': dataset['source'],
                                    'regrid': regrid if regrid is not None else dataset.get('regrid', None)}

                    greg[i] = Gregory(**init_args, **dataset_args)
                    greg[i].run(**run_args, **model_args)

                if config_dict['diagnostics']['gregory']['std']:
                    run_args.pop('reader_kwargs')  # Remove reader_kwargs from run_args for references
                    # t2m:
                    dataset_args = {**config_dict['diagnostics']['gregory']['t2m_ref'],
                                    'regrid': regrid,
                                    'startdate': config_dict['diagnostics']['gregory'].get('std_startdate'),
                                    'enddate': config_dict['diagnostics']['gregory'].get('std_enddate')}
                    greg_ref_t2m = Gregory(**init_args, **dataset_args)
                    greg_ref_t2m.run(**run_args, t2m=True, net_toa=False, std=True)

                    # net_toa:
                    dataset_args = {**config_dict['diagnostics']['gregory']['net_toa_ref'],
                                    'regrid': regrid,
                                    'startdate': config_dict['diagnostics']['gregory'].get('std_startdate'),
                                    'enddate': config_dict['diagnostics']['gregory'].get('std_enddate')}
                    greg_ref_toa = Gregory(**init_args, **dataset_args)
                    greg_ref_toa.run(**run_args, t2m=False, net_toa=True, std=True)
                
                # Plot the gregory
                if save_pdf or save_png:
                    logger.info(f"Plotting Gregory diagnostic")
                    plot_args = {'t2m_monthly_data': [greg[i].t2m_monthly for i in range(len(greg))],
                                't2m_annual_data': [greg[i].t2m_annual for i in range(len(greg))],
                                'net_toa_monthly_data': [greg[i].net_toa_monthly for i in range(len(greg))],
                                'net_toa_annual_data': [greg[i].net_toa_annual for i in range(len(greg))],
                                't2m_monthly_ref': greg_ref_t2m.t2m_monthly,
                                't2m_annual_ref': greg_ref_t2m.t2m_annual,
                                'net_toa_monthly_ref': greg_ref_toa.net_toa_monthly,
                                'net_toa_annual_ref': greg_ref_toa.net_toa_annual,
                                't2m_annual_std': greg_ref_t2m.t2m_std,
                                'net_toa_annual_std': greg_ref_toa.net_toa_std,
                                'diagnostic_name': diagnostic_name,
                                'loglevel': loglevel}
                    
                    plot_greg = PlotGregory(**plot_args)
                    title = plot_greg.set_title()
                    data_labels = plot_greg.set_data_labels()
                    ref_label = plot_greg.set_ref_label()
                    fig = plot_greg.plot(data_labels=data_labels, ref_label=ref_label, title=title)
                    description = plot_greg.set_description()

                    if save_pdf:
                        plot_greg.save_plot(fig, description=description, outputdir=outputdir,
                                            dpi=dpi, rebuild=rebuild, format='pdf', diagnostic_product='gregory')
                    if save_png:
                        plot_greg.save_plot(fig, description=description, outputdir=outputdir,
                                            dpi=dpi, rebuild=rebuild, format='png', diagnostic_product='gregory')
            except Exception as e:
                logger.error(f"Error running Gregory diagnostic: {e}")

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

    logger.info("Timeseries diagnostic completed.")
