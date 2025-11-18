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

from aqua.diagnostics.core import template_parse_arguments, DiagnosticCLI
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

    # Initialize and prepare CLI
    cli = DiagnosticCLI(
        args=args,
        diagnostic_name='timeseries',
        default_config='config_timeseries_atm.yaml'
    )
    cli.prepare()
    cli.open_dask_cluster()

    # Timeseries diagnostic
    if 'timeseries' in cli.config_dict['diagnostics']:
        if cli.config_dict['diagnostics']['timeseries']['run']:
            cli.logger.info("Timeseries diagnostic is enabled.")

            diagnostic_name = cli.config_dict['diagnostics']['timeseries'].get('diagnostic_name', 'timeseries')
            center_time = cli.config_dict['diagnostics']['timeseries'].get('center_time', True)

            for var in cli.config_dict['diagnostics']['timeseries'].get('variables', []):
                var_config, regions = load_var_config(cli.config_dict, var)
                cli.logger.info(f"Running Timeseries diagnostic for variable {var} with config {var_config} in regions {[region if region else 'global' for region in regions]}") # noqa
                for region in regions:
                    try:
                        cli.logger.info(f"Running Timeseries diagnostic in region {region if region else 'global'}")

                        init_args = {'region': region, 'loglevel': cli.loglevel, 'diagnostic_name': diagnostic_name}
                        run_args = {'var': var, 'formula': False, 'long_name': var_config.get('long_name'),
                                    'units': var_config.get('units'), 'short_name': var_config.get('short_name'),
                                    'freq': var_config.get('freq'), 'outputdir': cli.outputdir, 'rebuild': cli.rebuild,
                                    'center_time': center_time}

                        # Initialize a list of len from the number of datasets
                        ts = [None] * len(cli.config_dict['datasets'])
                        for i, dataset in enumerate(cli.config_dict['datasets']):
                            cli.logger.info(f'Running dataset: {dataset}, variable: {var}')
                            dataset_args = cli.dataset_args(dataset)
                            cli.logger.debug(f"Dataset args: {dataset_args}")
                            ts[i] = Timeseries(**init_args, **dataset_args)
                            ts[i].run(**run_args, create_catalog_entry=cli.create_catalog_entry,
                                      reader_kwargs=dataset.get('reader_kwargs') or cli.reader_kwargs)

                        # Reference datasets are evaluated on the maximum time range of the datasets
                        startdate = round_startdate(min(t.plt_startdate for t in ts))
                        enddate = round_enddate(max(t.plt_enddate for t in ts))
                        cli.logger.info(f"Start date: {startdate}, End date: {enddate}")

                        # Initialize a list of len from the number of references
                        if 'references' in cli.config_dict:
                            ts_ref = [None] * len(cli.config_dict['references'])
                            for i, reference in enumerate(cli.config_dict['references']):
                                cli.logger.info(f'Running reference: {reference}, variable: {var}')
                                reference_args = cli.dataset_args(reference)
                                reference_args.update({
                                    'startdate': startdate,
                                    'enddate': enddate,
                                    'std_startdate': var_config.get('std_startdate'),
                                    'std_enddate': var_config.get('std_enddate')
                                })
                                cli.logger.info(f"Reference args: {reference_args}")
                                ts_ref[i] = Timeseries(**init_args, **reference_args)
                                ts_ref[i].run(**run_args, std=True, create_catalog_entry=False,
                                              reader_kwargs=reference.get('reader_kwargs') or {})

                        # Plot the timeseries
                        if cli.save_pdf or cli.save_png:
                            cli.logger.info(f"Plotting Timeseries diagnostic for variable {var} in region {region if region else 'global'}") # noqa
                            plot_args = {'monthly_data': [t.monthly for t in ts],
                                        'annual_data': [t.annual for t in ts],
                                        'ref_monthly_data': [t.monthly for t in ts_ref],
                                        'ref_annual_data': [t.annual for t in ts_ref],
                                        'std_monthly_data': [t.std_monthly for t in ts_ref],
                                        'std_annual_data': [t.std_annual for t in ts_ref],
                                        'diagnostic_name': diagnostic_name,
                                        'loglevel': cli.loglevel}
                            plot_ts = PlotTimeseries(**plot_args)
                            data_label = plot_ts.set_data_labels()
                            ref_label = plot_ts.set_ref_label()
                            description = plot_ts.set_description()
                            title = plot_ts.set_title()
                            fig, _ = plot_ts.plot_timeseries(data_labels=data_label, ref_label=ref_label, title=title)

                            if cli.save_pdf:
                                plot_ts.save_plot(fig, description=description, outputdir=cli.outputdir,
                                                dpi=cli.dpi, rebuild=cli.rebuild, format='pdf')
                            if cli.save_png:
                                plot_ts.save_plot(fig, description=description, outputdir=cli.outputdir,
                                                dpi=cli.dpi, rebuild=cli.rebuild, format='png')
                    except Exception as e:
                        cli.logger.error(f"Error running Timeseries diagnostic for variable {var} in region {region if region else 'global'}: {e}")

            for var in cli.config_dict['diagnostics']['timeseries'].get('formulae', []):
                var_config, regions = load_var_config(cli.config_dict, var)
                cli.logger.info(f"Running Timeseries diagnostic for variable {var} with config {var_config}")

                diagnostic_name = cli.config_dict['diagnostics']['timeseries'].get('diagnostic_name', 'timeseries')
                center_time = cli.config_dict['diagnostics']['timeseries'].get('center_time', True)

                for region in regions:
                    try:
                        cli.logger.info(f"Running Timeseries diagnostic in region {region if region else 'global'}")

                        init_args = {'region': region, 'loglevel': cli.loglevel, 'diagnostic_name': diagnostic_name}
                        run_args = {'var': var, 'formula': True, 'long_name': var_config.get('long_name'),
                                    'units': var_config.get('units'), 'short_name': var_config.get('short_name'),
                                    'freq': var_config.get('freq'), 'outputdir': cli.outputdir, 'rebuild': cli.rebuild,
                                    'center_time': center_time}

                        # Initialize a list of len from the number of datasets
                        ts = [None] * len(cli.config_dict['datasets'])
                        for i, dataset in enumerate(cli.config_dict['datasets']):
                            cli.logger.info(f'Running dataset: {dataset}, variable: {var}')
                            dataset_args = cli.dataset_args(dataset)
                            ts[i] = Timeseries(**init_args, **dataset_args)
                            ts[i].run(**run_args, create_catalog_entry=cli.create_catalog_entry,
                                      reader_kwargs=dataset.get('reader_kwargs') or cli.reader_kwargs)

                        # Reference datasets are evaluated on the maximum time range of the datasets
                        startdate = min(t.plt_startdate for t in ts)
                        enddate = max(t.plt_enddate for t in ts)

                        # Initialize a list of len from the number of references
                        if 'references' in cli.config_dict:
                            ts_ref = [None] * len(cli.config_dict['references'])
                            for i, reference in enumerate(cli.config_dict['references']):
                                cli.logger.info(f'Running reference: {reference}, variable: {var}')
                                reference_args = cli.dataset_args(reference)
                                reference_args.update({
                                    'startdate': startdate,
                                    'enddate': enddate,
                                    'std_startdate': var_config.get('std_startdate'),
                                    'std_enddate': var_config.get('std_enddate')
                                })
                                ts_ref[i] = Timeseries(**init_args, **reference_args)
                                ts_ref[i].run(**run_args, std=True, create_catalog_entry=False,
                                              reader_kwargs=reference.get('reader_kwargs') or {})

                        # Plot the timeseries
                        if cli.save_pdf or cli.save_png:
                            cli.logger.info(f"Plotting Timeseries diagnostic for variable {var} in region {region if region else 'global'}") # noqa
                            plot_args = {'monthly_data': [t.monthly for t in ts],
                                        'annual_data': [t.annual for t in ts],
                                        'ref_monthly_data': [t.monthly for t in ts_ref],
                                        'ref_annual_data': [t.annual for t in ts_ref],
                                        'std_monthly_data': [t.std_monthly for t in ts_ref],
                                        'std_annual_data': [t.std_annual for t in ts_ref],
                                        'diagnostic_name': diagnostic_name,
                                        'loglevel': cli.loglevel}
                            plot_ts = PlotTimeseries(**plot_args)
                            data_label = plot_ts.set_data_labels()
                            ref_label = plot_ts.set_ref_label()
                            description = plot_ts.set_description()
                            title = plot_ts.set_title()
                            fig, _ = plot_ts.plot_timeseries(data_labels=data_label, ref_label=ref_label, title=title)

                            if cli.save_pdf:
                                plot_ts.save_plot(fig, description=description, outputdir=cli.outputdir,
                                                dpi=cli.dpi, rebuild=cli.rebuild, format='pdf')
                            if cli.save_png:
                                plot_ts.save_plot(fig, description=description, outputdir=cli.outputdir,
                                                dpi=cli.dpi, rebuild=cli.rebuild, format='png')
                    except Exception as e:
                        cli.logger.error(f"Error running Timeseries diagnostic for variable {var} in region {region if region else 'global'}: {e}")

    # SeasonalCycles diagnostic
    if 'seasonalcycles' in cli.config_dict['diagnostics']:
        if cli.config_dict['diagnostics']['seasonalcycles']['run']:
            cli.logger.info("SeasonalCycles diagnostic is enabled.")

            diagnostic_name = cli.config_dict['diagnostics']['seasonalcycles'].get('diagnostic_name', 'seasonalcycles')
            center_time = cli.config_dict['diagnostics']['seasonalcycles'].get('center_time', True)

            for var in cli.config_dict['diagnostics']['seasonalcycles'].get('variables', []):
                try:
                    var_config, regions = load_var_config(cli.config_dict, var, diagnostic='seasonalcycles')
                    cli.logger.info(f"Running SeasonalCycles diagnostic for variable {var} with config {var_config}")

                    for region in regions:
                        cli.logger.info(f"Running SeasonalCycles diagnostic in region {region if region else 'global'}")

                        init_args = {'region': region, 'loglevel': cli.loglevel, 'diagnostic_name': diagnostic_name}
                        run_args = {'var': var, 'formula': False, 'long_name': var_config.get('long_name'),
                                    'units': var_config.get('units'), 'short_name': var_config.get('short_name'),
                                    'outputdir': cli.outputdir, 'rebuild': cli.rebuild, 'center_time': center_time}

                        # Initialize a list of len from the number of datasets
                        sc = [None] * len(cli.config_dict['datasets'])

                        for i, dataset in enumerate(cli.config_dict['datasets']):
                            cli.logger.info(f'Running dataset: {dataset}, variable: {var}')
                            dataset_args = cli.dataset_args(dataset)
                            sc[i] = SeasonalCycles(**init_args, **dataset_args)
                            sc[i].run(**run_args, create_catalog_entry=cli.create_catalog_entry,
                                      reader_kwargs=dataset.get('reader_kwargs') or cli.reader_kwargs)

                        # Reference datasets are evaluated on the maximum time range of the datasets
                        startdate = min(t.plt_startdate for t in ts)
                        enddate = max(t.plt_enddate for t in ts)

                        # Initialize a list of len from the number of references
                        if 'references' in cli.config_dict:
                            sc_ref = [None] * len(cli.config_dict['references'])
                            for i, reference in enumerate(cli.config_dict['references']):
                                cli.logger.info(f'Running reference: {reference}, variable: {var}')
                                reference_args = cli.dataset_args(reference)
                                reference_args.update({
                                    'startdate': startdate,
                                    'enddate': enddate,
                                    'std_startdate': var_config.get('std_startdate'),
                                    'std_enddate': var_config.get('std_enddate')
                                })
                                sc_ref[i] = SeasonalCycles(**init_args, **reference_args)
                                sc_ref[i].run(**run_args, std=True, create_catalog_entry=False,
                                              reader_kwargs=reference.get('reader_kwargs') or {})

                        # Plot the seasonal cycles
                        if cli.save_pdf or cli.save_png:
                            cli.logger.info(f"Plotting SeasonalCycles diagnostic for variable {var} in region {region if region else 'global'}") # noqa
                            plot_args = {'monthly_data': [sc[i].monthly for i in range(len(sc))],
                                        'ref_monthly_data': [sc_ref[i].monthly for i in range(len(sc_ref))],
                                        'std_monthly_data': [sc_ref[i].std_monthly for i in range(len(sc_ref))],
                                        'loglevel': cli.loglevel, 'diagnostic_name': diagnostic_name}
                            plot_sc = PlotSeasonalCycles(**plot_args)
                            data_label = plot_sc.set_data_labels()
                            ref_label = plot_sc.set_ref_label()
                            description = plot_sc.set_description()
                            title = plot_sc.set_title()
                            fig, _ = plot_sc.plot_seasonalcycles(data_labels=data_label, ref_label=ref_label, title=title)

                            if cli.save_pdf:
                                plot_sc.save_plot(fig, description=description, outputdir=cli.outputdir,
                                                dpi=cli.dpi, rebuild=cli.rebuild, format='pdf')
                            if cli.save_png:
                                plot_sc.save_plot(fig, description=description, outputdir=cli.outputdir,
                                                dpi=cli.dpi, rebuild=cli.rebuild, format='png')
                except Exception as e:
                    cli.logger.error(f"Error running SeasonalCycles diagnostic for variable {var} in region {region if region else 'global'}: {e}")

    if 'gregory' in cli.config_dict['diagnostics']:
        if cli.config_dict['diagnostics']['gregory']['run']:
            cli.logger.info("Gregory diagnostic is enabled.")

            diagnostic_name = cli.config_dict['diagnostics']['gregory'].get('diagnostic_name', 'gregory')

            try:
                init_args = {'loglevel': cli.loglevel, 'diagnostic_name': diagnostic_name}
                freq = []
                if cli.config_dict['diagnostics']['gregory'].get('monthly', False):
                    freq.append('monthly')
                if cli.config_dict['diagnostics']['gregory'].get('annual', False):
                    freq.append('annual')
                run_args = {'freq': freq, 't2m_name': cli.config_dict['diagnostics']['gregory'].get('t2m_name', '2t'),
                            'net_toa_name': cli.config_dict['diagnostics']['gregory'].get('net_toa_name', 'tnlwrf+tnswrf'),
                            'exclude_incomplete': cli.config_dict['diagnostics']['gregory'].get('exclude_incomplete', True),
                            'outputdir': cli.outputdir, 'rebuild': cli.rebuild}

                # Initialize a list of len from the number of datasets
                greg = [None] * len(cli.config_dict['datasets'])
                model_args = {'t2m': True, 'net_toa': True, 'std': False}
                for i, dataset in enumerate(cli.config_dict['datasets']):
                    cli.logger.info(f'Running dataset: {dataset}')
                    dataset_args = cli.dataset_args(dataset)

                    greg[i] = Gregory(**init_args, **dataset_args)
                    greg[i].run(**run_args, **model_args, reader_kwargs=dataset.get('reader_kwargs') or cli.reader_kwargs)

                if cli.config_dict['diagnostics']['gregory']['std']:
                    # t2m:
                    dataset_args = {**cli.config_dict['diagnostics']['gregory']['t2m_ref'],
                                    'regrid': cli.regrid,
                                    'startdate': cli.config_dict['diagnostics']['gregory'].get('std_startdate'),
                                    'enddate': cli.config_dict['diagnostics']['gregory'].get('std_enddate')}
                    greg_ref_t2m = Gregory(**init_args, **dataset_args)
                    greg_ref_t2m.run(**run_args, t2m=True, net_toa=False, std=True)

                    # net_toa:
                    dataset_args = {**cli.config_dict['diagnostics']['gregory']['net_toa_ref'],
                                    'regrid': cli.regrid,
                                    'startdate': cli.config_dict['diagnostics']['gregory'].get('std_startdate'),
                                    'enddate': cli.config_dict['diagnostics']['gregory'].get('std_enddate')}
                    greg_ref_toa = Gregory(**init_args, **dataset_args)
                    greg_ref_toa.run(**run_args, t2m=False, net_toa=True, std=True)
                
                # Plot the gregory
                if cli.save_pdf or cli.save_png:
                    cli.logger.info("Plotting Gregory diagnostic")
                    plot_args = {'t2m_monthly_data': [t.t2m_monthly for t in greg],
                                't2m_annual_data': [t.t2m_annual for t in greg],
                                'net_toa_monthly_data': [t.net_toa_monthly for t in greg],
                                'net_toa_annual_data': [t.net_toa_annual for t in greg],
                                't2m_monthly_ref': greg_ref_t2m.t2m_monthly,
                                't2m_annual_ref': greg_ref_t2m.t2m_annual,
                                'net_toa_monthly_ref': greg_ref_toa.net_toa_monthly,
                                'net_toa_annual_ref': greg_ref_toa.net_toa_annual,
                                't2m_annual_std': greg_ref_t2m.t2m_std,
                                'net_toa_annual_std': greg_ref_toa.net_toa_std,
                                'diagnostic_name': diagnostic_name,
                                'loglevel': cli.loglevel}
                    
                    plot_greg = PlotGregory(**plot_args)
                    title = plot_greg.set_title()
                    data_labels = plot_greg.set_data_labels()
                    ref_label = plot_greg.set_ref_label()
                    fig = plot_greg.plot(data_labels=data_labels, ref_label=ref_label, title=title)
                    description = plot_greg.set_description()

                    if cli.save_pdf:
                        plot_greg.save_plot(fig, description=description, outputdir=cli.outputdir,
                                            dpi=cli.dpi, rebuild=cli.rebuild, format='pdf', diagnostic_product='gregory')
                    if cli.save_png:
                        plot_greg.save_plot(fig, description=description, outputdir=cli.outputdir,
                                            dpi=cli.dpi, rebuild=cli.rebuild, format='png', diagnostic_product='gregory')
            except Exception as e:
                cli.logger.error(f"Error running Gregory diagnostic: {e}")

    cli.close_dask_cluster()
