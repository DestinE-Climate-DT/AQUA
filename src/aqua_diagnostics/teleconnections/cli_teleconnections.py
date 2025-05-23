#!/usr/bin/env python3
'''
Command-line interface for Teleconnections diagnostic.

This CLI allows to run the NAO and ENSO diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
'''
import argparse
import sys

from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args
from aqua.diagnostics.teleconnections import NAO, ENSO
from aqua.diagnostics.teleconnections import PlotNAO, PlotENSO


def parse_arguments(args):
    """Parse command-line arguments for Teleconnections diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='Teleconnections CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='Teleconnections CLI')
    logger.info(f"Running Teleconnections diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster, = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments,
    # overwriting the configuration file values with the command-line arguments.
    config_dict = load_diagnostic_config(diagnostic='teleconnections', args=args,
                                         default_config='config_teleconnections.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    regrid = get_arg(args, 'regrid', None)

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild = config_dict['output'].get('rebuild', True)
    save_pdf = config_dict['output'].get('save_pdf', True)
    save_png = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300)

    if 'teleconnections' in config_dict['diagnostics']:
        if 'NAO' in config_dict['diagnostics']['teleconnections']:
            if config_dict['diagnostics']['teleconnections']['NAO']['run']:
                logger.info('Running NAO teleconnections diagnostic')

                nao = [None] * len(config_dict['datasets'])

                nao_config = config_dict['diagnostics']['teleconnections']['NAO']
                seasons = nao_config.get('seasons', 'annual')

                # Initialize a matrix to store the NAO regressions
                # for each dataset and each season
                nao_regressions = {season: [None] * len(config_dict['datasets']) for season in seasons}
                nao_correlations = {season: [None] * len(config_dict['datasets']) for season in seasons}

                init_args = {'loglevel': loglevel}

                for i, dataset in enumerate(config_dict['datasets']):
                    logger.info(f'Running dataset: {dataset}')
                    dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                                    'exp': dataset['exp'], 'source': dataset['source'],
                                    'regrid': dataset.get('regrid', regrid)}
                    nao[i] = NAO(**dataset_args, **init_args)
                    nao[i].retrieve()
                    nao[i].compute_index(months_window=nao_config.get('months_window', 3),
                                         rebuild=rebuild)

                    nao[i].save_netcdf(nao[i].index, diagnostic='nao', diagnostic_product='index',
                                       default_path=outputdir, rebuild=rebuild)
                    
                    for season in seasons:
                        nao_regressions[season][i] = nao[i].compute_regression(season=season)
                        nao_correlations[season][i] = nao[i].compute_correlation(season=season)

                        diagnostic_product_reg = f'regression_{season}' if season != 'annual' else 'regression'
                        diagnostic_product_cor = f'correlation_{season}' if season != 'annual' else 'correlation'

                        nao[i].save_netcdf(nao_regressions[season][i], diagnostic='nao', diagnostic_product=diagnostic_product_reg,
                                           default_path=outputdir, rebuild=rebuild)
                        nao[i].save_netcdf(nao_correlations[season][i], diagnostic='nao', diagnostic_product=diagnostic_product_cor,
                                           default_path=outputdir, rebuild=rebuild)

                nao_ref = [None] * len(config_dict['references'])

                nao_ref_regressions = {season: [None] * len(config_dict['references']) for season in seasons}
                nao_ref_correlations = {season: [None] * len(config_dict['references']) for season in seasons}

                for i, reference in enumerate(config_dict['references']):
                    logger.info(f'Running reference: {reference}')
                    reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                                      'exp': reference['exp'], 'source': reference['source'],
                                      'regrid': reference.get('regrid', regrid)}
                    nao_ref[i] = NAO(**reference_args, **init_args)
                    nao_ref[i].retrieve()
                    nao_ref[i].compute_index(months_window=nao_config.get('months_window', 3),
                                             rebuild=rebuild)
                    
                    nao_ref[i].save_netcdf(nao_ref[i].index, diagnostic='nao', diagnostic_product='index',
                                           default_path=outputdir, rebuild=rebuild)
                    
                    for season in seasons:
                        nao_ref_regressions[season][i] = nao_ref[i].compute_regression(season=season)
                        nao_ref_correlations[season][i] = nao_ref[i].compute_correlation(season=season)

                        diagnostic_product_reg = f'regression_{season}' if season != 'annual' else 'regression'
                        diagnostic_product_cor = f'correlation_{season}' if season != 'annual' else 'correlation'

                        nao_ref[i].save_netcdf(nao_ref_regressions[season][i], diagnostic='nao', diagnostic_product=diagnostic_product_reg,
                                               default_path=outputdir, rebuild=rebuild)
                        nao_ref[i].save_netcdf(nao_ref_correlations[season][i], diagnostic='nao', diagnostic_product=diagnostic_product_cor,
                                               default_path=outputdir, rebuild=rebuild)

                # Plot NAO regressions
                if save_pdf or save_png:
                    logger.info('Plotting NAO regressions')
                    plot_args = {'indexes': [nao[i].index for i in range(len(nao))],
                                 'ref_indexes': [nao_ref[i].index for i in range(len(nao_ref))],
                                 'outputdir': outputdir, 'rebuild': rebuild,
                                 'loglevel': loglevel}
                    
                    plot_nao = PlotNAO(**plot_args)

                    # Plot the NAO index
                    fig_index, _ = plot_nao.plot_index()
                    index_description = plot_nao.set_index_description()
                    if save_pdf:
                        plot_nao.save_plot(fig_index, diagnostic_product='index', format='pdf',
                                           metadata={'description': index_description}, dpi=dpi)
                    if save_png:
                        plot_nao.save_plot(fig_index, diagnostic_product='index', format='png',
                                           metadata={'description': index_description}, dpi=dpi)

                    # Plot regressions and correlations
                    for season in seasons:
                        nao_regressions[season].load()
                        nao_ref_regressions[season].load()
                        fig_reg = plot_nao.plot_maps(maps=nao_regressions[season], ref_maps=nao_ref_regressions[season],
                                                        statistic='regression')
                        fig_cor = plot_nao.plot_maps(maps=nao_correlations[season], ref_maps=nao_ref_correlations[season],
                                                        statistic='correlation')

                        regression_description = plot_nao.set_map_description(maps=nao_regressions[season],
                                                                             ref_maps=nao_ref_regressions[season],
                                                                             statistic='regression')
                        correlation_description = plot_nao.set_map_description(maps=nao_regressions[season],
                                                                             ref_maps=nao_ref_regressions[season],
                                                                             statistic='correlation')

                        reg_product = f'regression_{season}' if season != 'annual' else 'regression'
                        cor_product = f'correlation_{season}' if season != 'annual' else 'correlation'

                        if save_pdf:
                            plot_nao.save_plot(fig_reg, diagnostic_product=reg_product, format='pdf',
                                               metadata={'description': regression_description}, dpi=dpi)
                            plot_nao.save_plot(fig_cor, diagnostic_product=cor_product, format='pdf',
                                               metadata={'description': correlation_description}, dpi=dpi)
                        if save_png:
                            plot_nao.save_plot(fig_reg, diagnostic_product=reg_product, format='png',
                                               metadata={'description': regression_description}, dpi=dpi)
                            plot_nao.save_plot(fig_cor, diagnostic_product=cor_product, format='png',
                                               metadata={'description': correlation_description}, dpi=dpi)

        if 'ENSO' in config_dict['diagnostics']['teleconnections']:
            if config_dict['diagnostics']['teleconnections']['ENSO']['run']:
                logger.info('Running ENSO teleconnections diagnostic')

                enso = [None] * len(config_dict['datasets'])

                for i, dataset in enumerate(config_dict['datasets']):
                    logger.info(f'Running dataset: {dataset}')

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

    logger.info('Teleconnections diagnostic finished.')
