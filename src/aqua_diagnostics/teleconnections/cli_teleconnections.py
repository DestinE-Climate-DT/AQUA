#!/usr/bin/env python3
"""
Command-line interface for Teleconnections diagnostic.

This CLI allows to run the NAO and ENSO diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""
import argparse
import sys

from aqua.diagnostics.core import template_parse_arguments, DiagnosticCLI
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

    cli = DiagnosticCLI(
        args,
        diagnostic_name='teleconnections',
        default_config='config_teleconnections.yaml',
        log_name='Teleconnections CLI',
    ).prepare()
    cli.open_dask_cluster()

    logger = cli.logger
    config_dict = cli.config_dict

    if 'teleconnections' in config_dict['diagnostics']:
        # NAO
        if 'NAO' in config_dict['diagnostics']['teleconnections']:
            if config_dict['diagnostics']['teleconnections']['NAO']['run']:
                logger.info('Running NAO teleconnections diagnostic')

                nao = [None] * len(config_dict['datasets'])

                nao_config = config_dict['diagnostics']['teleconnections']['NAO']
                seasons = nao_config.get('seasons', 'annual')

                nao_regressions = {season: [None] * len(config_dict['datasets']) for season in seasons}
                nao_correlations = {season: [None] * len(config_dict['datasets']) for season in seasons}

                init_args = {'loglevel': cli.loglevel}

                for i, dataset in enumerate(config_dict['datasets']):
                    dataset_args = cli.dataset_args(dataset)
                    logger.info(f'Running dataset: {dataset_args}')

                    nao[i] = NAO(**dataset_args, **init_args)
                    nao[i].retrieve(reader_kwargs=cli.reader_kwargs)
                    nao[i].compute_index(months_window=nao_config.get('months_window', 3), rebuild=cli.rebuild)

                    nao[i].save_netcdf(nao[i].index, diagnostic='nao', diagnostic_product='index',
                                       outputdir=cli.outputdir, rebuild=cli.rebuild)

                    for season in seasons:
                        nao_regressions[season][i] = nao[i].compute_regression(season=season)
                        nao_correlations[season][i] = nao[i].compute_correlation(season=season)

                        diagnostic_product_reg = f'regression_{season}' if season != 'annual' else 'regression'
                        diagnostic_product_cor = f'correlation_{season}' if season != 'annual' else 'correlation'

                        nao[i].save_netcdf(nao_regressions[season][i], diagnostic='nao', diagnostic_product=diagnostic_product_reg,
                                           outputdir=cli.outputdir, rebuild=cli.rebuild)
                        nao[i].save_netcdf(nao_correlations[season][i], diagnostic='nao', diagnostic_product=diagnostic_product_cor,
                                           outputdir=cli.outputdir, rebuild=cli.rebuild)

                nao_ref = [None] * len(config_dict['references'])

                nao_ref_regressions = {season: [None] * len(config_dict['references']) for season in seasons}
                nao_ref_correlations = {season: [None] * len(config_dict['references']) for season in seasons}

                for i, reference in enumerate(config_dict['references']):
                    reference_args = cli.dataset_args(reference)
                    logger.info(f'Running reference: {reference_args}')
                    nao_ref[i] = NAO(**reference_args, **init_args)
                    nao_ref[i].retrieve()
                    nao_ref[i].compute_index(months_window=nao_config.get('months_window', 3), rebuild=cli.rebuild)

                    nao_ref[i].save_netcdf(nao_ref[i].index, diagnostic='nao', diagnostic_product='index',
                                           outputdir=cli.outputdir, rebuild=cli.rebuild)

                    for season in seasons:
                        nao_ref_regressions[season][i] = nao_ref[i].compute_regression(season=season)
                        nao_ref_correlations[season][i] = nao_ref[i].compute_correlation(season=season)

                        diagnostic_product_reg = f'regression_{season}' if season != 'annual' else 'regression'
                        diagnostic_product_cor = f'correlation_{season}' if season != 'annual' else 'correlation'

                        nao_ref[i].save_netcdf(nao_ref_regressions[season][i], diagnostic='nao', diagnostic_product=diagnostic_product_reg,
                                               outputdir=cli.outputdir, rebuild=cli.rebuild)
                        nao_ref[i].save_netcdf(nao_ref_correlations[season][i], diagnostic='nao', diagnostic_product=diagnostic_product_cor,
                                               outputdir=cli.outputdir, rebuild=cli.rebuild)

                # Plot NAO regressions
                if cli.save_pdf or cli.save_png:
                    logger.info('Plotting NAO')
                    plot_args = {
                        'indexes': [nao[i].index for i in range(len(nao))],
                        'ref_indexes': [nao_ref[i].index for i in range(len(nao_ref))],
                        'outputdir': cli.outputdir,
                        'rebuild': cli.rebuild,
                        'loglevel': cli.loglevel,
                    }

                    plot_nao = PlotNAO(**plot_args)

                    # Plot the NAO index
                    fig_index, _ = plot_nao.plot_index()
                    index_description = plot_nao.set_index_description()
                    if cli.save_pdf:
                        plot_nao.save_plot(fig_index, diagnostic_product='index', format='pdf',
                                           metadata={'description': index_description}, dpi=cli.dpi)
                    if cli.save_png:
                        plot_nao.save_plot(fig_index, diagnostic_product='index', format='png',
                                           metadata={'description': index_description}, dpi=cli.dpi)

                    # Plot regressions and correlations
                    for season in seasons:
                        for i in range(len(nao)):
                            nao_regressions[season][i].load(keep_attrs=True)
                            nao_ref_regressions[season][i].load(keep_attrs=True)
                            nao_correlations[season][i].load(keep_attrs=True)
                            nao_ref_correlations[season][i].load(keep_attrs=True)

                        fig_reg = plot_nao.plot_maps(maps=nao_regressions[season], ref_maps=nao_ref_regressions[season],
                                                     statistic='regression')
                        fig_cor = plot_nao.plot_maps(maps=nao_correlations[season], ref_maps=nao_ref_correlations[season],
                                                     statistic='correlation')

                        regression_description = plot_nao.set_map_description(maps=nao_regressions[season],
                                                                             ref_maps=nao_ref_regressions[season],
                                                                             statistic='regression')
                        correlation_description = plot_nao.set_map_description(maps=nao_correlations[season],
                                                                             ref_maps=nao_ref_correlations[season],
                                                                             statistic='correlation')

                        reg_product = f'regression_{season}' if season != 'annual' else 'regression'
                        cor_product = f'correlation_{season}' if season != 'annual' else 'correlation'

                        if cli.save_pdf:
                            plot_nao.save_plot(fig_reg, diagnostic_product=reg_product, format='pdf',
                                               metadata={'description': regression_description})
                            plot_nao.save_plot(fig_cor, diagnostic_product=cor_product, format='pdf',
                                               metadata={'description': correlation_description})
                        if cli.save_png:
                            plot_nao.save_plot(fig_reg, diagnostic_product=reg_product, format='png',
                                               metadata={'description': regression_description}, dpi=cli.dpi)
                            plot_nao.save_plot(fig_cor, diagnostic_product=cor_product, format='png',
                                               metadata={'description': correlation_description}, dpi=cli.dpi)

        # ENSO
        if 'ENSO' in config_dict['diagnostics']['teleconnections']:
            if config_dict['diagnostics']['teleconnections']['ENSO']['run']:
                logger.info('Running ENSO teleconnections diagnostic')

                enso = [None] * len(config_dict['datasets'])

                enso_config = config_dict['diagnostics']['teleconnections']['ENSO']
                seasons = enso_config.get('seasons', 'annual')

                enso_regressions = {season: [None] * len(config_dict['datasets']) for season in seasons}
                enso_correlations = {season: [None] * len(config_dict['datasets']) for season in seasons}

                init_args = {'loglevel': cli.loglevel}

                for i, dataset in enumerate(config_dict['datasets']):
                    dataset_args = cli.dataset_args(dataset)
                    logger.info(f'Running dataset: {dataset_args}')

                    enso[i] = ENSO(**dataset_args, **init_args)
                    enso[i].retrieve(reader_kwargs=cli.reader_kwargs)
                    enso[i].compute_index(months_window=enso_config.get('months_window', 3), rebuild=cli.rebuild)
                    enso[i].save_netcdf(enso[i].index, diagnostic='enso', diagnostic_product='index',
                                       outputdir=cli.outputdir, rebuild=cli.rebuild)

                    for season in seasons:
                        enso_regressions[season][i] = enso[i].compute_regression(season=season)
                        enso_correlations[season][i] = enso[i].compute_correlation(season=season)

                        diagnostic_product_reg = f'regression_{season}' if season != 'annual' else 'regression'
                        diagnostic_product_cor = f'correlation_{season}' if season != 'annual' else 'correlation'

                        enso[i].save_netcdf(enso_regressions[season][i], diagnostic='enso', diagnostic_product=diagnostic_product_reg,
                                            outputdir=cli.outputdir, rebuild=cli.rebuild)
                        enso[i].save_netcdf(enso_correlations[season][i], diagnostic='enso', diagnostic_product=diagnostic_product_cor,
                                            outputdir=cli.outputdir, rebuild=cli.rebuild)

                enso_ref = [None] * len(config_dict['references'])

                enso_ref_regressions = {season: [None] * len(config_dict['references']) for season in seasons}
                enso_ref_correlations = {season: [None] * len(config_dict['references']) for season in seasons}

                for i, reference in enumerate(config_dict['references']):
                    reference_args = cli.dataset_args(reference)
                    logger.info(f'Running reference: {reference_args}')

                    enso_ref[i] = ENSO(**reference_args, **init_args)
                    enso_ref[i].retrieve()
                    enso_ref[i].compute_index(months_window=enso_config.get('months_window', 3), rebuild=cli.rebuild)

                    enso_ref[i].save_netcdf(enso_ref[i].index, diagnostic='enso', diagnostic_product='index',
                                            outputdir=cli.outputdir, rebuild=cli.rebuild)

                    for season in seasons:
                        enso_ref_regressions[season][i] = enso_ref[i].compute_regression(season=season)
                        enso_ref_correlations[season][i] = enso_ref[i].compute_correlation(season=season)

                        diagnostic_product_reg = f'regression_{season}' if season != 'annual' else 'regression'
                        diagnostic_product_cor = f'correlation_{season}' if season != 'annual' else 'correlation'

                        enso_ref[i].save_netcdf(enso_ref_regressions[season][i], diagnostic='enso', diagnostic_product=diagnostic_product_reg,
                                                outputdir=cli.outputdir, rebuild=cli.rebuild)
                        enso_ref[i].save_netcdf(enso_ref_correlations[season][i], diagnostic='enso', diagnostic_product=diagnostic_product_cor,
                                                outputdir=cli.outputdir, rebuild=cli.rebuild)

                # Plot ENSO regressions
                if cli.save_pdf or cli.save_png:
                    logger.info('Plotting ENSO')
                    plot_args = {
                        'indexes': [enso[i].index for i in range(len(enso))],
                        'ref_indexes': [enso_ref[i].index for i in range(len(enso_ref))],
                        'outputdir': cli.outputdir,
                        'rebuild': cli.rebuild,
                        'loglevel': cli.loglevel,
                    }

                    plot_enso = PlotENSO(**plot_args)

                    # Plot the ENSO index
                    fig_index, _ = plot_enso.plot_index()
                    index_description = plot_enso.set_index_description()
                    if cli.save_pdf:
                        plot_enso.save_plot(fig_index, diagnostic_product='index', format='pdf',
                                            metadata={'description': index_description})
                    if cli.save_png:
                        plot_enso.save_plot(fig_index, diagnostic_product='index', format='png',
                                            metadata={'description': index_description}, dpi=cli.dpi)

                    # Plot regressions and correlations
                    for season in seasons:
                        for i in range(len(enso)):
                            enso_regressions[season][i].load(keep_attrs=True)
                            enso_ref_regressions[season][i].load(keep_attrs=True)
                            enso_correlations[season][i].load(keep_attrs=True)
                            enso_ref_correlations[season][i].load(keep_attrs=True)

                        fig_reg = plot_enso.plot_maps(maps=enso_regressions[season], ref_maps=enso_ref_regressions[season],
                                                      statistic='regression')
                        fig_cor = plot_enso.plot_maps(maps=enso_correlations[season], ref_maps=enso_ref_correlations[season],
                                                      statistic='correlation')

                        regression_description = plot_enso.set_map_description(maps=enso_regressions[season],
                                                                             ref_maps=enso_ref_regressions[season],
                                                                             statistic='regression')
                        correlation_description = plot_enso.set_map_description(maps=enso_correlations[season],
                                                                             ref_maps=enso_ref_correlations[season],
                                                                             statistic='correlation')

                        reg_product = f'regression_{season}' if season != 'annual' else 'regression'
                        cor_product = f'correlation_{season}' if season != 'annual' else 'correlation'

                        if cli.save_pdf:
                            plot_enso.save_plot(fig_reg, diagnostic_product=reg_product, format='pdf',
                                               metadata={'description': regression_description})
                            plot_enso.save_plot(fig_cor, diagnostic_product=cor_product, format='pdf',
                                               metadata={'description': correlation_description})
                        if cli.save_png:
                            plot_enso.save_plot(fig_reg, diagnostic_product=reg_product, format='png',
                                               metadata={'description': regression_description}, dpi=cli.dpi)
                            plot_enso.save_plot(fig_cor, diagnostic_product=cor_product, format='png',
                                               metadata={'description': correlation_description}, dpi=cli.dpi)

    cli.close_dask_cluster()

    logger.info('Teleconnections diagnostic completed.')
