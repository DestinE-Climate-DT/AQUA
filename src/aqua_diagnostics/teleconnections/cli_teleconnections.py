#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA teleconnections command line interface for a single dataset.
Reads configuration file and performs teleconnections diagnostic.
'''
import argparse
import os
import sys
import gc

from dask.distributed import Client, LocalCluster

from aqua import __version__ as aquaversion
from aqua.util import load_yaml, get_arg
from aqua.util import add_pdf_metadata, add_png_metadata
from aqua.util import OutputSaver, ConfigPath
from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure
from aqua.graphics import plot_single_map, plot_single_map_diff
from aqua.diagnostics.teleconnections.plots import indexes_plot
from aqua.diagnostics.teleconnections.tc_class import Teleconnection
from aqua.diagnostics.teleconnections.tools import set_figs


def parse_arguments(cli_args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Teleconnections CLI')

    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
    parser.add_argument('-d', '--dry', action='store_true',
                        required=False,
                        help='if True, run is dry, no files are written')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')
    parser.add_argument('--ref', action='store_true',
                        required=False,
                        help='if True, analysis is performed against a reference')
    parser.add_argument("--cluster", type=str,
                        required=False, help="dask cluster address")

    # This arguments will override the configuration file if provided
    parser.add_argument('--catalog', type=str, help='catalog name',
                        required=False)
    parser.add_argument('--model', type=str, help='model name',
                        required=False)
    parser.add_argument('--exp', type=str, help='experiment name',
                        required=False)
    parser.add_argument('--source', type=str, help='source name',
                        required=False)
    parser.add_argument('--outputdir', type=str, help='output directory',
                        required=False)
    parser.add_argument('--interface', type=str, help='interface to use',
                        required=False)

    return parser.parse_args(cli_args)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_name='Teleconnections CLI', log_level=loglevel)

    logger.info(f'Running AQUA v{aquaversion} Teleconnections diagnostic')

    # change the current directory to the one of the CLI so that relative path works
    # before doing this we need to get the directory from wich the script is running
    execdir = os.getcwd()
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')

    # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    cluster = get_arg(args, 'cluster', None)
    private_cluster = False
    if nworkers or cluster:
        if not cluster:
            cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
            logger.info(f"Initializing private cluster {cluster.scheduler_address} with {nworkers} workers.")
            private_cluster = True
        else:
            logger.info(f"Connecting to cluster {cluster}.")
        client = Client(cluster)
    else:
        client = None

    # Read configuration file
    configdir = ConfigPath(loglevel=loglevel).configdir
    default_config = os.path.join(configdir, "diagnostics", "teleconnections",
                                  "cli_config_atm.yaml")
    file = get_arg(args, 'config', default_config)
    logger.info('Reading configuration yaml file: {}'.format(file))
    config = load_yaml(file)

    # if ref we're running the analysis against a reference
    ref = get_arg(args, 'ref', False)
    if ref:
        logger.debug('Running against a reference')

    # if dry we're not saving any file, debug mode
    dry = get_arg(args, 'dry', False)
    if dry:
        logger.warning('Dry run, no files will be written')
        save_pdf, save_png = False, False
        save_netcdf = False
    else:
        logger.debug('Saving files')
        save_pdf, save_png = True, True
        save_netcdf = True

    dpi = 300

    try:
        outputdir = get_arg(args, 'outputdir', config['outputdir'])
        # if the outputdir is relative we need to make it absolute
        if not os.path.isabs(outputdir):
            outputdir = os.path.join(execdir, outputdir)
    except KeyError:
        outputdir = None
        logger.error('Output directory not defined')

    configdir = config['configdir']
    logger.debug('configdir: %s', configdir)

    interface = get_arg(args, 'interface', config['interface'])
    logger.debug('Interface name: %s', interface)

    # Turning on/off the teleconnections
    # the try/except is used to avoid KeyError if the teleconnection is not
    # defined in the yaml file, since we have oceanic and atmospheric
    # configuration files
    NAO = config['teleconnections'].get('NAO', False)
    ENSO = config['teleconnections'].get('ENSO', False)

    teleclist = []
    if NAO:
        teleclist.append('NAO')
    if ENSO:
        teleclist.append('ENSO')

    logger.debug('Teleconnections to be evaluated: %s', teleclist)

    # if exclusive we're running only the first model/exp/source combination
    # if model/exp/source are provided as arguments, we're overriding the
    # first model/exp/source combination
    models = config['models']

    models[0]['catalog'] = get_arg(args, 'catalog', models[0]['catalog'])
    models[0]['model'] = get_arg(args, 'model', models[0]['model'])
    models[0]['exp'] = get_arg(args, 'exp', models[0]['exp'])
    models[0]['source'] = get_arg(args, 'source', models[0]['source'])

    for telec in teleclist:
        logger.info('Running %s teleconnection', telec)
        # Getting generic configs
        months_window = config[telec].get('months_window', 3)
        full_year = config[telec].get('full_year', True)
        seasons = config[telec].get('seasons', None)

        if ref:  # We run first the reference model since it's needed for comparison plots
            logger.info('--ref: evaluating reference models first')
            ref_config = config['reference'][0]
            catalog_ref = ref_config.get('catalog', 'obs')
            model_ref = ref_config.get('model', 'ERA5')
            exp_ref = ref_config.get('exp', 'era5')
            source_ref = ref_config.get('source', 'monthly')
            regrid = ref_config.get('regrid', None)
            freq = ref_config.get('freq', None)
            logger.debug("setup: %s %s %s %s %s",
                         model_ref, exp_ref, source_ref, regrid, freq)

            try:
                tc = Teleconnection(telecname=telec,
                                    configdir=configdir,
                                    catalog=catalog_ref,
                                    model=model_ref, exp=exp_ref, source=source_ref,
                                    regrid=regrid, freq=freq,
                                    months_window=months_window,
                                    outputdir=outputdir,
                                    save_pdf=False, save_png=False,
                                    save_netcdf=save_netcdf,
                                    interface=interface,
                                    loglevel=loglevel)
                tc.retrieve()
                output_saver = OutputSaver(diagnostic='teleconnections', catalog=tc.catalog, model=model_ref,
                               exp=exp_ref, loglevel=loglevel, default_path=outputdir, filename_keys=None)
            except NoDataError:
                logger.error('No data available for %s teleconnection', telec)
                continue
            except ValueError as e:
                logger.error('Error retrieving data for %s teleconnection: %s',
                             telec, e)
                continue
            except Exception as e:
                logger.error('Unexpected error retrieving data for %s teleconnection: %s',
                             telec, e)

            try:
                tc.evaluate_index()
                ref_index = tc.index
            except NotEnoughDataError:
                logger.error('Not enough data available for %s teleconnection', telec)
                continue
            except Exception as e:
                logger.error('Error evaluating index for %s teleconnection: %s', telec, e)
                continue

            # We now evaluate the regression and correlation
            # They are not saved, we just need them for comparison plots
            # so we save them as variables
            if full_year:
                try:
                    ref_reg_full = tc.evaluate_regression()
                    ref_cor_full = tc.evaluate_correlation()
                except NotEnoughDataError:
                    logger.error('Not enough data available for %s teleconnection',
                                 telec)
                    continue
            else:
                ref_reg_full = None
                ref_cor_full = None

            if seasons:
                ref_reg_season = []
                ref_cor_season = []
                for i, season in enumerate(seasons):
                    try:
                        logger.info('Evaluating %s regression and correlation for %s season',
                                    telec, season)
                        reg = tc.evaluate_regression(season=season)
                        ref_reg_season.append(reg)
                        cor = tc.evaluate_correlation(season=season)
                        ref_cor_season.append(cor)
                    except NotEnoughDataError:
                        logger.error('Not enough data available for %s teleconnection',
                                     telec)
                        continue
            else:
                ref_reg_season = None
                ref_cor_season = None

            del tc
            gc.collect()
        else:
            ref_index = None
            ref_reg_full = None
            ref_cor_full = None
            ref_reg_season = None
            ref_cor_season = None

        # Model evaluation
        logger.debug('Models to be evaluated: %s', models)
        for mod in models:
            catalog = mod['catalog']
            model = mod['model']
            exp = mod['exp']
            source = mod['source']
            regrid = mod.get('regrid', None)
            freq = mod.get('freq', None)
            reference = mod.get('reference', False)
            startdate = mod.get('startdate', None)
            enddate = mod.get('enddate', None)

            logger.debug("setup: %s %s %s %s %s",
                         model, exp, source, regrid, freq)

            try:
                tc = Teleconnection(telecname=telec,
                                    configdir=configdir,
                                    catalog=catalog,
                                    model=model, exp=exp, source=source,
                                    regrid=regrid, freq=freq,
                                    months_window=months_window,
                                    outputdir=outputdir,
                                    # If ref we do index plot against the reference model
                                    save_pdf=save_pdf if not ref else False,
                                    save_png=save_png if not ref else False,
                                    save_netcdf=save_netcdf,
                                    startdate=startdate, enddate=enddate,
                                    interface=interface,
                                    loglevel=loglevel)
                tc.retrieve()
                catalog = tc.catalog
                output_saver = OutputSaver(diagnostic='teleconnections', catalog=catalog, model=model,
                                           exp=exp, loglevel=loglevel, default_path=outputdir, filename_keys=None)
            except NoDataError:
                logger.error('No data available for %s teleconnection', telec)
                continue
            except ValueError as e:
                logger.error('Error retrieving data for %s teleconnection: %s',
                             telec, e)
                continue
            except Exception as e:
                logger.error('Unexpected error retrieving data for %s teleconnection: %s',
                             telec, e)

            try:
                tc.evaluate_index()
            except NotEnoughDataError:
                logger.error('Not enough data available for %s teleconnection', telec)
                continue
            except Exception as e:
                logger.error('Error evaluating index for %s teleconnection: %s', telec, e)
                continue

            if full_year:
                try:
                    reg_full = tc.evaluate_regression()
                    cor_full = tc.evaluate_correlation()
                except NotEnoughDataError:
                    logger.error('Not enough data available for %s teleconnection',
                                 telec)
                    continue
            else:
                reg_full = None
                cor_full = None

            if seasons:
                reg_season = []
                cor_season = []
                for i, season in enumerate(seasons):
                    try:
                        logger.info('Evaluating %s regression and correlation for %s season',
                                    telec, season)
                        reg = tc.evaluate_regression(season=season)
                        reg_season.append(reg)
                        cor = tc.evaluate_correlation(season=season)
                        cor_season.append(cor)
                    except NotEnoughDataError:
                        logger.error('Not enough data available for %s teleconnection',
                                     telec)
                        continue
            else:
                reg_season = None
                cor_season = None

            if save_pdf or save_png:
                if ref:  # Plot against the reference model
                    title = '{} index'.format(telec)
                    titles = ['{} index for {} {}'.format(telec, model, exp),
                              '{} index for {}'.format(telec, model_ref)]
                    description = '{} index plot for {} {} and {}'.format(telec, model, exp, model_ref)
                    logger.debug('Description: %s', description)
                    # Index plots
                    try:
                        fig, _ = indexes_plot(indx1=tc.index, indx2=ref_index, titles=titles, loglevel=loglevel)
                        common_save_args = {'diagnostic_product': telec + '_index', 'dpi': dpi, 'model_2': model_ref}
                        if save_pdf:
                            output_saver.save_pdf(fig, **common_save_args, metadata={'description': description})
                        if save_png:
                            output_saver.save_png(fig, **common_save_args, metadata={'description': description})
                    except Exception as e:
                        logger.error('Error plotting %s index: %s', telec, e)

                    # Correlation plot
                    map_names_pdf, map_names_png, maps, ref_maps, titles, descriptions, cbar_labels =\
                        set_figs(telec=telec,
                                 catalog=catalog,
                                 model=model,
                                 exp=exp,
                                 ref=model_ref,
                                 cor=True, reg=False,
                                 full_year=full_year,
                                 seasons=seasons,
                                 reg_full=reg_full,
                                 cor_full=cor_full,
                                 reg_season=reg_season,
                                 cor_season=cor_season,
                                 ref_reg_full_year=ref_reg_full,
                                 ref_cor_full_year=ref_cor_full,
                                 ref_reg_season=ref_reg_season,
                                 ref_cor_season=ref_cor_season)
                    logger.debug('map_names_pdf: %s (same for png)', map_names_pdf)
                    logger.debug('titles: %s', titles)
                    logger.debug('descriptions: %s', descriptions)
                    for i, data_map in enumerate(maps):
                        vmin = -1
                        vmax = 1
                        plot_args = {'data': data_map, 'data_ref': ref_maps[i], 'save': False, 'sym': False,
                                     'cbar_label': cbar_labels[i], 'outputdir': outputdir,
                                     'title': titles[i], 'vmin_contour': vmin, 'vmax_contour': vmax,
                                     'return_fig': True, 'vmin_fill': vmin, 'vmax_fill': vmax,
                                     'loglevel': loglevel}
                        try:
                            fig, _ =  plot_single_map_diff(**plot_args, transform_first=False)
                        except Exception as err:
                            logger.warning('Error plotting %s %s %s: %s',
                                           model, exp, map_names_pdf[i], err)
                            logger.info('Trying with transform_first=True')
                            try:
                                fig, _ = plot_single_map_diff(**plot_args, transform_first=True)
                            except Exception as err2:
                                logger.error('Error plotting %s %s %s: %s',
                                             model, exp, map_names_pdf[i], err2)
                        if save_pdf and fig is not None:
                            fig.savefig(os.path.join(outputdir, map_names_pdf[i]), dpi=dpi)
                            add_pdf_metadata(filename=os.path.join(outputdir, map_names_pdf[i]), metadata_value=descriptions[i])
                        if save_png and fig is not None:
                            fig.savefig(os.path.join(outputdir, map_names_png[i]), dpi=dpi)
                            add_png_metadata(png_path=os.path.join(outputdir, map_names_png[i]), metadata={'description': descriptions[i]})

                    # Regression plot
                    map_names_pdf, map_names_png, maps, ref_maps, titles, descriptions, cbar_labels =\
                        set_figs(telec=telec,
                                 catalog=catalog,
                                 model=model,
                                 exp=exp,
                                 ref=model_ref,
                                 cor=False, reg=True,
                                 full_year=full_year,
                                 seasons=seasons,
                                 reg_full=reg_full,
                                 cor_full=cor_full,
                                 reg_season=reg_season,
                                 cor_season=cor_season,
                                 ref_reg_full_year=ref_reg_full,
                                 ref_cor_full_year=ref_cor_full,
                                 ref_reg_season=ref_reg_season,
                                 ref_cor_season=ref_cor_season)
                    logger.debug('map_names: %s (same for png)', map_names_pdf)
                    logger.debug('titles: %s', titles)
                    logger.debug('descriptions: %s', descriptions)
                    for i, data_map in enumerate(maps):
                        vmin, vmax = config[telec].get('cbar_range', None)
                        if vmin is None or vmax is None:
                            sym = True
                        else:
                            sym = False
                        plot_args = {'data': data_map, 'data_ref': ref_maps[i], 'save': False, 'sym': sym,
                                     'cbar_label': cbar_labels[i], 'outputdir': outputdir,
                                     'title': titles[i], 'vmin_fill': vmin, 'vmax_fill': vmax, 'return_fig': True,
                                     'loglevel': loglevel}
                        try:
                            fig, _ = plot_single_map_diff(**plot_args, transform_first=False)
                        except Exception as err:
                            logger.warning('Error plotting %s %s %s: %s',
                                           model, exp, map_names_pdf[i], err)
                            logger.info('Trying with transform_first=True')
                            try:
                                fig, _ = plot_single_map_diff(**plot_args, transform_first=True)
                            except Exception as err2:
                                logger.error('Error plotting %s %s %s: %s',
                                             model, exp, map_names_pdf[i], err2)
                        if save_pdf and fig is not None:
                            fig.savefig(os.path.join(outputdir, map_names_pdf[i]), dpi=dpi)
                            add_pdf_metadata(filename=os.path.join(outputdir, map_names_pdf[i]), metadata_value=descriptions[i])
                        if save_png and fig is not None:
                            fig.savefig(os.path.join(outputdir, map_names_png[i]), dpi=dpi)
                            add_png_metadata(png_path=os.path.join(outputdir, map_names_png[i]), metadata={'description': descriptions[i]})
                else:  # Individual plots
                    # Index plot
                    try:
                        tc.plot_index()
                    except Exception as e:
                        logger.error('Error plotting %s index: %s', telec, e)
                    # Correlation plot
                    map_names_pdf, map_names_png, maps, ref_maps, titles, descriptions, cbar_labels = \
                        set_figs(telec=telec,
                                 catalog=catalog,
                                 model=model,
                                 exp=exp,
                                 cor=True, reg=False,
                                 full_year=full_year,
                                 seasons=seasons,
                                 reg_full=reg_full,
                                 cor_full=cor_full,
                                 reg_season=reg_season,
                                 cor_season=cor_season)
                    logger.debug('map_names: %s (same for png)', map_names_pdf)
                    logger.debug('titles: %s', titles)
                    logger.debug('descriptions: %s', descriptions)
                    for i, data_map in enumerate(maps):
                        vmin = -1
                        vmax = 1
                        plot_args = {'data': data_map, 'save': True, 'sym': False,
                                     'cbar_label': cbar_labels[i], 'outputdir': outputdir,
                                     'title': titles[i], 'vmin': vmin, 'vmax': vmax, 'return_fig': True,
                                     'loglevel': loglevel}
                        try:
                            fig, _ = plot_single_map(**plot_args, transform_first=False)
                        except Exception as err:
                            logger.warning('Error plotting %s %s %s: %s',
                                           model, exp, map_names_pdf[i], err)
                            logger.info('Trying with transform_first=True')
                            try:
                                fig, _ = plot_single_map(**plot_args, transform_first=True)
                            except Exception as err2:
                                logger.error('Error plotting %s %s %s: %s',
                                             model, exp, map_names_pdf[i], err2)
                        if save_pdf and fig is not None:
                            fig.savefig(os.path.join(outputdir, map_names_pdf[i]), dpi=dpi)
                            add_pdf_metadata(filename=os.path.join(outputdir, map_names_pdf[i]), metadata_value=descriptions[i])
                        if save_png and fig is not None:
                            fig.savefig(os.path.join(outputdir, map_names_png[i]), dpi=dpi)
                            add_png_metadata(png_path=os.path.join(outputdir, map_names_png[i]), metadata={'description': descriptions[i]})
                    # Regression plot
                    map_names_pdf, map_names_png, maps, ref_maps, titles, descriptions, cbar_labels =\
                        set_figs(telec=telec,
                                 catalog=catalog,
                                 model=model,
                                 exp=exp,
                                 cor=False, reg=True,
                                 full_year=full_year,
                                 seasons=seasons,
                                 reg_full=reg_full,
                                 cor_full=cor_full,
                                 reg_season=reg_season,
                                 cor_season=cor_season)
                    logger.debug('map_names: %s (same for png)', map_names_pdf)
                    logger.debug('titles: %s', titles)
                    logger.debug('descriptions: %s', descriptions)
                    for i, data_map in enumerate(maps):
                        vmin, vmax = config[telec].get('cbar_range', None)
                        if vmin is None or vmax is None:
                            sym = True
                        else:
                            sym = False
                        plot_args = {'data': data_map, 'save': True, 'sym': sym,
                                     'cbar_label': cbar_labels[i], 'outputdir': outputdir,
                                     'title': titles[i], 'vmin_fill': vmin, 'vmax_fill': vmax, 'return_fig': True,
                                     'loglevel': loglevel}
                        try:
                            fig, _ = plot_single_map(**plot_args, transform_first=False)
                        except Exception as err:
                            logger.warning('Error plotting %s %s %s: %s',
                                           model, exp, map_names_pdf[i], err)
                            logger.info('Trying with transform_first=True')
                            try:
                                fig, _ = plot_single_map(**plot_args, transform_first=True)
                            except Exception as err2:
                                logger.error('Error plotting %s %s %s: %s',
                                             model, exp, map_names_pdf[i], err2)
                        if save_pdf and fig is not None:
                            fig.savefig(os.path.join(outputdir, map_names_pdf[i]), dpi=dpi)
                            add_pdf_metadata(filename=os.path.join(outputdir, map_names_pdf[i]), metadata_value=descriptions[i])
                        if save_png and fig is not None:
                            fig.savefig(os.path.join(outputdir, map_names_png[i]), dpi=dpi)
                            add_png_metadata(png_path=os.path.join(outputdir, map_names_png[i]), metadata={'description': descriptions[i]})

    if client:
        client.close()
        logger.debug("Dask client closed.")

    if private_cluster:
        cluster.close()
        logger.debug("Dask cluster closed.")
    
    logger.info('Teleconnections diagnostic finished.')
