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

from aqua import __version__ as aquaversion
from aqua.util import load_yaml, get_arg, add_pdf_metadata
from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure
from aqua.graphics import plot_single_map, plot_single_map_diff
from teleconnections import __version__ as telecversion
from teleconnections.plots import indexes_plot
from teleconnections.tc_class import Teleconnection
from teleconnections.tools import set_figs, set_filename


def parse_arguments(cli_args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Teleconnections CLI')

    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-d', '--dry', action='store_true',
                        required=False,
                        help='if True, run is dry, no files are written')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')
    parser.add_argument('--ref', action='store_true',
                        required=False,
                        help='if True, analysis is performed against a reference')

    # This arguments will override the configuration file if provided
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

    logger.info(f'Running AQUA v{aquaversion} Teleconnections diagnostic v{telecversion}')

    # change the current directory to the one of the CLI so that relative path works
    # before doing this we need to get the directory from wich the script is running
    execdir = os.getcwd()
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')

    # Read configuration file
    file = get_arg(args, 'config', 'cli_config_atm.yaml')
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
        savefig = False
        savefile = False
    else:
        logger.debug('Saving files')
        savefig = True
        savefile = True

    try:
        outputdir = get_arg(args, 'outputdir', config['outputdir'])
        # if the outputdir is relative we need to make it absolute
        if not os.path.isabs(outputdir):
            outputdir = os.path.join(execdir, outputdir)
        outputnetcdf = os.path.join(outputdir, 'netcdf')
        outputpdf = os.path.join(outputdir, 'pdf')
    except KeyError:
        outputdir = None
        outputnetcdf = None
        outputpdf = None
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
            model_ref = ref_config.get('model', 'ERA5')
            exp_ref = ref_config.get('exp', 'era5')
            source_ref = ref_config.get('source', 'monthly')
            regrid = ref_config.get('regrid', None)
            zoom = ref_config.get('zoom', None)
            freq = ref_config.get('freq', None)
            logger.debug("setup: %s %s %s %s %s %s",
                         model_ref, exp_ref, source_ref, regrid, freq, zoom)

            try:
                tc = Teleconnection(telecname=telec,
                                    configdir=configdir,
                                    model=model_ref, exp=exp_ref, source=source_ref,
                                    regrid=regrid, freq=freq, zoom=zoom,
                                    months_window=months_window,
                                    outputdir=outputnetcdf,
                                    outputfig=outputpdf,
                                    savefig=savefig, savefile=savefile,
                                    interface=interface,
                                    loglevel=loglevel)
                tc.retrieve()
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
            model = mod['model']
            exp = mod['exp']
            source = mod['source']
            regrid = mod.get('regrid', None)
            freq = mod.get('freq', None)
            zoom = mod.get('zoom', None)
            reference = mod.get('reference', False)

            logger.debug("setup: %s %s %s %s %s %s",
                         model, exp, source, regrid, freq, zoom)

            try:
                tc = Teleconnection(telecname=telec,
                                    configdir=configdir,
                                    model=model, exp=exp, source=source,
                                    regrid=regrid, freq=freq, zoom=zoom,
                                    months_window=months_window,
                                    outputdir=outputnetcdf,
                                    outputfig=outputpdf,
                                    savefig=savefig, savefile=savefile,
                                    interface=interface,
                                    loglevel=loglevel)
                tc.retrieve()
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

            if savefig:
                if ref:  # Plot against the reference model
                    title = '{} index'.format(telec)
                    titles = ['{} index for {} {}'.format(telec, model, exp),
                              '{} index for {}'.format(telec, model_ref)]
                    description = '{} index plot for {} {} and {}'.format(telec, model, exp, model_ref)
                    logger.debug('Description: %s', description)
                    # Index plots
                    try:
                        filename = set_filename(filename=tc.filename, fig_type='index')
                        filename += '_{}'.format(model_ref)
                        filename += '.pdf'
                        indexes_plot(indx1=tc.index, indx2=ref_index,
                                     titles=titles,
                                     save=True, outputdir=tc.outputfig,
                                     filename=filename,
                                     loglevel=loglevel)
                        add_pdf_metadata(filename=os.path.join(tc.outputfig, filename),
                                         metadata_value=description)
                    except Exception as e:
                        logger.error('Error plotting %s index: %s', telec, e)

                    # Correlation plot
                    map_names, maps, ref_maps, titles, descriptions, cbar_labels = set_figs(telec=telec,
                                                                                            model=model,
                                                                                            exp=exp,
                                                                                            ref=model_ref,
                                                                                            filename=tc.filename,
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
                    logger.debug('map_names: %s', map_names)
                    logger.debug('titles: %s', titles)
                    logger.debug('descriptions: %s', descriptions)
                    for i, data_map in enumerate(maps):
                        vmin = -1
                        vmax = 1
                        try:
                            plot_single_map_diff(data=data_map,
                                                 data_ref=ref_maps[i],
                                                 save=True,
                                                 sym=False, sym_contour=False,
                                                 cbar_label=cbar_labels[i],
                                                 outputdir=tc.outputfig,
                                                 filename=map_names[i],
                                                 title=titles[i],
                                                 transform_first=False,
                                                 vmin_contour=vmin, vmax_contour=vmax,
                                                 vmin_fill=vmin, vmax_fill=vmax,
                                                 loglevel=loglevel)
                        except Exception as err:
                            logger.warning('Error plotting %s %s %s: %s',
                                           model, exp, map_names[i], err)
                            logger.info('Trying with transform_first=True')
                            try:
                                plot_single_map_diff(data=data_map,
                                                     data_ref=ref_maps[i],
                                                     save=True,
                                                     sym=False, sym_contour=False,
                                                     cbar_label=cbar_labels[i],
                                                     outputdir=tc.outputfig,
                                                     filename=map_names[i],
                                                     title=titles[i],
                                                     transform_first=True,
                                                     vmin_contour=vmin, vmax_contour=vmax,
                                                     vmin_fill=vmin, vmax_fill=vmax,
                                                     loglevel=loglevel)
                            except Exception as err2:
                                logger.error('Error plotting %s %s %s: %s',
                                             model, exp, map_names[i], err2)
                        try:
                            add_pdf_metadata(filename=os.path.join(tc.outputfig, map_names[i]),
                                             metadata_value=descriptions[i])
                        except FileNotFoundError as e:
                            logger.error('Error adding metadata to %s: %s', map_names[i], e)

                    # Regression plot
                    map_names, maps, ref_maps, titles, descriptions, cbar_labels = set_figs(telec=telec,
                                                                                            model=model,
                                                                                            exp=exp,
                                                                                            ref=model_ref,
                                                                                            filename=tc.filename,
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
                    logger.debug('map_names: %s', map_names)
                    logger.debug('titles: %s', titles)
                    logger.debug('descriptions: %s', descriptions)
                    for i, data_map in enumerate(maps):
                        try:
                            plot_single_map_diff(data=data_map,
                                                 data_ref=ref_maps[i],
                                                 save=True, sym=True,
                                                 cbar_label=cbar_labels[i],
                                                 outputdir=tc.outputfig,
                                                 filename=map_names[i],
                                                 title=titles[i],
                                                 transform_first=False,
                                                 loglevel=loglevel)
                        except Exception as err:
                            logger.warning('Error plotting %s %s %s: %s',
                                           model, exp, map_names[i], err)
                            logger.info('Trying with transform_first=True')
                            try:
                                plot_single_map_diff(data=data_map,
                                                     data_ref=ref_maps[i],
                                                     save=True, sym=True,
                                                     cbar_label=cbar_labels[i],
                                                     outputdir=tc.outputfig,
                                                     filename=map_names[i],
                                                     title=titles[i],
                                                     transform_first=True,
                                                     loglevel=loglevel)
                            except Exception as err2:
                                logger.error('Error plotting %s %s %s: %s',
                                             model, exp, map_names[i], err2)
                        try:
                            add_pdf_metadata(filename=os.path.join(tc.outputfig, map_names[i]),
                                             metadata_value=descriptions[i])
                        except FileNotFoundError as e:
                            logger.error('Error adding metadata to %s: %s', map_names[i], e)
                else:  # Individual plots
                    # Index plot
                    try:
                        tc.plot_index()
                    except Exception as e:
                        logger.error('Error plotting %s index: %s', telec, e)
                    # Correlation plot
                    map_names, maps, ref_maps, titles, descriptions, cbar_labels = set_figs(telec=telec,
                                                                                            model=model,
                                                                                            exp=exp,
                                                                                            filename=tc.filename,
                                                                                            cor=True, reg=False,
                                                                                            full_year=full_year,
                                                                                            seasons=seasons,
                                                                                            reg_full=reg_full,
                                                                                            cor_full=cor_full,
                                                                                            reg_season=reg_season,
                                                                                            cor_season=cor_season)
                    logger.debug('map_names: %s', map_names)
                    logger.debug('titles: %s', titles)
                    logger.debug('descriptions: %s', descriptions)
                    for i, data_map in enumerate(maps):
                        vmin = -1
                        vmax = 1
                        try:
                            plot_single_map(data=data_map,
                                            save=True, sym=False,
                                            cbar_label=cbar_labels[i],
                                            outputdir=tc.outputfig,
                                            filename=map_names[i],
                                            title=titles[i],
                                            transform_first=False,
                                            vmin=vmin, vmax=vmax,
                                            loglevel=loglevel)
                        except Exception as err:
                            logger.warning('Error plotting %s %s %s: %s',
                                           model, exp, map_names[i], err)
                            logger.info('Trying with transform_first=True')
                            try:
                                plot_single_map(data=data_map,
                                                save=True, sym=False,
                                                cbar_label=cbar_labels[i],
                                                outputdir=tc.outputfig,
                                                filename=map_names[i],
                                                title=titles[i],
                                                transform_first=True,
                                                vmin=vmin, vmax=vmax,
                                                loglevel=loglevel)
                            except Exception as err2:
                                logger.error('Error plotting %s %s %s: %s',
                                             model, exp, map_names[i], err2)
                        try:
                            add_pdf_metadata(filename=os.path.join(tc.outputfig, map_names[i]),
                                             metadata_value=descriptions[i])
                        except FileNotFoundError as e:
                            logger.error('Error adding metadata to %s: %s', map_names[i], e)
                    # Regression plot
                    map_names, maps, ref_maps, titles, descriptions, cbar_labels = set_figs(telec=telec,
                                                                                            model=model,
                                                                                            exp=exp,
                                                                                            filename=tc.filename,
                                                                                            cor=False, reg=True,
                                                                                            full_year=full_year,
                                                                                            seasons=seasons,
                                                                                            reg_full=reg_full,
                                                                                            cor_full=cor_full,
                                                                                            reg_season=reg_season,
                                                                                            cor_season=cor_season)
                    logger.debug('map_names: %s', map_names)
                    logger.debug('titles: %s', titles)
                    logger.debug('descriptions: %s', descriptions)
                    for i, data_map in enumerate(maps):
                        try:
                            plot_single_map(data=data_map,
                                            save=True, sym=True,
                                            cbar_label=cbar_labels[i],
                                            outputdir=tc.outputfig,
                                            filename=map_names[i],
                                            title=titles[i],
                                            transform_first=False,
                                            loglevel=loglevel)
                        except Exception as err:
                            logger.warning('Error plotting %s %s %s: %s',
                                           model, exp, map_names[i], err)
                            logger.info('Trying with transform_first=True')
                            try:
                                plot_single_map(data=data_map,
                                                save=True, sym=True,
                                                cbar_label=cbar_labels[i],
                                                outputdir=tc.outputfig,
                                                filename=map_names[i],
                                                title=titles[i],
                                                transform_first=True,
                                                loglevel=loglevel)
                            except Exception as err2:
                                logger.error('Error plotting %s %s %s: %s',
                                             model, exp, map_names[i], err2)
                        try:
                            add_pdf_metadata(filename=os.path.join(tc.outputfig, map_names[i]),
                                             metadata_value=descriptions[i])
                        except FileNotFoundError as e:
                            logger.error('Error adding metadata to %s: %s', map_names[i], e)

    logger.info('Teleconnections diagnostic finished.')
