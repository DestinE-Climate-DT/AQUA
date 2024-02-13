#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA teleconnections command line interface for a single dataset.
Reads configuration file and performs teleconnections diagnostic.
'''
import argparse
import os
import sys

from aqua import __version__ as aquaversion
from aqua.util import load_yaml, get_arg
from aqua.exceptions import NoDataError, NotEnoughDataError
from aqua.logger import log_configure
from aqua.graphics import plot_single_map
from teleconnections import __version__ as telecversion
from teleconnections.tc_class import Teleconnection


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

    # if reference is True the reference models are added to the list
    if ref:
        logger.info('--ref: adding reference models to the list')
        # Add a reference tag to the config block so that if needed
        # we can distinguish between reference and model
        for reference in config['references']:
            reference['reference'] = True
        models.extend(config['references'])

    logger.debug('Models to be evaluated: %s', models)

    for telec in teleclist:
        logger.info('Running %s teleconnection', telec)

        months_window = config[telec].get('months_window', 3)
        full_year = config[telec].get('full_year', True)
        seasons = config[telec].get('seasons', None)

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
                                    outputdir=os.path.join(outputnetcdf,
                                                           telec),
                                    outputfig=os.path.join(outputpdf,
                                                           telec),
                                    savefig=savefig, savefile=savefile,
                                    interface=interface,
                                    loglevel=loglevel)
                tc.retrieve()
            except NoDataError:
                logger.error('No data available for %s teleconnection', telec)
                sys.exit(0)

            # Data are available, are there enough?
            try:
                tc.evaluate_index()
                if full_year:
                    reg_full = tc.evaluate_regression()
                    cor_full = tc.evaluate_correlation()
            except NotEnoughDataError:
                logger.error('Not enough data available for %s teleconnection',
                             telec)
                sys.exit(0)

            if seasons:
                reg_season = []
                cor_season = []
                for i, season in enumerate(seasons):
                    try:
                        logger.info('Evaluating %s regression and correlation for %s season',
                                    telec, season)
                        # We need rebuild=True because tc.regression is
                        # already assigned and will not be rebuilt
                        reg = tc.evaluate_regression(season=season)
                        reg_season.append(reg)
                        cor = tc.evaluate_correlation(season=season)
                        cor_season.append(cor)
                    except NotEnoughDataError:
                        logger.error('Not enough data available for %s teleconnection',
                                     telec)
                        sys.exit(0)

            if savefig:
                try:
                    tc.plot_index()
                except Exception as e:
                    logger.error('Error plotting %s index: %s', telec, e)

                # Regression and correlation map setups
                # This way we make the plot routine more compact
                map_names = []  # we cover the case of no full_year
                maps = []
                titles = []
                if telec == 'NAO':
                    # TODO: units are Pa since msl is in Pa
                    #       but we should convert to hPa for readability
                    #       see issue #575
                    label1 = tc.var + ' [Pa]'
                    cbar_label = [label1, 'Pearson correlation']
                    transform_first = False
                    # We duplicate maps if we create more plots
                    # for different teleconnections
                    if full_year:
                        map_names = ['regression', 'correlation']
                        maps = [reg_full, cor_full]
                        titles = map_names
                elif telec == 'ENSO':
                    label1 = tc.var + ' [K]'
                    cbar_label = [label1, 'Pearson correlation']
                    transform_first = True
                    if full_year:
                        map_names = ['regression', 'correlation']
                        maps = [reg_full, cor_full]
                        titles = map_names
                if seasons and (telec == 'NAO' or telec == 'ENSO'):
                    for i, season in enumerate(seasons):
                        map_names.append('regression_{}'.format(season))
                        map_names.append('correlation_{}'.format(season))
                        maps.append(reg_season[i])
                        maps.append(cor_season[i])
                        cbar_label.append(cbar_label[0])
                        cbar_label.append(cbar_label[1])
                        titles.append('regression {}'.format(season))
                        titles.append('correlation {}'.format(season))
                logger.debug('map_names: %s', map_names)

                for i, data_map in enumerate(maps):
                    # Check if there is a correlation map
                    # if this is the case, we plot with vmin=-1 and vmax=1
                    if map_names[i].startswith('correlation'):
                        logger.debug('Setting vmin=-1 and vmax=1')
                        vmin = -1
                        vmax = 1
                    else:  # otherwise we evaluate vmin and vmax
                        vmin = None
                        vmax = None
                    try:
                        plot_single_map(data=data_map,
                                        save=True, sym=True,
                                        cbar_label=cbar_label[i],
                                        outputdir=tc.outputfig,
                                        filename=tc.filename + '_{}'.format(map_names[i]),
                                        title='{} {} {} {}'.format(model, exp,
                                                                   telec, titles[i]),
                                        transform_first=transform_first,
                                        vmin=vmin, vmax=vmax,
                                        loglevel=loglevel)
                    except Exception as err:
                        logger.error('Error plotting %s %s %s %s: %s',
                                     model, exp, telec, map_names[i], err)
                        logger.info('Trying without contour')
                        try:
                            plot_single_map(data=data_map, sym=True,
                                            save=True, contour=False,
                                            cbar_label=cbar_label[i],
                                            outputdir=tc.outputfig,
                                            filename=tc.filename + '_{}'.format(map_names[i]),
                                            title='{} {} {} {}'.format(model, exp,
                                                                       telec, titles[i]),
                                            transform_first=transform_first,
                                            vmin=vmin, vmax=vmax,
                                            loglevel=loglevel)
                        except Exception as err2:
                            logger.error('Error plotting %s %s %s %s: %s',
                                         model, exp, telec, map_names[i], err2)

    # TODO: additional comparison plots can be added here

    logger.info('Teleconnections diagnostic finished.')
