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
from teleconnections import __version__ as telecversion
from teleconnections.plots import single_map_plot
from teleconnections.tc_class import Teleconnection


def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Teleconnections CLI')

    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-d', '--dry', action='store_true',
                        required=False,
                        help='if True, run is dry, no files are written')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')
    parser.add_argument('--obs', action='store_true',
                        required=False,
                        help='evaluate observations')

    # This arguments will override the configuration file if provided
    parser.add_argument('--model', type=str, help='model name',
                        required=False)
    parser.add_argument('--exp', type=str, help='experiment name',
                        required=False)
    parser.add_argument('--source', type=str, help='source name',
                        required=False)
    parser.add_argument('--outputdir', type=str, help='output directory',
                        required=False)

    return parser.parse_args(args)


if __name__ == '__main__':

    print(f'Running AQUA v{aquaversion} Teleconnections diagnostic v{telecversion}')
    args = parse_arguments(sys.argv[1:])

    # Read configuration file
    file = get_arg(args, 'config', 'teleconnections_config.yaml')
    print('Reading configuration yaml file..')
    config = load_yaml(file)

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_name='Teleconnections CLI', log_level=loglevel)

    model = get_arg(args, 'model', config['model'])
    exp = get_arg(args, 'exp', config['exp'])
    source = get_arg(args, 'source', config['source'])

    # if obs we're performing the analysis for observations as well
    obs = get_arg(args, 'obs', False)

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

    # These may be needed if we're not using an LRA entry
    regrid = config['regrid']
    freq = config['freq']
    zoom = config['zoom']

    logger.debug('Reader configuration:')
    logger.debug('regrid: {}'.format(regrid))
    logger.debug('freq: {}'.format(freq))
    logger.debug('zoom: {}'.format(zoom))

    try:
        outputdir = get_arg(args, 'outputdir', config['outputdir'])
        outputnetcdf = os.path.join(outputdir, 'netcdf')
        outputpdf = os.path.join(outputdir, 'pdf')
    except KeyError:
        outputdir = None
        outputnetcdf = None
        outputpdf = None

    configdir = config['configdir']
    logger.debug('configdir: {}'.format(configdir))

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

    logger.debug('Teleconnections to be evaluated: {}'.format(teleclist))

    for telec in teleclist:
        logger.warning('Running {} teleconnection...'.format(telec))

        months_window = config[telec]['months_window']
        logger.debug('months_window: {}'.format(months_window))

        try:
            teleconnection = Teleconnection(telecname=telec,
                                            configdir=configdir,
                                            regrid=regrid, freq=freq,
                                            zoom=zoom,
                                            model=model, exp=exp,
                                            source=source,
                                            months_window=months_window,
                                            outputdir=os.path.join(outputnetcdf,
                                                                   telec),
                                            outputfig=os.path.join(outputpdf,
                                                                     telec),
                                            savefig=savefig, savefile=savefile,
                                            loglevel=loglevel)
            teleconnection.retrieve()
        except NoDataError:
            logger.error('No data available for {} teleconnection'.format(telec))
            sys.exit(0)

        # Data are available, are there enough?
        try:
            teleconnection.evaluate_index()
            teleconnection.evaluate_correlation()
            teleconnection.evaluate_regression()
        except NotEnoughDataError:
            logger.error('Not enough data available for {} teleconnection'.format(telec))
            sys.exit(0)

        if savefig:
            try:
                teleconnection.plot_index()
            except Exception as e:
                logger.error('Error plotting {} index: '.format(telec), e)

            # Regression map
            if telec == 'NAO':
                cbar_label = 'msl [hPa]'
            elif telec == 'ENSO':
                cbar_label = 'sst [K]'

            try:
                # NOTE: we always try transform_first=False because it
                # gives a better image quality
                single_map_plot(map=teleconnection.regression,
                                loglevel=loglevel,
                                model=model, exp=exp,
                                outputdir=teleconnection.outputfig,
                                filename=teleconnection.filename + '_regression.pdf',
                                save=True, cbar_label=cbar_label, sym=True,
                                transform_first=False)
            except Exception as e:
                try:
                    logger.error('Error plotting {} regression: '.format(telec), e)
                    logger.info('Trying transform_first=True')
                    single_map_plot(map=teleconnection.regression,
                                    loglevel=loglevel,
                                    model=model, exp=exp,
                                    outputdir=teleconnection.outputfig,
                                    filename=teleconnection.filename + '_regression.pdf',
                                    save=True, cbar_label=cbar_label, sym=True,
                                    transform_first=True)
                except Exception as e:
                    logger.error('Error plotting {} regression: '.format(telec), e)

            # Correlation map
            cbar = 'Pearson correlation'

            try:
                single_map_plot(map=teleconnection.correlation,
                                loglevel=loglevel,
                                model=model, exp=exp,
                                outputdir=teleconnection.outputfig,
                                filename=teleconnection.filename + '_correlation.pdf',
                                save=True, cbar_label=cbar, sym=True,
                                transform_first=False)
            except Exception as e:
                logger.error('Error plotting {} correlation: '.format(telec), e)
                logger.info('Trying transform_first=True')
                try:
                    single_map_plot(map=teleconnection.correlation,
                                    loglevel=loglevel,
                                    model=model, exp=exp,
                                    outputdir=teleconnection.outputfig,
                                    filename=teleconnection.filename + '_correlation.pdf',
                                    save=True, cbar_label=cbar, sym=True,
                                    transform_first=True)
                except Exception as e:
                    logger.error('Error plotting {} correlation: '.format(telec), e)

        if obs:
            logger.warning('Analysing ERA5')
            try:
                teleconnection_ERA5 = Teleconnection(telecname=telec,
                                                     configdir=configdir,
                                                     #regrid='r100', # This would be better but we've some issue with plots
                                                     model='ERA5', exp='era5',
                                                     source='monthly',
                                                     months_window=months_window,
                                                     outputdir=os.path.join(outputnetcdf,
                                                                            telec),
                                                     outputfig=os.path.join(outputpdf,
                                                                            telec),
                                                     savefig=savefig,
                                                     savefile=savefile,
                                                     loglevel=loglevel)
            except NoDataError:
                logger.error('No ERA5 data available for {} teleconnection'.format(telec))
                sys.exit(0)

            # Data are available, are there enough?
            try:
                teleconnection_ERA5.evaluate_index()
                teleconnection_ERA5.evaluate_correlation()
                teleconnection_ERA5.evaluate_regression()
            except NotEnoughDataError:
                logger.error('Not enough data available for ERA5 {} teleconnection'.format(telec))
                sys.exit(0)

            if savefig:
                try:
                    teleconnection_ERA5.plot_index()
                except Exception as e:
                    logger.error('Error plotting ERA5 {} index: '.format(telec), e)

                # Regression map
                if telec == 'NAO':
                    cbar_label = 'msl [hPa]'
                elif telec == 'ENSO':
                    cbar_label = 'sst [K]'

                try:
                    single_map_plot(map=teleconnection_ERA5.regression,
                                    loglevel=loglevel,
                                    title='ERA5',
                                    outputdir=teleconnection_ERA5.outputfig,
                                    filename=teleconnection_ERA5.filename + '_regression.pdf',
                                    save=True, cbar_label=cbar_label,
                                    sym=True, transform_first=False)
                except Exception as e:
                    logger.error('Error plotting ERA5 {} regression: '.format(telec), e)
                    logger.info('Trying transform_first=True')
                    try:
                        single_map_plot(map=teleconnection_ERA5.regression,
                                        loglevel=loglevel,
                                        title='ERA5',
                                        outputdir=teleconnection_ERA5.outputfig,
                                        filename=teleconnection_ERA5.filename + '_regression.pdf',
                                        save=True, cbar_label=cbar_label,
                                        sym=True, transform_first=True)
                    except Exception as e:
                        logger.error('Error plotting ERA5 {} regression: '.format(telec), e)

                # Correlation map
                cbar_label = 'Pearson correlation'

                try:
                    single_map_plot(map=teleconnection_ERA5.correlation,
                                    loglevel=loglevel,
                                    title='ERA5',
                                    outputdir=teleconnection_ERA5.outputfig,
                                    filename=teleconnection_ERA5.filename + '_correlation.pdf',
                                    save=True,
                                    cbar_label='Pearson correlation',
                                    sym=True, transform_first=False)
                except Exception as e:
                    logger.error('Error plotting ERA5 {} correlation: '.format(telec), e)
                    logger.info('Trying transform_first=True')
                    try:
                        single_map_plot(map=teleconnection_ERA5.correlation,
                                        loglevel=loglevel,
                                        title='ERA5',
                                        outputdir=teleconnection_ERA5.outputfig,
                                        filename=teleconnection_ERA5.filename + '_correlation.pdf',
                                        save=True,
                                        cbar_label='Pearson correlation',
                                        sym=True, transform_first=True)
                    except Exception as e:
                        logger.error('Error plotting ERA5 {} correlation: '.format(telec), e)

    logger.warning('Teleconnections diagnostic finished.')
