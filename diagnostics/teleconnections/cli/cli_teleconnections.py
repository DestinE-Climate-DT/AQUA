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
from teleconnections.plots import plot_single_map
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

    return parser.parse_args(cli_args)


if __name__ == '__main__':

    # change the current directory to the one of the CLI so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        print(f'Moving from current directory to {dname} to run!')

    print(f'Running AQUA v{aquaversion} Teleconnections diagnostic v{telecversion}')
    args = parse_arguments(sys.argv[1:])

    # Read configuration file
    file = get_arg(args, 'config', 'cli_config_atm.yaml')
    print('Reading configuration yaml file: {}'.format(file))
    config = load_yaml(file)

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_name='Teleconnections CLI', log_level=loglevel)

    # if ref we're running the analysis against a reference
    ref = get_arg(args, 'ref', False)

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
        outputnetcdf = os.path.join(outputdir, 'netcdf')
        outputpdf = os.path.join(outputdir, 'pdf')
    except KeyError:
        outputdir = None
        outputnetcdf = None
        outputpdf = None

    configdir = config['configdir']
    logger.debug('configdir: %s', configdir)

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
                                    loglevel=loglevel)
                tc.retrieve()
            except NoDataError:
                logger.error('No data available for %s teleconnection', telec)
                sys.exit(0)

            # Data are available, are there enough?
            try:
                tc.evaluate_index()
                tc.evaluate_correlation()
                tc.evaluate_regression()
            except NotEnoughDataError:
                logger.error('Not enough data available for %s teleconnection', telec)
                sys.exit(0)

            if savefig:
                try:
                    tc.plot_index()
                except Exception as e:
                    logger.error('Error plotting %s index: %s', telec, e)

                # Regression and correlation map setups
                # This way we make the plot routine more compact
                if telec == 'NAO':
                    # We duplicate maps if we create more plots
                    # for different teleconnections
                    map_names = ['regression', 'correlation']
                    maps = [tc.regression, tc.correlation]
                    cbar_label = ['msl [hPa]', 'Pearson correlation']
                    transform_first = False
                elif telec == 'ENSO':
                    map_names = ['regression', 'correlation']
                    maps = [tc.regression, tc.correlation]
                    cbar_label = ['sst [K]', 'Pearson correlation']
                    transform_first = True

                for i, data_map in enumerate(maps):
                    try:
                        plot_single_map(data=data_map,
                                        save=True,
                                        cbar_label=cbar_label[i],
                                        outputdir=tc.outputfig,
                                        filename=tc.filename + '_{}'.format(map_names[i]),
                                        title='{} {} {} {}'.format(model, exp,
                                                                   telec, map_names[i]),
                                        transform_first=transform_first,
                                        loglevel=loglevel)
                    except Exception as err:
                        logger.error('Error plotting %s %s %s %s: %s',
                                     model, exp, telec, map_names[i], err)
                        logger.info('Trying without contour')
                        try:
                            plot_single_map(data=data_map,
                                            save=True, contour=False,
                                            cbar_label=cbar_label[i],
                                            outputdir=tc.outputfig,
                                            filename=tc.filename + '_{}'.format(map_names[i]),
                                            title='{} {} {} {}'.format(model, exp,
                                                                       telec, map_names[i]),
                                            transform_first=transform_first,
                                            loglevel=loglevel)
                        except Exception as err2:
                            logger.error('Error plotting %s %s %s %s: %s',
                                         model, exp, telec, map_names[i], err2)

    # TODO: additional comparison plots can be added here

    logger.info('Teleconnections diagnostic finished.')
