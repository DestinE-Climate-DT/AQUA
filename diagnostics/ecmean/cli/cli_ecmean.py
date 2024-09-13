#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA ECmean4 Performance diagnostic CLI
'''
import sys
import argparse
import os
import xarray as xr
from ecmean.performance_indices import performance_indices
from ecmean.global_mean import global_mean
from ecmean import __version__ as eceversion
from aqua.util import load_yaml, get_arg, ConfigPath
from aqua import Reader
from aqua import __version__ as aquaversion
from aqua.logger import log_configure
from aqua.exceptions import NoDataError, NotEnoughDataError


def parse_arguments(args):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='ECmean Performance Indices  CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='ecmean yaml configuration file', default='config_ecmean_cli.yaml')
    parser.add_argument('-n', '--nworkers',  type=int,
                        help='number of dask distributed processes')
    parser.add_argument('--catalog', type=str,
                        help='catalog to be analysed')    
    parser.add_argument('-m', '--model', type=str,
                        help='model to be analysed')
    parser.add_argument('-e', '--exp', type=str,
                        help='exp to be analysed')
    parser.add_argument('-s', '--source', type=str,
                        help='source to be analysed', default='lra-r100-monthly')
    parser.add_argument('-i', '--interface', type=str,
                        help='non-standard interface file')
    parser.add_argument('-o', '--outputdir', type=str,
                        help='source to be analysed')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]', default='warning')

    return parser.parse_args(args)


def reader_data(model, exp, source, catalog=None, keep_vars=None):
    """
    Simple function to retrieve and do some operation on reader data
    """
    # if False/None return empty array
    if model is False:
        return None

    # Try to read the data, if dataset is not available return None
    try:
        reader = Reader(model=model, exp=exp, source=source, catalog=catalog, 
                        areas=False)
        data = reader.retrieve()
     
    except Exception as err:
        logger.error('Error while reading model %s: %s', model, err)
        return None

    # return only vars that are available: slower but avoid reader failures
    if keep_vars is None:
        return data
    return data[[value for value in keep_vars if value in data.data_vars]]


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='PI')

    # change the current directory to the one of the CLI so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')

    file = get_arg(args, 'config', 'config_ecmean_cli.yaml')
    configfile = load_yaml(file)
    logger.info(f'Running AQUA v{aquaversion} Performance Indices diagnostic with ECmean4 v{eceversion}')

    # setting options from configuration files
    atm_vars = configfile['dataset']['atm_vars']
    oce_vars = configfile['dataset']['oce_vars']
    year1 = configfile['dataset']['year1']
    year2 = configfile['dataset']['year2']
    config = configfile['setup']['config_file']

    numproc = get_arg(args, 'nworkers', configfile['compute']['numproc'])

    # define the interface file
    Configurer = ConfigPath(configdir=None)
    #interface = '../config/interface_AQUA_' + Configurer.catalog + '.yml'
    interface = '../config/interface_AQUA_destine-v1.yml'
    logger.debug('Default interface file: %s', interface)

    # activate override from command line
    exp = get_arg(args, 'exp', configfile['dataset']['exp'])
    source = get_arg(args, 'source', 'lra-r100-monthly')
    model = get_arg(args, 'model', configfile['dataset']['model'])
    catalog = get_arg(args, 'catalog', configfile['dataset']['catalog'])
    outputdir = get_arg(args, 'outputdir', configfile['setup']['outputdir'])
    interface = get_arg(args, 'interface', interface)
    logger.debug('Definitive interface file %s', interface)

    # load the data
    logger.info('Loading atmospheric data %s', model)
    data_atm = reader_data(model=model, exp=exp, source=source, 
                           catalog=catalog, keep_vars=atm_vars)
    
    logger.info('Loading oceanic data from %s', model)
    data_oce = reader_data(model=model, exp=exp, source=source, 
                            catalog=catalog, keep_vars=oce_vars)

    # create a single dataset
    if data_oce is None:
        data = data_atm
        logger.warning('No oceanic data, only atmospheric data will be used')
    elif data_atm is None:
        data = data_oce
        logger.warning('No atmospheric data, only oceanic data will be used')
    else:
        data = xr.merge([data_atm, data_oce])
        logger.debug('Merging atmospheric and oceanic data')

    # Quit if no data is available
    if data is None:
        raise NoDataError('No data available, exiting...')

    # guessing years from the dataset
    if year1 is None:
        year1 = int(data.time[0].values.astype('datetime64[Y]').astype(str))
        logger.info('Guessing starting year %s', year1)
    if year2 is None:
        year2 = int(data.time[-1].values.astype('datetime64[Y]').astype(str))
        logger.info('Guessing ending year %s', year2)

    # run the performance indices if you have at least 12 month of data
    if len(data.time) < 12:
        raise NotEnoughDataError("Not enough data, exiting...")
    else:
        logger.info('Launching ECmean performance indices...')
        performance_indices(exp, year1, year2, numproc=numproc, config=config,
                            interface=interface, loglevel=loglevel,
                            outputdir=outputdir, xdataset=data)
        logger.info('Launching ECmean global mean...')
        global_mean(exp, year1, year2, numproc=numproc, config=config,
                            interface=interface, loglevel=loglevel,
                            outputdir=outputdir, xdataset=data)

    logger.info('AQUA ECmean4 Performance Diagnostic is terminated. Go outside and live your life!')
