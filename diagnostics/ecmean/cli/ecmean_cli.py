#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA dummy diagnositc command line interface. Reads configuration file and performs dummy 
diagnostic.

Important: need to be run from its own directory
'''
import sys
import argparse
import os
import xarray as xr
from ecmean.performance_indices import performance_indices
from ecmean import __version__ as eceversion
from aqua.util import load_yaml, get_arg, ConfigPath
from aqua import Reader
from aqua import __version__ as aquaversion
from aqua.logger import log_configure


def parse_arguments(args):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='ECmean Performance Indices  CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='ecmean yaml configuration file', default='config_ecmean_cli.yaml')
    parser.add_argument('-m', '--model_atm', type=str,
                        help='atmospheric model to be analysed')
    parser.add_argument('-x', '--model_oce', type=str,
                        help='oceanic model to be analysed')
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


def reader_data(model, exp, source, keep_vars):
    """
    Simple function to retrieve and do some operation on reader data
    """
    # if False/None return empty array
    if model is False:
        return None
    
    # Try to read the data, if dataset is not available return None
    try:
        reader = Reader(model=model, exp=exp, source=source, areas=False,
                        fix=False)
        data = reader.retrieve()
    except Exception as err:
        logger.error('Error while reading model %s: %s', model, err)
        return None

    # return only vars that are available
    if keep_vars is None:
        return data
    return data[[value for value in keep_vars if value in data.data_vars]]


if __name__ == '__main__':

    # change the current directory to the one of the CLI so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        print(f'Moving from current directory to {dname} to run!')

    print(f'Running AQUA v{aquaversion} Performance Indices diagnostic with ECmean4 v{eceversion}')
    args = parse_arguments(sys.argv[1:])
    file = get_arg(args, 'config', 'config_ecmean_cli.yaml')

    configfile = load_yaml(file)
    loglevel = configfile['setup']['loglevel']
    loglevel = get_arg(args, 'loglevel', loglevel)
    logger = log_configure(log_level=loglevel, log_name='PI')

    # setting options from configuration files
    atm_vars = configfile['dataset']['atm_vars']
    oce_vars = configfile['dataset']['oce_vars']
    numproc = configfile['compute']['numproc']
    config = configfile['setup']['config_file']

    # define the interface file
    Configurer = ConfigPath(configdir=None)
    interface = '../config/interface_AQUA_' + Configurer.machine + '.yml'

    # activate override from command line
    exp = get_arg(args, 'exp', configfile['dataset']['exp'])
    source = get_arg(args, 'source', 'lra-r100-monthly')
    model_atm = get_arg(args, 'model_atm', configfile['dataset']['model_atm'])
    model_oce = get_arg(args, 'model_oce', configfile['dataset']['model_oce'])
    outputdir = get_arg(args, 'outputdir', configfile['setup']['outputdir'])
    interface = get_arg(args, 'interface', interface)
    logger.debug('interface file %s', interface)

    # load the data
    logger.warning('Loading atmospheric data %s', model_atm)
    data_atm = reader_data(model=model_atm, exp=exp, source=source, keep_vars=atm_vars)
    logger.warning('Loading oceanic data from %s', model_oce)
    data_oce = reader_data(model=model_oce, exp=exp, source=source, keep_vars=oce_vars)

    # there are issues with vertical axis in LRA
    #if 'level' in data_atm.coords:
    #    data_atm = data_atm.rename({'level': 'plev'})

    # create a single dataset
    if data_oce is None:
        data = data_atm
        logger.info('No oceanic data, only atmospheric data will be used')
    elif data_atm is None:
        data = data_oce
        logger.info('No atmospheric data, only oceanic data will be used')
    else:
        data = xr.merge([data_atm, data_oce])
        logger.debug('Merging atmospheric and oceanic data')

    # Quit if no data is available
    if data is None:
        logger.error('No data available, exiting...')
        exit(0)

    # guessing years from the dataset
    year1 = int(data.time[0].values.astype('datetime64[Y]').astype(str))
    year2 = int(data.time[-1].values.astype('datetime64[Y]').astype(str))
    logger.warning('Guessing starting year %s and ending year %s',
                   year1, year2)

    # run the performance indices if you have at least 12 month of data
    if len(data.time) < 12:
        # NOTE: this should become a NotEnoughData exception
        logger.error('Not enough data, exiting...')
        exit(0)
    else:
        logger.warning('Launching ECmean performance indices...')
        performance_indices(exp, year1, year2, numproc=numproc, config=config,
                            interface=interface, loglevel=loglevel,
                            outputdir=outputdir, xdataset=data)

    print('AQUA ECmean4 Performance diagnostic run completed. Go outside and live your life!')
