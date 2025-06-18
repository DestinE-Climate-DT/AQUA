#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA ECmean4 Performance diagnostic CLI
'''
import sys
import argparse
import os
import xarray as xr
from ecmean import __version__ as eceversion
from aqua.diagnostics import PerformanceIndices, GlobalMean
from aqua.util import load_yaml, get_arg, ConfigPath, OutputSaver
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
                        help='source to be analysed')
    parser.add_argument('--regrid', type=str,
                        help='regrid method to be used [default: r100]', default='r100')
    parser.add_argument('-i', '--interface', type=str,
                        help='non-standard interface file')
    parser.add_argument('-o', '--outputdir', type=str,
                        help='source to be analysed')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]', default='warning')

    return parser.parse_args(args)


def reader_data(model, exp, source, catalog=None, regrid='r100', keep_vars=None):
    """
    Simple function to retrieve and do some operation on reader data

    Args:
        model (str): model name
        exp (str): experiment name
        source (str): source of the data
        catalog (str, optional): catalog to be used, defaults to None
        regrid (str, optional): regrid method, defaults to 'r100'
        keep_vars (list, optional): list of variables to keep, defaults to None
    
    Returns:
        xarray.Dataset: dataset with the data retrieved and regridded
        None: if model is False or if there is an error retrieving the data
    """
    # if False/None return empty array
    if model is False:
        return None

    # Try to read the data, if dataset is not available return None
    try:
        reader = Reader(model=model, exp=exp, source=source, catalog=catalog, 
                        regrid='r100')
        data = reader.retrieve()
        data = reader.regrid(data)
     
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
    logger = log_configure(log_level=loglevel, log_name='ECmean')

    # change the current directory to the one of the CLI so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info('Moving from current directory to %s to run!', dname)

    file = get_arg(args, 'config', 'config_ecmean_cli.yaml')
    configfile = load_yaml(file)
    logger.info(
        'Running AQUA v%s Performance Indices diagnostic with ECmean4 v%s',
        aquaversion,
        eceversion
    )

    # set configuration
    ecmean_config = configfile['diagnostics']['ecmean']
    output_config = configfile['output']

    outputdir = get_arg(args, 'outputdir', output_config.get('outputdir'))
    numproc = get_arg(args, 'nworkers', ecmean_config.get('numproc'))

    # define the interface file
    Configurer = ConfigPath()
    ecmeandir = os.path.join(Configurer.configdir, 'diagnostics', 'ecmean')
    interface = get_arg(args, 'interface', ecmean_config.get('interface_file'))
    interface = os.path.join(ecmeandir, interface)
    logger.debug('Default interface file: %s', interface)

    # define the ecmean configuration file
    config = os.path.join(ecmeandir, ecmean_config.get('config_file'))
    config = load_yaml(config)
    config['dirs']['exp'] = ecmeandir
    logger.debug('Default config file: %s', config)
    logger.debug('Definitive interface file %s', interface)

    # loop on datasets
    for dataset in configfile['datasets']:
        catalog = get_arg(args, 'catalog', dataset.get('catalog'))
        model = get_arg(args, 'model', dataset.get('model'))
        exp = get_arg(args, 'exp', dataset.get('exp'))
        source = get_arg(args, 'source', dataset.get('source', 'lra-r100-monthly'))
        regrid = get_arg(args, 'regrid', dataset.get('regrid'))

        logger.info('Model %s, exp %s, source %s', model, exp, source)

        for diagnostic in ['global_mean', 'performance_indices']:

            # setting options from configuration files
            atm_vars = ecmean_config[diagnostic]['atm_vars']
            oce_vars = ecmean_config[diagnostic]['oce_vars']
            year1 = ecmean_config[diagnostic]['year1']
            year2 = ecmean_config[diagnostic]['year2']

            # load the data
            logger.info('Loading atmospheric data %s', model)
            data_atm = reader_data(model=model, exp=exp, source=source,
                                catalog=catalog, keep_vars=atm_vars, regrid=regrid)
            
            logger.info('Loading oceanic data from %s', model)
            data_oce = reader_data(model=model, exp=exp, source=source, 
                                    catalog=catalog, keep_vars=oce_vars, regrid=regrid)

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
       
            if diagnostic == 'performance_indices':
                logger.info('Launching ECmean performance indices...')
                pi = PerformanceIndices(exp, year1, year2, numproc=numproc, config=config,
                                    interface=interface, loglevel=loglevel,
                                    outputdir=outputdir, xdataset=data)
                pi.prepare()
                pi.run()
                pi.store()
                pi.plot()
            if diagnostic == 'global_mean':
                logger.info('Launching ECmean global mean...')
                gm = GlobalMean(exp, year1, year2, numproc=numproc, config=config,
                                    interface=interface, loglevel=loglevel,
                                    outputdir=outputdir, xdataset=data)
                gm.prepare()
                gm.run()
                gm.store()
                gm.plot()

            logger.info('AQUA ECmean4 Performance Diagnostic is terminated. Go outside and live your life!')
