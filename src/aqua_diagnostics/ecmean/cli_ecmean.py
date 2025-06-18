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

from aqua import Reader
from aqua import __version__ as aquaversion
from aqua.util import load_yaml, get_arg, ConfigPath
from aqua.util import add_pdf_metadata, add_png_metadata
from aqua.logger import log_configure
from aqua.exceptions import NoDataError, NotEnoughDataError

from aqua.diagnostics import PerformanceIndices, GlobalMean
from aqua.diagnostics.core import template_parse_arguments
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args
from aqua.diagnostics.core import OutputSaver


def parse_arguments(arguments):
    """
    Parse command line arguments, extending the AQUA core parser
    """

    # load AQUA core diagnostic default parser
    parser = argparse.ArgumentParser(description='ECmean Performance Indices  CLI')
    parser = template_parse_arguments(parser)
    
    # Extend the parser with specific arguments for ECmean
    # processors here is controlled by multhprocess, so it is not standard dask workers
    # interface file is the one to match names of variables in the dataset
    # source_oce is the source of the oceanic data, to be used when oceanic data is in a different source than atmospheric data
    parser.add_argument('--nprocs',  type=int,
                        help='number of multiprocessing processes to use', default=1)
    parser.add_argument('-i', '--interface', type=str,
                        help='non-standard interface file')
    parser.add_argument('--source_oce', type=str, 
                        help='source of the oceanic data, to be used when oceanic data is in a different source than atmospheric data',
                        default=None)

    return parser.parse_args(arguments)


def reader_data(model, exp, source, 
                catalog=None, regrid='r100',
                keep_vars=None, loglevel='WARNING'):
    """
    Simple function to retrieve and do some operation on reader data

    Args:
        model (str): model name
        exp (str): experiment name
        source (str): source of the data
        catalog (str, optional): catalog to be used, defaults to None
        regrid (str, optional): regrid method, defaults to 'r100'
        keep_vars (list, optional): list of variables to keep, defaults to None
        loglevel (str, optional): logging level, defaults to 'WARNING'
    
    Returns:
        xarray.Dataset: dataset with the data retrieved and regridded
        None: if model is False or if there is an error retrieving the data
    """
    reader_logger = log_configure(log_level=loglevel, log_name='ECmean.Reader')

    # if False/None return empty array
    if model is False:
        return None

    # Try to read the data, if dataset is not available return None
    try:
        reader = Reader(model=model, exp=exp, source=source, catalog=catalog, 
                        regrid=regrid)
        xfield = reader.retrieve()
        if regrid is not None:
            xfield = reader.regrid(xfield)
     
    except Exception as err:
        reader_logger.error('Error while reading model %s: %s', model, err)
        return None

    # return only vars that are available: slower but avoid reader failures
    if keep_vars is None:
        return xfield
    return xfield[[value for value in keep_vars if value in xfield.data_vars]]

def data_check(data_atm, data_oce, logger=None):
    """
    Check if the data is available and has enough time steps

    Args:
        data_atm (xarray.Dataset): atmospheric data
        data_oce (xarray.Dataset): oceanic data
    """

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
    
    return data

def time_check(data, year1, year2, logger=None):
    """
    Check if the data has enough time steps

    Args:
        data (xarray.Dataset): dataset to check
        year1 (int): first year of the time period
        year2 (int): last year of the time period

    Raises:
        NotEnoughDataError: if the data does not have enough time steps
    """
    
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
    
    return year1, year2


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='ECmean')

    configfile = load_diagnostic_config(diagnostic='ecmean', args=args,
                           default_config='config_ecmean_cli.yaml', loglevel=loglevel)
    configfile = merge_config_args(configfile, args)

    
    logger.info(
        'Running AQUA v%s Performance Indices diagnostic with ECmean4 v%s',
        aquaversion,
        eceversion
    )

    # set configuration
    ecmean_config = configfile['diagnostics']['ecmean']
    output_config = configfile['output']

    # define the output properties
    outputdir = output_config.get('outputdir')
    rebuild = output_config.get('rebuild', True)
    save_pdf = output_config.get('save_pdf', False)
    save_png = output_config.get('save_png', False)

    # merge config args works only with a predefined set of options, need to extend it
    numproc = get_arg(args, 'nprocs', ecmean_config.get('nprocs', 1))
    interface_file = get_arg(args, 'interface', ecmean_config.get('interface_file'))

    # define the interface file
    ecmeandir = os.path.join(ConfigPath().configdir, 'diagnostics', 'ecmean')
    interface = os.path.join(ecmeandir, interface_file)
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
        source_atm = get_arg(args, 'source', dataset.get('source', 'lra-r100-monthly'))
        source_oce = get_arg(args, 'source_oce', dataset.get('source_oce', source_atm))
        regrid = get_arg(args, 'regrid', dataset.get('regrid'))

        logger.info('Model %s, exp %s, source %s', model, exp, source_atm)

        #setup the output saver
        outputsaver = OutputSaver(diagnostic='ecmean',
                          catalog=catalog, model=model, exp=exp,
                          outdir=outputdir, loglevel=loglevel)

        for diagnostic in ['global_mean', 'performance_indices']:

            # setting options from configuration files
            atm_vars = ecmean_config[diagnostic]['atm_vars']
            oce_vars = ecmean_config[diagnostic]['oce_vars']
            year1 = ecmean_config[diagnostic]['year1']
            year2 = ecmean_config[diagnostic]['year2']

            # load the data
            logger.info('Loading atmospheric data %s', model)
            data_atm = reader_data(model=model, exp=exp, source=source_atm,
                                catalog=catalog, keep_vars=atm_vars, regrid=regrid)

            logger.info('Loading oceanic data from %s', model)
            data_oce = reader_data(model=model, exp=exp, source=source_oce,
                                    catalog=catalog, keep_vars=oce_vars, regrid=regrid)

            # check the data
            data = data_check(data_atm, data_oce, logger=logger)
            year1, year2 = time_check(data, year1, year2, logger=logger)

            # store the data in the output saver and create the metadata
            filename_dict = {x: outputsaver.generate_path(extension=x, diagnostic_product=diagnostic) for x in ['yml', 'pdf', 'png'] }
            metadata = outputsaver.create_metadata(diagnostic_product=diagnostic)
            
            # performance indices
            if diagnostic == 'performance_indices':
                logger.info('Launching ECmean performance indices...')
                pi = PerformanceIndices(exp, year1, year2, numproc=numproc, config=config,
                                    interface=interface, loglevel=loglevel,
                                    outputdir=outputdir, xdataset=data)
                pi.prepare()
                pi.run()
                pi.store(yamlfile=filename_dict['yml'])
                if save_pdf and rebuild:
                    logger.info('Saving PDF performance indices plot...')
                    pi.plot(mapfile=filename_dict['pdf'])
                    add_pdf_metadata(filename_dict['pdf'], metadata, loglevel=loglevel)

                # there is a weird bug in ECmean when trying to plot the png
                #if save_png and rebuild:
                #    logger.info('Saving PNG performance indices plot...')
                #    pi.plot(mapfile=filename_dict['png'])

            # global mean
            if diagnostic == 'global_mean':
                logger.info('Launching ECmean global mean...')
                gm = GlobalMean(exp, year1, year2, numproc=numproc, config=config,
                                    interface=interface, loglevel=loglevel,
                                    outputdir=outputdir, xdataset=data)
                gm.prepare()
                gm.run()
                gm.store(yamlfile=filename_dict['yml'])
                if save_pdf and rebuild:
                    logger.info('Saving PDF global mean plot...')
                    gm.plot(mapfile=filename_dict['pdf'])
                    add_pdf_metadata(filename_dict['pdf'], metadata, loglevel=loglevel)
                if save_png and rebuild:
                    logger.info('Saving PNG global mean plot...')
                    gm.plot(mapfile=filename_dict['png'])
                    add_png_metadata(filename_dict['png'], metadata, loglevel=loglevel)

            logger.info('AQUA ECmean4 Performance Diagnostic is terminated. Go outside and live your life!')
