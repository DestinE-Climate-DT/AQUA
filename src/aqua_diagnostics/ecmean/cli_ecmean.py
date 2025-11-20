#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA ECmean4 Performance diagnostic CLI
'''
import argparse
import os
import sys
import xarray as xr
from ecmean import __version__ as eceversion

from aqua import Reader
from aqua import __version__ as aquaversion
from aqua.util import get_arg
from aqua.logger import log_configure
from aqua.exceptions import NoDataError, NotEnoughDataError

from aqua.diagnostics import PerformanceIndices, GlobalMean
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args, get_diagnostic_configpath
from aqua.diagnostics.core import template_parse_arguments, OutputSaver
from aqua.util import strlist_to_phrase, lat_to_phrase
from aqua.configurer import ConfigPath


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
    # source_oce is the source of the oceanic data,
    # to be used when oceanic data is in a different source than atmospheric data
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
                keep_vars=None, loglevel='WARNING',
                reader_kwargs: dict = {}):
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
        reader_kwargs (dict, optional): list of reader_kwargs. Defaults to {}.
    
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
        reader = Reader(
            model=model, exp=exp, source=source, catalog=catalog, 
            regrid=regrid, **reader_kwargs
        )
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
        mydata = data_atm
        if logger is not None:
            logger.warning('No oceanic data, only atmospheric data will be used')
    elif data_atm is None:
        mydata = data_oce
        if logger is not None:
            logger.warning('No atmospheric data, only oceanic data will be used')
    else:
        mydata = xr.merge([data_atm, data_oce])
        if logger is not None:
            logger.debug('Merging atmospheric and oceanic data')

    # Quit if no data is available
    if mydata is None:
        raise NoDataError('No data available, exiting...')
    
    return mydata

def time_check(mydata, y1, y2, logger=None):
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
    if y1 is None:
        y1 = int(mydata.time[0].values.astype('datetime64[Y]').astype(str))
        if logger is not None:
            logger.info('Guessing starting year %s', y1)
    if y2 is None:
        y2 = int(mydata.time[-1].values.astype('datetime64[Y]').astype(str))
        if logger is not None:
            logger.info('Guessing ending year %s', y2)

    # run the performance indices if you have at least 12 month of data
    if len(mydata.time) < 12:
        raise NotEnoughDataError("Not enough data, exiting...")

    return y1, y2


def set_description(diagnostic, model, exp, year1, year2, config):
    """
    Build the metadata description for figures.

    Args:
        diagnostic (str): The diagnostic type.
        model (str): Model name.
        exp (str): Experiment identifier.
        year1, year2 (int): First and last year of the period covered by the data.
        config (dict): configuration file.
    
    Returns:
        description (str)
    """
    model_time = f"for {model} {exp} from {year1}-01-01 to {year2}-12-31. "

    region_bounds = {
        'Global':       (-90.0,  90.0),
        'NH':           ( 20.0,  90.0),
        'SH':           (-90.0, -20.0),
        'Equatorial':   (-20.0,  20.0),
        'Tropical':     (-30.0,  30.0),
        'North Midlat': ( 30.0,  90.0),
        'South Midlat': (-90.0, -30.0),
        'North Pole':   ( 60.0,  90.0),
        'South Pole':   (-90.0, -60.0)
    }
    region_text = strlist_to_phrase([f"{r} ({lat_to_phrase(int(lat1))}-{lat_to_phrase(int(lat2))})"
                                       for r in config[diagnostic]["regions"] for (lat1, lat2) in [region_bounds.get(r, (0, 0))]])

    regions_phrase = f"Processed regions are {region_text}."

    if diagnostic == 'performance_indices':
        description = (f"Performance Indices normalized to the CMIP6 average "
                       f"for different regions and seasons {model_time}"
                       f"{regions_phrase}. Numbers < 1 imply better results than CMIP6 mean.")
    elif diagnostic == 'global_mean':
        description = (f"Global mean biases normalized to observed interannual variability "
                       f"with respect to references for different regions and seasons {model_time}"
                       f"{regions_phrase}")
    else:
        # produce a generic description
        description = f"Diagnostic {diagnostic} {model_time.strip()}"

    return description


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='ECmean')

    # load the configuration files and override with command line arguments
    config_dict = load_diagnostic_config(
        diagnostic='ecmean',
        config=args.config,
        default_config='config_ecmean_cli.yaml',
        loglevel=loglevel)
    config_dict = merge_config_args(config_dict, args)

    logger.info(
        'Running AQUA v%s Performance Indices diagnostic with ECmean4 v%s',
        aquaversion,
        eceversion
    )

    # set configuration
    ecmean_config = config_dict['diagnostics']['ecmean']
    output_config = config_dict['output']

    # define the output properties
    outputdir = output_config.get('outputdir')
    rebuild = output_config.get('rebuild', True)
    save_pdf = output_config.get('save_pdf', False)
    save_png = output_config.get('save_png', False)

    # merge config args works only with a predefined set of options, need to extend it
    numproc = get_arg(args, 'nprocs', ecmean_config.get('nprocs', 1))
    interface_file = get_arg(args, 'interface', ecmean_config.get('interface_file'))

    # define the interface file
    ecmeandir = get_diagnostic_configpath('ecmean', folder="tools", loglevel=loglevel)
    interface = os.path.join(ecmeandir, "interface", interface_file)

    # define the ecmean configuration file, using the default as a trick
    config = load_diagnostic_config(
        diagnostic='ecmean',
        folder="tools",
        config=None,
        default_config=ecmean_config.get('config_file'),
        loglevel=loglevel
    )
    # this is required to access the predefined areas and masks
    config['dirs']['exp'] = ecmeandir
    logger.debug('Default config file: %s', config)
    logger.debug('Definitive interface file %s', interface)

    # loop on datasets
    for dataset in config_dict['datasets']:
        model = get_arg(args, 'model', dataset.get('model'))
        exp = get_arg(args, 'exp', dataset.get('exp'))
        source_atm = get_arg(args, 'source', dataset.get('source', 'lra-r100-monthly'))
        source_oce = get_arg(args, 'source_oce', dataset.get('source_oce', source_atm))
        regrid = get_arg(args, 'regrid', dataset.get('regrid', 'r100'))
        catalog = get_arg(args, 'catalog', dataset.get('catalog'))
        startdate = get_arg(args, 'startdate', dataset.get('startdate'))
        enddate = get_arg(args, 'enddate', dataset.get('enddate'))
        if catalog is None:
            configurer = ConfigPath(loglevel=loglevel)
            cat, _, _ = configurer.deliver_intake_catalog(model=model, exp=exp, source=source_atm)
            catalog = cat.name

        # activate override from command line
        realization = get_arg(args, 'realization', None)
        # This reader_kwargs will be used if the dataset corresponding value is None or not present
        reader_kwargs = config_dict['datasets'][0].get('reader_kwargs') or {}
        if realization:
            reader_kwargs['realization'] = realization

        for diagnostic in ['global_mean', 'performance_indices']:

            diagnostic_name =  ecmean_config.get(diagnostic, 'ecmean').get('diagnostic_name', 'ecmean')
            outputsaver = OutputSaver(diagnostic=diagnostic_name,
                                  catalog=catalog, model=model, exp=exp,
                                  outputdir=outputdir, realization=realization, loglevel=loglevel)

            # setting options from configuration files
            atm_vars = ecmean_config[diagnostic]['atm_vars']
            oce_vars = ecmean_config[diagnostic]['oce_vars']
            year1 = ecmean_config[diagnostic].get('year1') if not startdate else int(startdate[:4])
            year2 = ecmean_config[diagnostic].get('year2') if not enddate else int(enddate[:4])

            # load the data
            logger.info('Loading atmospheric data %s', model)
            data_atm = reader_data(model=model, exp=exp, source=source_atm,
                                   catalog=catalog, keep_vars=atm_vars, regrid=regrid,
                                   reader_kwargs=reader_kwargs)

            logger.info('Loading oceanic data from %s', model)
            data_oce = reader_data(model=model, exp=exp, source=source_oce,
                                   catalog=catalog, keep_vars=oce_vars, regrid=regrid,
                                   reader_kwargs=reader_kwargs)

            # check the data
            data = data_check(data_atm, data_oce, logger=logger)
            year1, year2 = time_check(data, year1, year2, logger=logger)


            # store the data in the output saver and create the metadata
            filename_dict = {x: outputsaver.generate_path(extension=x, diagnostic_product=diagnostic) for x in ['yml', 'txt'] }
            description = set_description(diagnostic, model, exp, year1, year2, config)
            metadata = outputsaver.create_metadata(diagnostic_product=diagnostic,
                                                   metadata={'Description': description})

            # performance indices
            if diagnostic == 'performance_indices':
                logger.info('Launching ECmean performance indices...')
                ecmean = PerformanceIndices(exp, year1, year2, numproc=numproc, config=config,
                                            interface=interface, loglevel=loglevel,
                                            outputdir=outputdir, xdataset=data)
            elif diagnostic == 'global_mean':
                logger.info('Launching ECmean global mean...')
                ecmean = GlobalMean(exp, year1, year2, numproc=numproc, config=config,
                                    interface=interface, loglevel=loglevel,
                                    outputdir=outputdir, xdataset=data)
            else:
                logger.error('Unknown diagnostic %s, exiting...', diagnostic)
                sys.exit()
            
            ecmean.prepare()
            ecmean.run()
            if diagnostic == 'performance_indices':
                ecmean.store(yamlfile=filename_dict['yml'])
            elif diagnostic == 'global_mean':
                ecmean.store(yamlfile=filename_dict['yml'], tablefile=filename_dict['txt'])
            ecmean_fig = ecmean.plot(diagname=diagnostic, returnfig=True, storefig=False)
            if save_pdf:
                logger.info('Saving PDF %s plot...', diagnostic)
                outputsaver.save_pdf(fig=ecmean_fig, diagnostic_product=diagnostic,
                                     metadata=metadata, rebuild=rebuild)

            if save_png:
                logger.info('Saving PNG %s plot...', diagnostic)
                outputsaver.save_png(fig=ecmean_fig, diagnostic_product=diagnostic,
                                     metadata=metadata, rebuild=rebuild)

            logger.info('ECmean4 diagnostic completed.')