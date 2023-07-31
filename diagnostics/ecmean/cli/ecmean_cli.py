#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA dummy diagnositc command line interface. Reads configuration file and performs dummy 
diagnostic
'''
import sys
import argparse
import xarray as xr
from aqua.util import load_yaml, get_arg
from aqua import Reader
from ecmean.performance_indices import performance_indices


def parse_arguments(args):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='ECmean Performance Indices  CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    return parser.parse_args(args)


def reader_data(model, exp, keep_vars):
    """
    Simple function to retrieve and do some operation on reader data
    """

    reader = Reader(model=model, exp=exp, source="lra-r100-monthly", areas=False)
    data = reader.retrieve(fix=False)

    # return only vars that are available
    if keep_vars is None:
        return data
    return data[[value for value in keep_vars if value in data.data_vars]]
    
if __name__ == '__main__':

    print('Running AQUA Performance Indices diagnostic...')
    args = parse_arguments(sys.argv[1:])
    file = get_arg(args, 'config', 'config_ecmean_cli.yaml')
    print('Reading configuration yaml file..')

    configfile = load_yaml(file)

    model_atm = configfile['dataset']['model_atm']
    model_oce = configfile['dataset']['model_oce']
    exp = configfile['dataset']['exp']
    year1 = configfile['dataset']['year1']
    year2 = configfile['dataset']['year2']

    numproc = configfile['compute']['numproc']

    interface = configfile['setup']['interface_file']
    loglevel = configfile['setup']['loglevel']
    config = configfile['setup']['config_file']

    loglevel = get_arg(args, 'loglevel', loglevel)

    # these are hard coded because are the ones we need to check
    atm_vars = ['2t', 'tprate', 'msl', 'u', 'v', 't', 'q']
    data_atm = reader_data(model=model_atm, exp=exp, keep_vars=atm_vars)
    oce_vars = ['sst', 'ci', 'sos']
    data_oce = reader_data(model=model_oce, exp=exp, keep_vars=oce_vars)

    # there are issues with vertical axis in LRA
    #if 'level' in data_atm.coords:
    #    data_atm = data_atm.rename({'level': 'plev'})

    # create a single dataset
    data = xr.merge([data_atm, data_oce])
 

    # run the performance indices
    performance_indices(exp, year1, year2, numproc = numproc, config = config,
            interface = interface, loglevel = loglevel, xdataset = data)


    print('AQUA ECmean4 Performance diagnostic run completed. Go outside and live your life!')
