#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA regridding tool to create low resolution archive.
Make use of aqua.LRA_Generator class to perform the regridding.
Functionality can be controlled through CLI options and
a configuration yaml file.
'''
import sys
import argparse
from aqua import LRAgenerator
from aqua import OPAgenerator
from aqua.util import load_yaml, get_arg
from glob import glob
import os


def parse_arguments(args):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='AQUA LRA generator')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-w', '--workers', type=str,
                        help='number of dask workers')
    parser.add_argument('-d', '--definitive', action="store_true",
                        help='definitive run with files creation')
    parser.add_argument('-o', '--overwrite', action="store_true",
                        help='overwrite existing output')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    return parser.parse_args(args)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    
    file = get_arg(args, 'config', 'streaming_lra.yaml')
    print('Reading configuration yaml file..')

    config = load_yaml(file)

    # model setup
    resolution = config['target']['resolution']
    frequency = config['target']['frequency']
    outdir = config['target']['outdir']
    tmpdir = config['target']['tmpdir']
    configdir = config['configdir']


    #opa setup
    opadir =  config['opa']['opadir']
    opacheckpoint =  config['opa']['opacheckpoint']

    # gsv setup
    use_gsv =  config['gsv']['use_gsv']
    gsv_start =  str(config['gsv']['start'])
    gsv_end =  str(config['gsv']['end'])

    # configuration of the tool
    definitive = get_arg(args, 'definitive', False)
    overwrite = get_arg(args, 'overwrite', False)
    workers = get_arg(args, 'workers', 1)
    loglevel= config['loglevel']
    loglevel = get_arg(args, 'loglevel', loglevel)

    for model in config['catalog'].keys():
        for exp in config['catalog'][model].keys():
            for source in config['catalog'][model][exp].keys():
                variables = config['catalog'][model][exp][source]['vars']
                for varname in variables:

                    # get the zoom level
                    zoom_level = config['catalog'][model][exp][source].get('zoom', None)
                    # init the OPA
                    opa = OPAgenerator(model=model, exp=exp, source=source, zoom=zoom_level,
                                        var=varname, frequency=frequency, checkpoint = opacheckpoint, stream_step=None,
                                        outdir=opadir, tmpdir=tmpdir, configdir=configdir,
                                        loglevel=loglevel, definitive=definitive, nproc=workers)
                    opa.retrieve()
                    opa.generate_opa(gsv=use_gsv, start=gsv_start, end=gsv_end)
                    opa.create_catalog_entry()

    
                # LRA will run only if NetCDF files from OPA are found
                for varname in variables:
                    opa_files = glob(f"{opa.outdir}/*{varname}*.nc")

                    if opa_files: 
                        print('Netcdf files found in %s: Launching LRA', opa.outdir)
                        # init the LRA
                        lra = LRAgenerator(model=model, exp=exp, source=opa.entry_name, zoom=zoom_level,
                                            var=varname, resolution=resolution,
                                            frequency=frequency, fix=False,
                                            outdir=outdir, tmpdir=tmpdir, configdir=configdir,
                                            nproc=workers, loglevel=loglevel,
                                            definitive=definitive, overwrite=overwrite)
                        
                        # check that your LRA is not already there (it will not work in streaming mode)
                        #check = lra.check_integrity(varname)

                        lra.retrieve()
                        lra.generate_lra()
                        lra.create_catalog_entry()
                
                # cleaning the opa NetCDF files
                for varname in variables:
                    opa_files = glob(f"{opa.outdir}/*{varname}*.nc")
                    for file_name in opa_files:
                        os.remove(file_name)

    print('LRA run completed. Have yourself a beer!')
