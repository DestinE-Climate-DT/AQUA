#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA regridding tool to create low resolution archive
This tool implements regridding of required files from the intake catalog
using precomputed weights and sparse-array multiplication using smmregrid package 
included in AQUA. Functionality can be controlled through CLI options and
a configuration yaml file.
'''
import sys
sys.path.append("..")

import argparse
from aqua import Reader
from aqua.util import load_yaml
import dask
from dask.distributed import Client, LocalCluster, progress
from dask.diagnostics import ProgressBar
from time import time
import os
import logging


def lra(modelname, expname, sourcename, 
        resolution, frequency, varlist, outdir, 
        definitive=False, overwrite=False, multi=False):
    """
    Produce LRA data at required frequency/resolution

    Args:
        modelname (string): The model name from the catalog
        expname (string): The experiment name from the catalog
        sourcename (string): The sourceid name from the catalog
        resolution (string): The target resolution for the LRA
        frequency (string): The target frequency for averaging the LRA
        varlist (list): The list fo variables to be processed and archived in LRA
        outdir (string): Where the LRA is
        definitive (bool): True to create the output file, False to just explore the reader operations
        overwrite (bool): True to overwrite existing files in LRA
        multi (bool): True if you are running dask.distributed with multiple workers

    """

    if not definitive:
        logging.info('IMPORTANT: no file will be created, this is a dry run')

    logging.warning(f'Accessing catalog for {modelname}-{expname}-{sourcename}...')
    logging.warning(f'I am going to produce LRA at {resolution} resolution and {frequency} frequency...')

    lowdir = os.path.join(outdir, expname, resolution, frequency)
    os.makedirs(lowdir, exist_ok=True)
    logging.info(f'Target directory is {lowdir}')

    # start the reader
    reader = Reader(model=modelname, exp=expname, source=sourcename,
                    regrid=resolution, freq=frequency, configdir="../config") # this is hardcoded
    
    logging.info('Retrieving data...')
    data = reader.retrieve(fix=False)
    logging.debug(data)

    # option for time encoding, defined once for all
    time_encoding = {'units': 'days since 1970-01-01',
                 'calendar': 'standard',
                 'dtype': 'float64'}

    for var in varlist:
        tic = time()

        logging.info(f'Processing variable {var}...')

        # decumulate
        #decum = reader.decumulate(data[var])

        # time average
        averaged = reader.average(data[var]) # here level selection can be applied

        # here we can apply time selection, for instance if we want to process one year at the time
        interp = reader.regrid(averaged)
    
        logging.info(f'Output will have shape {interp.shape}')
        logging.info(f'From {min(interp.time.data)} to {max(interp.time.data)}')
        logging.debug(interp)

        # USEFUL: if you want to do tests subselect time to speed up...
        #interp = interp.isel(time=0)
            
        # we can of course extend this and also save to grib
        if definitive:
        
            outname = os.path.join(lowdir, f'{var}_{expname}_{resolution}_{frequency}.nc')
            if os.path.isfile(outname) and not overwrite:
                logging.warning(f'{outname} already exists. Please run with -o to allow overwriting')
            else:
                # extra clean
                if os.path.exists(outname): 
                    os.remove(outname)
                logging.warning(f'Writing {var} to {outname}...')

                # keep it lazy
                write_job = interp.to_netcdf(outname, encoding={'time': time_encoding}, compute=False)
                
                # progress bar for distributed or for for singlemachine
                if multi:
                    w = write_job.persist() 
                    progress(w)
                else:
                    with ProgressBar():
                        write_job.compute()
    
        toc = time()
        logging.info('Done in {:.4f} seconds'.format(toc - tic))


def parse_arguments(args):
    """Parse CLI arguments"""

    parser = argparse.ArgumentParser(description='AQUA regridder')
    parser.add_argument('-c', '--config', type=str, help='yaml configuration file')
    parser.add_argument('-w', '--workers', type=str, help='number of dask workers')
    parser.add_argument('-d', '--definitive', action="store_true", help='run the code to create the output files')
    parser.add_argument('-f', '--frequency', type=str, help='frequency to be obtained')
    parser.add_argument('-r', '--resolution', type=str, help='resolution to be obtained')
    parser.add_argument('-o', '--overwrite', action="store_true", help='overwrite existing output')
    parser.add_argument('-l', '--loglevel', type=str, help='overwrite existing output')

    return parser.parse_args(args)

def get_arg(args, arg, default):
    """Aider function to get arguments"""

    res = getattr(args, arg)
    if not res:
        res = default
    return res

# setting up dask
if __name__ == "__main__":

    """Main for command line execution of the regridder"""

    args = parse_arguments(sys.argv[1:])

    #assign from parsed
    workers = int(get_arg(args, 'workers', 1))
    config = get_arg(args, 'config', 'config_lra.yml')
    loglevel = get_arg(args, 'loglevel', 'INFO')
    if args.definitive :
        definitive=True
    else:
        definitive=False
    if args.overwrite :
        overwrite=True
    else:
        overwrite=False
    
    # set default loglevel
    logging.basicConfig(level=loglevel.upper())

    cfg = load_yaml(config)
    resolution = get_arg(args, 'resolution', cfg['target']['resolution'])
    frequency = get_arg(args, 'frequency', cfg['target']['frequency'])
    outdir = cfg['target']['outdir']

    # loop on all the elements of the catalog
    for model in cfg['catalog'].keys():
        for exp in cfg['catalog'][model].keys():
            for sourceid in cfg['catalog'][model][exp].keys():
                varlist = cfg['catalog'][model][exp][sourceid]['vars']
                if varlist: 

                    # set up clusters if more than workers is provided
                    if workers > 1: 
                        cluster = LocalCluster(threads_per_worker=1, n_workers=workers)
                        #dask.config.set({'temporary_directory': cfg['target']['outdir']})
                        client = Client(cluster)
                        multi=True
                    else: 
                        dask.config.set(scheduler="synchronous")
                        multi=False

                    # call the function
                    lra(modelname=model, expname=exp, sourcename=sourceid, resolution=resolution,
                        frequency=frequency, varlist=varlist, outdir=outdir, 
                        definitive=definitive, overwrite=overwrite, multi=multi)
                    
                    if workers>1:
                        client.close()
                        cluster.close()

    logging.info('Everything completed... goodbye!')

