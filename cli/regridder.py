#!/usr/bin/env python3

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA regridding tool demo to create low resolution archive
This tool implements regridding of required files from the intake catalog
using precomputed weights and sparse-array multiplication using smmregrid package 
included in AQUA. Functionality can be controlled through CLI options.
'''
import sys
sys.path.append("..")

import argparse
from aqua import Reader
import dask
from dask.distributed import Client, LocalCluster
from time import time
import os
import logging

# default configurations for testing
ddir= '/home/b/b382076/work/lowres2'
dmodel = 'IFS'
dexp = 'tco2559-ng5'
dres = 'r100'
dfreq = 'day'

# temporary hardcoded
varlist=['tp', '2t']


def main(modelname, expname, resolution, frequency, varlist, outdir):
    """
    Function to produce interpolation to disk of a selected variables
    """

    lowdir = os.path.join(outdir, f'{expname}-{resolution}-{frequency}')
    os.makedirs(lowdir, exist_ok=True)
    logging.info(f'Target directory is {lowdir}')

    logging.info(f'Accessing catalog...')
    reader = Reader(model=modelname, exp=expname, 
        regrid=resolution, freq=frequency, source="ICMGG_atm2d", configdir="../config") # this is hardcoded
    data = reader.retrieve(fix=False)
    for var in varlist:
        tic = time()

        logging.info(f'Processing variable {var}...')
        averaged = reader.average(data[var])
        interp = reader.regrid(averaged) # here we can apply time selection, for instance if we want to process one year at the time
        logging.info(f'Output will have shape {interp.shape}')
        logging.info(f'From {min(interp.time.data)} to {max(interp.time.data)}')
        

        logging.info(f'Writing it to disk...')
        outname = os.path.join(lowdir, f'{var}_{expname}.nc')
        
        # we can of course extend this and also save to grib
        interp.to_netcdf(outname)

        toc = time()
        logging.info('Done in {:.4f} seconds'.format(toc - tic))


def parse_arguments(args):
    """Parse CLI arguments"""

    parser = argparse.ArgumentParser(description='AQUA regridder')
    parser.add_argument('-o', '--outdir', type=str, help='output directory')
    parser.add_argument('-m', '--model', type=str, help='input model')
    parser.add_argument('-e', '--exp', type=str, help='input experiment')
    parser.add_argument('-r', '--resolution', type=str, help='target resolution')
    parser.add_argument('-w', '--workers', type=str, help='number of dask workers')
    parser.add_argument('-f', '--frequency', type=str, help='output frequency for averaging')
    #parser.add_argument('-v', '--var', type=str, help='select variable')

    return parser.parse_args(args)

def get_arg(args, arg, default):
    """Aider function to get arguments"""

    res = getattr(args, arg)
    if not res:
        res = default
    return res


# setting up dask
if __name__ == "__main__":

    args = parse_arguments(sys.argv[1:])

    # assign from parsed
    model = get_arg(args, 'model', dmodel)
    exp = get_arg(args, 'exp', dexp)
    resolution = get_arg(args, 'resolution', dres)
    outdir = get_arg(args, 'outdir', ddir)
    frequency = get_arg(args, 'frequency', dfreq)
    workers = int(get_arg(args, 'workers', 1))

    # set default loglevel
    logging.basicConfig(level='INFO')

    # set up clusters if more than workers is provided
    if workers > 1: 
        cluster = LocalCluster(threads_per_worker=1, n_workers=workers)
        client = Client(cluster)
    else: 
        dask.config.set(scheduler="synchronous")

    # call the function
    main(modelname=model, expname=exp, resolution=resolution,
        frequency=frequency, varlist=varlist, outdir=outdir)
    
    logging.info('Everything completed... goodbye!')
