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
from aqua.lra_generator.lra_util import opa_catalog_entry
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

    file = get_arg(args, 'config', 'only_lra.yaml')
    print('Reading configuration yaml file..')

    config = load_yaml(file)

    # model setup
    resolution = config['target']['resolution']
    frequency = config['target']['frequency']
    fixer_name = config['target']['fixer_name']
    outdir = config['target']['outdir']
    tmpdir = config['target']['tmpdir']
    opadir = config['target']['opadir']
    configdir = config['configdir']

    # configuration of the tool
    definitive = get_arg(args, 'definitive', False)
    overwrite = get_arg(args, 'overwrite', False)
    workers = get_arg(args, 'workers', 1)
    loglevel = config['loglevel']
    loglevel = get_arg(args, 'loglevel', loglevel)

    for model in config['catalog'].keys():
        for exp in config['catalog'][model].keys():
            source = f'lra-{resolution}-{frequency}'
            variables = config['catalog'][model][exp][source]['vars']
            print(f'LRA Processing {model}-{exp}-opa-{frequency}')

            # update the dir
            #opadir = os.path.join(opadir, model, exp, frequency)
            # check if files are there
            opa_files = glob(f"{opadir}/*{frequency}*.nc")
            if opa_files:
                for varname in variables:

                    # create the catalog entry
                    entry_name = opa_catalog_entry(datadir=opadir, model=model, source=source,
                                                exp=exp, frequency=frequency, 
                                                fixer_name=fixer_name, loglevel=loglevel)

                    print(f'Netcdf files found in {opadir}: Launching LRA')

                    # init the LRA
                    # zoom_level = config['catalog'][model][exp][source].get('zoom', None)
                    lra = LRAgenerator(model=model, exp=exp, source=entry_name, zoom=None,
                                    var=varname, resolution=resolution,
                                    frequency=frequency, fix=True,
                                    outdir=outdir, tmpdir=tmpdir, configdir=configdir,
                                    nproc=workers, loglevel=loglevel,
                                    definitive=definitive, overwrite=overwrite)

                    lra.retrieve()
                    lra.generate_lra()
                    lra.create_catalog_entry()

                # cleaning the opa NetCDF files
                # for varname in variables:
                #    for file_name in opa_files:
                #        os.remove(file_name)
            else:
                print(f'There are no Netcdf files in {opadir}')

    print('LRA run completed. Have yourself a beer!')
