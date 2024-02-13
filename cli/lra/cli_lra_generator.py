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
from aqua.util import load_yaml, get_arg
from aqua import __version__ as version

# to check if GSV is available and return the version
try:
    import gsv
    print('GSV version is: ' + gsv.__version__)
except RuntimeError:
    print("GSV not available. FDB5 binary library not present on system.")
except KeyError:
    print("GSV not available. Environment variables not correctly set.")

print('AQUA version is: ' + version)


def parse_arguments(arguments):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='AQUA LRA generator')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-f', '--fix', action="store_true",
                        help='fixer on existing data')
    parser.add_argument('-w', '--workers', type=str,
                        help='number of dask workers')
    parser.add_argument('-d', '--definitive', action="store_true",
                        help='definitive run with files creation')
    parser.add_argument('-o', '--overwrite', action="store_true",
                        help='overwrite existing output')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    return parser.parse_args(arguments)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    file = get_arg(args, 'config', 'lra_config.yaml')
    print('Reading configuration yaml file..')

    config = load_yaml(file)
    resolution = config['target']['resolution']
    frequency = config['target']['frequency']
    outdir = config['target']['outdir']
    tmpdir = config['target']['tmpdir']
    configdir = config['configdir']
    loglevel = config['loglevel']

    definitive = get_arg(args, 'definitive', False)
    overwrite = get_arg(args, 'overwrite', False)
    fix = get_arg(args, 'fix', True)
    workers = get_arg(args, 'workers', 1)
    loglevel = get_arg(args, 'loglevel', loglevel)

    for model in config['catalog'].keys():
        for exp in config['catalog'][model].keys():
            for source in config['catalog'][model][exp].keys():
                for varname in config['catalog'][model][exp][source]['vars']:

                    # get the zoom level
                    zoom_level = config['catalog'][model][exp][source].get('zoom', None)

                    # init the LRA
                    lra = LRAgenerator(model=model, exp=exp, source=source, zoom=zoom_level,
                                       var=varname, resolution=resolution,
                                       frequency=frequency, fix=fix,
                                       outdir=outdir, tmpdir=tmpdir, configdir=configdir,
                                       nproc=workers, loglevel=loglevel,
                                       definitive=definitive, overwrite=overwrite,
                                       exclude_incomplete=True)

                    # check that your LRA is not already there (it will not work in streaming mode)
                    lra.check_integrity(varname)

                    # retrieve and generate
                    lra.retrieve()
                    lra.generate_lra()
                    
        # create the catalog once the loop is over
        lra.create_catalog_entry()

    print('LRA run completed. Have yourself a beer!')
