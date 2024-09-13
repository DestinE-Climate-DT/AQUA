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
from glob import glob
from aqua import LRAgenerator
from aqua.util import load_yaml, get_arg, to_list
from aqua.lra_generator.lra_util import opa_catalog_entry
from aqua import __version__ as version


def lra_parser(parser = None):
    """
    Parse command line arguments for the LRA CLI

    Args:
        Optional part to be extended with LRA options
    """

    if parser is None:
        parser = argparse.ArgumentParser(description='AQUA LRA generator')
    
    parser.add_argument('-a', '--autosubmit', action="store_true", 
                        help='Run the LRA with OPA through autosubmit')
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
    parser.add_argument('--monitoring', action="store_true",
                        help='enable the dask performance monitoring. Will run a single chunk')
    parser.add_argument('--only-catalog', action="store_true",
                        help='To not run the LRA but simply create the catalog entries for netcdf and zarr')
    parser.add_argument('--catalog', type=str,
                        help='catalog to be processed. Use with coherence with --model, -exp and --source')
    parser.add_argument('-m', '--model', type=str,
                        help='model to be processed. Use with coherence with --exp and --source')
    parser.add_argument('-e', '--exp', type=str,
                        help='experiment to be processed. Use with coherence with --source and --model')
    parser.add_argument('-s', '--source', type=str,
                        help='source to be processed. Use with coherence with --exp and --var')
    parser.add_argument('-v', '--var', type=str,
                        help='var to be processed. Use with coherence with --source')

    #return parser.parse_args(arguments)
    return parser

def lra_execute(args):
    """
    Executing the LRA by parsing the arguments and configuring the machinery
    """

        # to check if GSV is available and return the version
    try:
        import gsv
        print('GSV version is: ' + gsv.__version__)
    except RuntimeError:
        print("GSV not available. FDB5 binary library not present on system.")
    except KeyError:
        print("GSV not available. Environment variables not correctly set.")

    print('AQUA version is: ' + version)

    file = get_arg(args, 'config', 'lra_config.yaml')
    print('Reading configuration yaml file..')

    # basic from configuration
    config = load_yaml(file)

    #safety check
    for item in ['target', 'paths', 'data', 'options']:
        if not item in config:
            raise KeyError(f'Configuration file {file} does not have the "{item}" key, please modify it according to the template')

    # mandatory arguments
    resolution = config['target']['resolution']
    frequency = config['target']['frequency']
    paths = config['paths']
    outdir = paths['outdir']
    tmpdir = paths['tmpdir']

    # optional main catalog switch
    catalog = config['target'].get('catalog', None)

    # for autosubmit workflow
    opadir = paths.get('opadir', None)
    fixer_name = config['target'].get('fixer_name', None)
    #configdir = config.get('configdir', None) #is this really used?

    # options
    loglevel = config['options'].get('loglevel', 'WARNING')
    do_zarr = config['options'].get('zarr', False)
    verify_zarr = config['options'].get('verify_zarr', False)

    # command line override
    definitive = get_arg(args, 'definitive', False)
    monitoring = get_arg(args, 'monitoring', False)
    overwrite = get_arg(args, 'overwrite', False)
    #only_catalog = get_arg(args, 'only_catalog', False)  # option not used yet
    fix = get_arg(args, 'fix', True)
    default_workers = get_arg(args, 'workers', 1)
    loglevel = get_arg(args, 'loglevel', loglevel)

    # run the LRA through the workflow, based on OPA
    # please notice that calls are very similar but to preserve previous structure two funcionts are implemented
    if get_arg(args, 'autosubmit', False):
        lra_autosubmit(args=args, config=config, catalog=catalog, resolution=resolution, 
                       frequency=frequency, fix=fix,  
                       outdir=outdir, tmpdir=tmpdir, loglevel=loglevel,
                       definitive=definitive, overwrite=overwrite, default_workers=default_workers,
                       opadir=opadir, fixer_name=fixer_name)
    # default run of the LRA
    else:
        lra_cli(args=args, config=config, catalog=catalog, resolution=resolution, 
                frequency=frequency, fix=fix,
                outdir=outdir, tmpdir=tmpdir, loglevel=loglevel,
                definitive=definitive, overwrite=overwrite, default_workers=default_workers,
                monitoring=monitoring, do_zarr=do_zarr, verify_zarr=verify_zarr)

def lra_cli(args, config, catalog, resolution, frequency, fix, outdir, tmpdir, loglevel,
            definitive, overwrite, monitoring, default_workers, do_zarr, verify_zarr):
    """
    Running the default LRA from CLI, looping on all the configuration model/exp/source/var combination
    Options for dry run and overwriting, as well as monitoring and zarr creation, are available
    """

    models = to_list(get_arg(args, 'model', config['data'].keys()))
    for model in models:
        exps = to_list(get_arg(args, 'exp', config['data'][model].keys()))
        for exp in exps:
            sources = to_list(get_arg(args, 'source', config['data'][model][exp].keys()))
            for source in sources:
                varnames = to_list(get_arg(args, 'var', config['data'][model][exp][source]['vars']))
                for varname in varnames:

                    # get the zoom level - this might need some tuning for extra kwargs
                    extra_args = {}
                    zoom = config['data'][model][exp][source].get('zoom', None)
                    if zoom is not None:
                        extra_args = {**extra_args, **{'zoom': zoom}}

                    # get the number of workers for this specific configuration
                    workers = config['data'][model][exp][source].get('workers', default_workers)

                    # init the LRA
                    lra = LRAgenerator(catalog=catalog, model=model, exp=exp, source=source,
                                       var=varname, resolution=resolution,
                                       frequency=frequency, fix=fix,
                                       outdir=outdir, tmpdir=tmpdir,
                                       nproc=workers, loglevel=loglevel,
                                       definitive=definitive, overwrite=overwrite,
                                       performance_reporting=monitoring,
                                       exclude_incomplete=True,
                                       **extra_args)

                    # check that your LRA is not already there (it will not work in streaming mode)
                    lra.check_integrity(varname)

                    # retrieve and generate
                    lra.retrieve()
                    lra.generate_lra()

            # create the catalog once the loop is over
            lra.create_catalog_entry()
            if do_zarr:
                lra.create_zarr_entry(verify=verify_zarr)

    print('CLI LRA run completed. Have yourself a pint of beer!')

def lra_autosubmit(config, catalog, resolution, frequency, fix, fixer_name, 
                   outdir, tmpdir, opadir, loglevel, definitive, overwrite, default_workers):
    
    """
    If the --autosubmit flag is selected, run the LRA via the workflow assuming OPA files 
    have been build beforehand 
    """

    models = to_list(get_arg(args, 'model', config['data'].keys()))
    for model in models:
        exps = to_list(get_arg(args, 'exp', config['data'][model].keys()))
        for exp in exps:
            source = f'lra-{resolution}-{frequency}'
            variables = config['data'][model][exp][source]['vars']
            print(f'LRA Processing {model}-{exp}-opa-{frequency}')

            # update the dir
            #opadir = os.path.join(opadir, model, exp, frequency)
            # check if files are there
            opa_files = glob(f"{opadir}/*{frequency}*.nc")
            if opa_files:
                for varname in variables:

                    # create the catalog entry
                    entry_name = opa_catalog_entry(datadir=opadir, catalog=catalog, model=model, source=source,
                                                exp=exp, frequency=frequency, 
                                                fixer_name=fixer_name, loglevel=loglevel)

                    print(f'Netcdf files found in {opadir}: Launching LRA')

                    # init the LRA
                    # init the LRA
                    lra = LRAgenerator(catalog=catalog, model=model, exp=exp, source=entry_name,
                                       var=varname, resolution=resolution,
                                       frequency=frequency, fix=fix,
                                       outdir=outdir, tmpdir=tmpdir,
                                       nproc=default_workers, loglevel=loglevel,
                                       definitive=definitive, overwrite=overwrite,
                                       performance_reporting=False,
                                       exclude_incomplete=True)

                    lra.retrieve()
                    lra.generate_lra()
                    lra.create_catalog_entry()

                # cleaning the opa NetCDF files
                # for varname in variables:
                #    for file_name in opa_files:
                #        os.remove(file_name)
            else:
                print(f'There are no Netcdf files in {opadir}')

    print('Workflow LRA run completed. Have yourself a large beer!')


# if you want to execute the script from terminal without the aqua entry point
if __name__ == '__main__':

    args = lra_parser().parse_args(sys.argv[1:])
    lra_execute(args)
   
