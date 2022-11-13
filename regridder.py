#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA regridding tool

This tool implements regridding of all files in a target directory
using precomputed weights and sparse-array multiplication.
Functionality can be controlled either through a yaml file or CLI options.
Precomputed weights for the intended pairs (source model/grid : target grid)
are used. Functionality to compute the weights will be added to the tool
but also externally produced ESMF or CDO weights can be used.
If weights are not available the tool will attempt to generate them.
'''

__author__ = "Jost von Hardenberg"
__email__ = "jost.hardenberg@polito.it"
__version__ = "0.1.0"


import argparse
import os
import sys
from glob import glob
from pathlib import Path

import dask
import xarray as xr
import yaml

from aqua import regrid

dask.config.set(scheduler="synchronous")


def parse_arguments(args):
    """Parse CLI arguments"""

    parser = argparse.ArgumentParser(description='AQUA regridder')
    parser.add_argument('-i', '--indir', type=str, help='input directory')
    parser.add_argument('-o', '--outdir', type=str, help='output directory')
    parser.add_argument('-c', '--config', type=str, help='config yaml file',
                        default='config.yaml')
    parser.add_argument('-w', '--weights', type=str, help='weights file')
    parser.add_argument('-m', '--model', type=str, help='input model')
    parser.add_argument('-r', '--resolution', type=str, help='target resolution')
    parser.add_argument('-t', '--method', type=str, help='interpolation method')
    parser.add_argument('-v', '--var', type=str, help='select variable')
    parser.add_argument('-g', '--generate', action='store_true', help='generate weights only, do not actually run')
    return parser.parse_args(args)


def load_yaml(infile):
    """Load generic yaml file"""

    try:
        with open(infile, 'r', encoding='utf-8') as file:
            cfg = yaml.load(file, Loader=yaml.FullLoader)
    except IOError:
        sys.exit(f'{infile} not found: you need to have this configuration file!')
    return cfg


def get_arg(args, arg, default):
    """Aider function to get arguments"""

    res = getattr(args, arg)
    if not res:
        res = default
    return res


def mfopener(*args, format='netcdf', **kwargs):
    """Open file or set of files using open_mfdataset for different input formats"""

    if format == 'netcdf':
        ds = xr.open_mfdataset(*args, **kwargs)
    elif format == 'grib2':
        ds = xr.open_mfdataset(*args, engine='cfgrib',
                               backend_kwargs={'filter_by_keys': {'edition': 2}},
                               **kwargs)
    elif format == 'grib1':
        ds = xr.open_mfdataset(*args, engine='cfgrib',
                               backend_kwargs={'filter_by_keys': {'edition': 1}},
                               **kwargs)
    return ds


def expand_path(fn, **kwargs):
    """Expands a path (filename or dir) for var, expname, frequency, ensemble etc.
    and environment variables."""

    out = str(os.path.expandvars(fn)).format(**kwargs)
 #   print(f"template: {fn}")
 #   print(f"expanded: {out}")
    return out


def main(argv):
    """
    Tool to regrid all files in a directory.
    Skips output files which already exist.
    """

    args = parse_arguments(argv)

    indir = Path(os.path.dirname(os.path.abspath(__file__)))
    # config file (looks for it in the same dir as the .py program file
    if args.config:
        cfg = load_yaml(args.config)
    else:
        cfg = load_yaml(indir / 'config.yaml')

    model = get_arg(args, 'model', cfg['input']['model'])
    method = get_arg(args, 'method', cfg['output']['method'])
    resolution = get_arg(args, 'resolution', cfg['output']['resolution'])
    in_dir = get_arg(args, 'indir', cfg['input']['dir'])
    out_dir = get_arg(args, 'outdir', cfg['output']['dir'])
    weights_fn = get_arg(args, 'weights', cfg['models'][model]['weights'])
    var = get_arg(args, 'var', '')
    fgen = get_arg(args, 'generate', False)

    if var:
        filename_exp = expand_path(cfg['input']['filename'], var=var)
        in_dir_exp = expand_path(in_dir, var=var)
    else:
        filename_exp = expand_path(cfg['input']['filename'], var='*')
        in_dir_exp = expand_path(in_dir, var='*')

    path = os.path.join(in_dir_exp, filename_exp)

    weights_fn_exp = expand_path(weights_fn, model=model, method=method, resolution=resolution)
    # If weight file does exist try to generate it

    if not os.path.isfile(weights_fn_exp):
        print("file " + weights_fn_exp + " does not exist. Trying to generate ...")
        try:
            gridfile = cfg['models'][model].get('gridfile', glob(path)[0])
            if gridfile:
                weights_ds = regrid.cdo_generate_weights(gridfile, resolution, method=method,
                                                         extrapolate=True, remap_norm='fracarea',
                                                         remap_area_min=0.0)
                weights_ds.to_netcdf(weights_fn_exp)
        except BaseException:
            print("Weight generation failed!")
            sys.exit()
  
    if not fgen:
        weights = xr.open_mfdataset(weights_fn_exp)
        regridder = regrid.Regridder(weights=weights)

        for fn in glob(path):
            src_ds = mfopener(fn, format=cfg['models'][model]['format'])
            if var:
                vars = [var]
            else:
                vars = list(src_ds.data_vars)
                vars = [x for x in vars if x not in ['time_bnds', 'lat_bnds', 'lon_bnds', 'vertices_latitude', 'vertices_longitude']]
            out_ds_list = []
            out_fn_list = []
            fnr = str(Path(os.path.relpath(fn, in_dir_exp)).with_suffix(''))
    #       print(f"fnr: {fnr}")
            for vv in vars:
                out_dir_exp = expand_path(out_dir, model=model, var=vv, resolution=resolution)
                os.makedirs(out_dir_exp, exist_ok=True)
                out_fn = expand_path(cfg['output']['filename'], fnr=fnr, var=vv, model=model)
                out_fn = os.path.join(out_dir_exp, out_fn)
                if not os.path.isfile(out_fn):
                    out_ds_list.append(regridder.regrid(src_ds.get([vv])))
                    out_fn_list.append(out_fn)
                    print(f"Regridding variable {vv} from {fn} to {out_fn}")
            if out_fn_list:
                xr.save_mfdataset(out_ds_list, out_fn_list, mode='w', format="NETCDF4")


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
