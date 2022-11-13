#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
 AQUA regridding tool
'''

import sys
import xarray as xr
import dask
from aqua import regrid
from glob import glob
import os
import yaml
import argparse
from pathlib import PurePath, Path

dask.config.set(scheduler="synchronous")

def parse_arguments(args):
    """Parse CLI arguments"""

    parser = argparse.ArgumentParser(description='AQUA regridder')
    parser.add_argument('-i', '--indir', type=str, help='input directory')
    parser.add_argument('-o', '--outdir', type=str, help='output directory')
    parser.add_argument('-c', '--config', type=str, help='config yaml file', default='config.yaml')
    parser.add_argument('-w', '--weights', type=str, help='weights file')
    parser.add_argument('-m', '--model', type=str, help='specify input model')
    parser.add_argument('-v', '--var', type=str, help='select variable')
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
    res = getattr(args, arg)
    if not res:
        res = default
    return res


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
    in_dir = get_arg(args, 'indir', cfg['input']['dir'])
    out_dir = get_arg(args, 'outdir', cfg['output']['dir'])
    weights_fn = get_arg(args, 'weights', cfg['models'][model]['weights'])
    var = get_arg(args, 'var', '')

    # we use precomputed weights for now
    weights = xr.open_mfdataset(weights_fn)

    regridder = regrid.Regridder(weights=weights)
    
    for fn in glob(os.path.join(in_dir, '*.nc')):
        src_ds = xr.open_mfdataset(fn)
        if var:
            vars = [var]
        else:
            vars = list(src_ds.data_vars)
        for vv in vars:
            print("Regridding " + fn + f" variable {vv}")
            out_fn = os.path.join(out_dir, vv + '_' + os.path.relpath(fn, in_dir))
            if not os.path.isfile(out_fn):
                out_ds = regridder.regrid(src_ds[vv])
                out_fn = os.path.join(out_dir, vv + '_' + os.path.relpath(fn, in_dir))
                out_ds.to_netcdf(out_fn, format="NETCDF4", mode="w")

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))