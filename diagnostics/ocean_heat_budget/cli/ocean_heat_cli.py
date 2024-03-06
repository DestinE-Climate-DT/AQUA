#!/usr/bin/env python3
"""
Command-line interface for global ocean heat budget time series diagnostic.

This CLI allows to plot timeseries of a set of variables
defined in a yaml configuration file for a single experiment
and gregory plot.
"""
import os
import sys
import argparse
from aqua import Reader
from aqua.util import load_yaml, get_arg, create_folder
from aqua.logger import log_configure
import numpy as np

# include functions/utilities from the diagnostics/ocean_heat_budget directory
sys.path.insert(0, "../")
from ocean_heat_functions import compute_net_surface_fluxes, plot_time_series


def parse_arguments(args):
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Global time series CLI")

    parser.add_argument("-c", "--config",
                        type=str, required=False,
                        help="yaml configuration file")
    parser.add_argument("--loglevel", "-l", type=str,
                        required=False, help="loglevel")

    # These will override the ones in the config file if provided
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source_atm", type=str,
                        required=False, help="source name for atmosphere")
    parser.add_argument("--source_oc", type=str,
                        required=False, help="source name for ocean")
    
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")

    return parser.parse_args(args)


def create_filename(outputdir=None, plotname=None, type=None,
                    model=None, exp=None, source=None, resample=None):
    """
    Create a filename for the plots

    Args:
        outputdir (str): output directory
        plotname (str): plot name
        type (str): type of output file (pdf or nc)
        model (str): model name
        exp (str): experiment name
        source (str): source name
        resample (str): resample frequency

    Returns:
        filename (str): filename

    Raises:
        ValueError: if no output directory is provided
        ValueError: if no plotname is provided
        ValueError: if type is not pdf or nc
    """
    if outputdir is None:
        print("No output directory provided, using current directory.")
        outputdir = "."

    if plotname is None:
        raise ValueError("No plotname provided.")

    if type != "pdf" and type != "nc":
        raise ValueError("Type must be pdf or nc.")

    diagnostic = "ocean_heat_budget_time_series"
    filename = f"{diagnostic}"
    filename += f"_{model}_{exp}_{source}"
    filename += f"_{plotname}"

    if resample == 'YS':
        filename += "_annual"

    if type == "pdf":
        filename += ".pdf"
    elif type == "nc":
        filename += ".nc"

    return filename

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    # Loglevel settings
    loglevel = get_arg(args, 'loglevel', 'WARNING')

    logger = log_configure(log_level=loglevel,
                           log_name='Ocean Heat Budget Time Series')

    logger.info('Running Ocean Heat Budget time series diagnostic...')

    # we change the current directory to the one of the CLI
    # so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')

    # Read configuration file
    file = get_arg(args, 'config', 'config_ocean_heat_budget.yaml')
    logger.info(f"Reading configuration yaml file: {file}")
    config = load_yaml(file)

    models = config['models']
    models['model'] = get_arg(args, 'model', models['model'])
    models['exp'] = get_arg(args, 'exp', models['exp'])
    models['source_atm'] = get_arg(args, 'source_atm', models['source_atm'])
    models['source_oc'] = get_arg(args, 'source_oc', models['source_oc'])

    startdate=config['startdate']
    enddate=config['enddate']
    regrid=config['regrid']

    logger.debug("Analyzing models:")
    models_list = []
    exp_list = []
    source_list_atm = []
    source_list_oc = []

    for model in models:
        logger.debug(f"  - {model['model']} {model['exp']} {model['source']}")
        models_list.append(model['model'])
        exp_list.append(model['exp'])
        source_list_atm.append(model['source_atm'])
        source_list_oc.append(model['source_oc'])

    outputdir = get_arg(args, 'outputdir', config['outputdir'])
    logger.debug(f"outputdir: {outputdir}")

    outputdir_nc = os.path.join(outputdir, "netcdf")
    create_folder(folder=outputdir_nc, loglevel=loglevel)
    outputdir_pdf = os.path.join(outputdir, "pdf")
    create_folder(folder=outputdir_pdf, loglevel=loglevel)

    #code to compute ocean heat budget time series starts here

    reader_atm = Reader(model=model, exp=exp_list, source=source_list_atm, startdate=startdate, enddate=enddate, regrid="r010")
    data_atm = reader_atm.retrieve(var=['mslhf','msnlwrf','msnswrf','msshf'])
    data_atm = reader_atm.timmean(data_atm, freq="daily")
    data_atm = reader_atm.regrid(data_atm)

    reader_oc = Reader(model=model, exp=exp_list, source=source_list_oc, startdate=startdate, enddate=enddate, regrid=regrid)
    data_oc = reader_oc.retrieve(var=["avg_tos", "avg_hc700m"])
    #fai lo stesso time mean
    data_oc = reader_oc.regrid(data_oc)

    #computes net surface fluxes at the ocean surface including land sea mask
    net_surface_fluxes, mask = compute_net_surface_fluxes(data_atm, data_oc)
    # compute the time series of the net surface fluxes
    net_surface_fluxes = reader_atm.fldmean(net_surface_fluxes)

    # get the heat content of the 700m ocean layer and perform spatial averaging
    avg_hc700m = data_oc['avg_hc700m']
    avg_hc700m=reader_oc.fldmean(avg_hc700m)
    # compute time derivative of the heat content
    time_diff = np.diff(avg_hc700m.time.values, axis=0) / np.timedelta64(1, 's')
    avg_hc_time_derivative = np.diff(avg_hc700m, axis=0) / time_diff

    # now plot the time series
    title_args = {'model': model, 'exp': exp, 'source': source_atm}
    plot_time_series(net_surface_fluxes, avg_hc_time_derivative, title_args, var1_label="Net OSF [W m**2]",  var2_label="HC700m time derivative [W m**2]")