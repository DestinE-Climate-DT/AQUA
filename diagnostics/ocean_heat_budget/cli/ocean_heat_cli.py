#!/usr/bin/env python3
"""
Command-line interface for global ocean heat budget time series diagnostic.

This CLI allows to plot timeseries of the net surface fluxes over the ocean and
the time derivative of the ocean heat content.
See config details in the dedicated yaml configuration file for model, experiment, and different sources.
"""
import os
import sys
import argparse
from aqua import Reader
from aqua.util import load_yaml, get_arg, create_folder
from aqua.logger import log_configure
from dask.distributed import Client, LocalCluster

# include functions/utilities from the diagnostics/ocean_heat_budget directory
sys.path.insert(0, "../")
from ocean_heat_functions_cli import compute_net_surface_fluxes, plot_time_series, plot_difference


def parse_arguments(args):
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Ocean Heat Content time series CLI")

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

    model = get_arg(args, 'model', config['model'])
    exp = get_arg(args, 'exp', config['exp'])
    source_atm = get_arg(args, 'source_atm', config['source_atm'])
    source_oc = get_arg(args, 'source_oc', config['source_oc'])

    startdate=config['startdate']
    enddate=config['enddate']

    logger.debug(f"Analyzing {model}, {exp}, atm src: {source_atm}, oc src: {source_oc}")


    outputdir = get_arg(args, 'outputdir', config['outputdir'])
    logger.debug(f"outputdir: {outputdir}")

    #outputdir_nc = os.path.join(outputdir, "netcdf")
    #create_folder(folder=outputdir_nc, loglevel=loglevel)
    outputdir_pdf = os.path.join(outputdir, "pdf")
    create_folder(folder=outputdir_pdf, loglevel=loglevel)

    #code to compute ocean heat budget time series starts here
    cluster = LocalCluster(n_workers=4, threads_per_worker=1)
    client = Client(cluster)

    reader_atm = Reader(model=model, exp=exp, source=source_atm, startdate=startdate, enddate=enddate)
    data_atm = reader_atm.retrieve(var=['mslhf','msnlwrf','msnswrf','msshf',"sf"])
    data_atm = reader_atm.timmean(data_atm, freq="daily")

    reader_oc = Reader(model=model, exp=exp, source=source_oc, startdate=startdate, enddate=enddate)
    data_oc = reader_oc.retrieve(var=["avg_tos", "avg_hc700m"])

    #computes net surface fluxes at the ocean surface including land sea mask
    net_surface_fluxes, mask = compute_net_surface_fluxes(data_atm, data_oc)
    # compute the time series of the net surface fluxes
    net_surface_fluxes = reader_atm.fldmean(net_surface_fluxes).compute()
    logger.info(f"computec net_surface_fluxes")

    # get the heat content of the 700m ocean layer and perform spatial averaging
    avg_hc700m = data_oc['avg_hc700m']
    avg_hc700m=reader_oc.fldmean(avg_hc700m).compute()
    # compute time derivative of the heat content
    avg_hc_time_derivative = (avg_hc700m.shift(time=-1) - avg_hc700m) / (avg_hc700m['time'].shift(time=-1) - avg_hc700m['time']).dt.total_seconds()
    logger.info(f"computed avg_hc_time_derivative")

    # now plot the time series
    title_args = {'model': model, 'exp': exp, 'source': source_atm}
    plot_time_series(net_surface_fluxes, avg_hc_time_derivative, title_args, var1_label="Net OSF [W m**2]",  var2_label="HC700m time derivative [W m**2]", outdir=outputdir_pdf)
    plot_difference(net_surface_fluxes, avg_hc_time_derivative, title_args, var1_label="Net OSF [W m**2]",  var2_label="HC700m time derivative [W m**2]", outdir=outputdir_pdf)
    client.shutdown()
    cluster.close()