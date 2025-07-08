import argparse
import sys
import pandas as pd

from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args

from aqua.util import load_yaml, get_arg, OutputSaver, ConfigPath, to_list
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.diagnostics import GlobalBiases, PlotGlobalBiases

def parse_arguments(args):
    """Parse command-line arguments for GlobalBiases diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='GlobalBiases CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='GlobalBiases CLI')
    logger.info(f"Running GlobalBiases diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster, = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments
    config_dict = load_diagnostic_config(diagnostic='globalbiases', config=args.config,
                                         default_config='config_global_biases.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    regrid = get_arg(args, 'regrid', None) 

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild = config_dict['output'].get('rebuild', True)
    save_netcdf = config_dict['output'].get('save_netcdf', True)
    save_pdf = config_dict['output'].get('save_pdf', True)
    save_png = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300) 

    # Global Biases diagnostic
    if 'globalbiases' in config_dict['diagnostics']:
        if config_dict['diagnostics']['globalbiases']['run']:
            logger.info("GlobalBiases diagnostic is enabled.")

            if len(config_dict['datasets']) > 1:
                logger.warning(
                    "Only the first entry in 'datasets' will be used.\n"
                    "Multiple datasets are not supported by this diagnostic."
                )
            if len(config_dict['references']) > 1:
                logger.warning(
                    "Only the first entry in 'references' will be used.\n"
                    "Multiple references are not supported by this diagnostic."
                )
            dataset = config_dict['datasets'][0]
            reference = config_dict['references'][0]
            dataset_args = {'catalog': dataset['catalog'], 'model': dataset['model'],
                            'exp': dataset['exp'], 'source': dataset['source'],
                            'regrid': regrid if regrid is not None else dataset.get('regrid', None)}
            reference_args = {'catalog': reference['catalog'], 'model': reference['model'],
                            'exp': reference['exp'], 'source': reference['source'],
                            'regrid': regrid if regrid is not None else reference.get('regrid', None)}
            
            variables = config_dict['diagnostics']['globalbiases'].get('variables', [])
            plev = config_dict['diagnostics']['globalbiases']['params']['default'].get('plev')
            seasons = config_dict['diagnostics']['globalbiases']['params']['default'].get('seasons', False)
            seasons_stat = config_dict['diagnostics']['globalbiases']['params']['default'].get('seasons_stat', 'mean')
            vertical = config_dict['diagnostics']['globalbiases']['params']['default'].get('vertical', False)

            startdate_data = config_dict['diagnostics']['globalbiases']['params']['default'].get('startdate_data', None)
            enddate_data = config_dict['diagnostics']['globalbiases']['params']['default'].get('enddate_data', None)
            startdate_ref = config_dict['diagnostics']['globalbiases']['params']['default'].get('startdate_ref', None)
            enddate_ref = config_dict['diagnostics']['globalbiases']['params']['default'].get('enddate_ref', None)
            projection = config_dict['diagnostics']['globalbiases']['plot_params']['default'].get('projection', None)

            biases_dataset = GlobalBiases(**dataset_args, startdate=startdate_data, enddate=enddate_data,
                                          outputdir=outputdir, loglevel=loglevel)
            biases_reference = GlobalBiases(**reference_args, startdate=startdate_ref, enddate=enddate_ref,
                                            outputdir=outputdir, loglevel=loglevel)


            for var in variables:
                logger.info(f"Running Global Biases diagnostic for variable: {var}")
                plot_params = config_dict['diagnostics']['globalbiases']['plot_params'].get(var, {})
                vmin, vmax = plot_params.get('vmin'), plot_params.get('vmax')
                projection = plot_params.get('projection', projection)
                units = 'mm/day' if var in ['tprate', 'mtpr'] else None
                logger.warning(f"Using projection: {projection} for variable: {var}")


                param_dict = config_dict['diagnostics']['globalbiases'].get('params', {}).get(var, {})
                biases_dataset.retrieve(var=var, units=units)
                biases_reference.retrieve(var=var, units=units)

                biases_dataset.compute_climatology(seasonal=seasons, seasons_stat=seasons_stat)
                biases_reference.compute_climatology(seasonal=seasons, seasons_stat=seasons_stat)

                if 'plev' in biases_dataset.data.get(var, {}).dims and plev:
                    plev_list = to_list(plev)
                else: 
                    plev_list = [None] 

                for p in plev_list:
                    logger.info(f"Processing variable: {var} at pressure level: {p}" if p else f"Processing variable: {var} at surface level")

                    plot_biases = PlotGlobalBiases(save_pdf=save_pdf, save_png=save_png, dpi=dpi, outputdir=outputdir, loglevel=loglevel)
                    plot_biases.plot_bias(data=biases_dataset.climatology, data_ref=biases_reference.climatology,
                                          var=var, plev=p, proj=projection, vmin=vmin, vmax=vmax) 
                    if seasons:
                        plot_biases.plot_seasonal_bias(data=biases_dataset.seasonal_climatology, 
                                                       data_ref=biases_reference.seasonal_climatology,
                                                       var=var, plev=p, proj=projection, vmin=vmin, vmax=vmax)

                if vertical and 'plev' in biases_dataset.data.get(var, {}).dims:
                    logger.debug(f"Plotting vertical bias for variable: {var}")
                    vmin, vmax = plot_params.get('vmin_v'), plot_params.get('vmax_v')
                    plot_biases.plot_vertical_bias(data=biases_dataset.climatology, data_ref=biases_reference.climatology, 
                                                   var=var, vmin=vmin, vmax=vmax)

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

    logger.info("Global Biases diagnostic completed.")
