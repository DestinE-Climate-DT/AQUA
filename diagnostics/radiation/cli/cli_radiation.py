# All necessary import for a cli diagnostic
import sys
import os
import argparse

from dask.distributed import Client, LocalCluster

from aqua.util import load_yaml, get_arg
from aqua.logger import log_configure

def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Radiation Budget Diagnostic CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
    # This arguments will override the configuration file if provided
    parser.add_argument('--model', type=str, help='model name',
                        required=False)
    parser.add_argument('--exp', type=str, help='experiment name',
                        required=False)
    parser.add_argument('--source', type=str, help='source name',
                        required=False)
    parser.add_argument('--outputdir', type=str, help='output directory',
                        required=False)
    parser.add_argument('--loglevel', '-l', type=str, help='loglevel',
                        required=False)

    return parser.parse_args(args)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='Radiation CLI')

    # Setting the path to this directory
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')
    sys.path.insert(0, '../../')
    try:
        from radiation import process_ceres_data, process_model_data
        from radiation import boxplot_model_data, plot_mean_bias, plot_model_comparison_timeseries
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(0)

    logger.info('Running Radiation Budget Diagnostic ...')

    # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    if nworkers:
        cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
        client = Client(cluster)
        logger.info(f"Running with {nworkers} dask distributed workers.")

    file = get_arg(args, 'config', 'config/radiation_config.yml')
    logger.info('Reading configuration yaml file..')
    config = load_yaml(file)

    model = get_arg(args, 'model', config['data']['model'])
    exp = get_arg(args, 'exp', config['data']['exp'])
    source = get_arg(args, 'source', config['data']['source'])

    logger.debug(f"model: {model}, exp: {exp}, source: {source}")

    exp_ceres = config['data']['exp_ceres']
    source_ceres = config['data']['source_ceres']

    exp_era5 = config['data']['exp_era5']
    source_era5 = config['data']['source_era5']

    path_to_output = get_arg(
        args, 'outputdir', config['path']['path_to_output'])
    if path_to_output is not None:
        outputdir = os.path.join(path_to_output, 'netcdf/')
        outputfig = os.path.join(path_to_output, 'pdf/')

    logger.debug(f"outputdir: {outputdir}")
    logger.debug(f"outputfig: {outputfig}")

    box_plot_bool = config['diagnostic_attributes']['box_plot']
    bias_maps_bool = config['diagnostic_attributes']['bias_maps']
    time_series_bool = config['diagnostic_attributes']['time_series']
    try:
        model_data = process_model_data(model=model, exp=exp, source=source, loglevel=loglevel)
    except Exception as e:
        logger.error(f"No model data found: {e}")
        logger.error("Radiation diagnostic is terminated.")
        sys.exit(0)
    try:
        # Call the method to retrieve CERES data
        ceres = process_ceres_data(exp=exp_ceres, source=source_ceres, fix=True, loglevel=loglevel)
        era5 = process_model_data(model='ERA5', exp=exp_era5, source=source_era5,
                                  fix=True, loglevel=loglevel)
    except Exception as e:
        logger.warning(f"No observation data found: {e}")
        logger.error("Radiation diagnostic is terminated.")
        sys.exit(0)  # remove @ and change loggers

    if box_plot_bool:
        try:
            datasets = [era5, ceres, model_data]
            boxplot_model_data(datasets=datasets, outputdir=outputdir, outputfig=outputfig, loglevel=loglevel)
            logger.info("The boxplot with provided model and observation was created and saved. Variables are plotted to show imbalances.")
        except Exception as e:
            # Handle other exceptions
            logger.error(f"An unexpected error occurred: {e}")
            
    if bias_maps_bool:
        for var in ['mtnlwrf', 'mtnswrf', 'tnr']:
            try:
                plot_mean_bias(model=model_data, var=var, ceres=ceres,
                               outputdir=outputdir, outputfig=outputfig, loglevel=loglevel)
                logger.info(
                    f"The mean bias of the data over the specified time range is calculated, plotted, and saved for {var} variable.")
            except Exception as e:
                # Handle other exceptions
                logger.error(f"An unexpected error occurred: {e}")

    if time_series_bool:
        try:
            plot_model_comparison_timeseries(models=model_data, ceres=ceres,
                                             outputdir=outputdir, outputfig=outputfig,
                                             loglevel=loglevel)
            logger.info(
                "The time series bias plot with various models and CERES was created and saved.")
        except Exception as e:
            # Handle other exceptions
            logger.error(f"An unexpected error occurred: {e}")

    logger.info("Radiation Budget Diagnostic is terminated.")
