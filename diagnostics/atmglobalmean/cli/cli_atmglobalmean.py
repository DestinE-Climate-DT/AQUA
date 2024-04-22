# All necessary import for a cli diagnostic
import sys
import os
import argparse

from aqua.util import load_yaml, get_arg
from aqua import Reader
from aqua.logger import log_configure

from dask.distributed import Client, LocalCluster

def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Atmospheric global mean biases CLI')
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
    parser.add_argument('--start_date1', type=str, help='start date for dataset1',
                        required=False)
    parser.add_argument('--end_date1', type=str, help='end date for dataset1',
                        required=False)
    parser.add_argument('--start_date2', type=str, help='start date for dataset2',
                        required=False)
    parser.add_argument('--end_date2', type=str, help='end date for dataset2',
                        required=False)

    return parser.parse_args(args)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')

    logger = log_configure(log_level=loglevel, log_name='Atmglobalmean CLI')
    logger.info('Running atmospheric global mean biases diagnostic...')

    # change the current directory to the one of the CLI so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')

    try:
        sys.path.insert(0, '../../')
        from atmglobalmean import compare_datasets_plev, seasonal_bias, plot_map_with_stats
    except ImportError as import_error:
        # Handle ImportError
        logger.error(f"ImportError occurred: {import_error}")
        sys.exit(0)
    except Exception as custom_error:
        # Handle other custom exceptions if needed
        logger.error(f"Exception occurred: {custom_error}")
        sys.exit(0)

    # Aquiring the configuration
    file = get_arg(args, 'config', 'config/atm_mean_bias_config.yaml')
    logger.info('Reading configuration yaml file..')
    config = load_yaml(file)

    # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    if nworkers:
        cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
        client = Client(cluster)
        logger.info(f"Running with {nworkers} dask distributed workers.")

    # Acquiring model, experiment and source
    model = get_arg(args, 'model', config['data']['model'])
    exp = get_arg(args, 'exp', config['data']['exp'])
    source = get_arg(args, 'source', config['data']['source'])

    # Acquiring start and end dates for dataset1 and dataset2
    start_date1 = get_arg(args, 'start_date1', None)
    end_date1 = get_arg(args, 'end_date1', None)
    start_date2 = get_arg(args, 'start_date2', None)
    end_date2 = get_arg(args, 'end_date2', None)

    logger.debug(f"Running for {model} {exp} {source}.")

    path_to_output = get_arg(
        args, 'outputdir', config['path']['path_to_output'])
    if path_to_output:
        outputdir = os.path.join(path_to_output, 'netcdf/')
        outputfig = os.path.join(path_to_output, 'pdf/')
    else:
        logger.error("No output directory provided.")
        logger.critical("Atmospheric global mean biases diagnostic is terminated.")
        sys.exit(0)

    logger.debug(f"outputdir: {outputdir}")
    logger.debug(f"outputfig: {outputfig}")

    # This is the model to compare with
    model_obs = config['data']['model_obs']
    exp_obs = config['data']['exp_obs']
    source_obs = config['data']['source_obs']

    logger.debug(f"Comparing with {model_obs} {exp_obs} {source_obs}.")

    variables_no_plev = config['diagnostic_attributes'].get('variables_no_plev', [])
    variables_with_plev = config['diagnostic_attributes'].get('variables_with_plev', [])
    plev = config['diagnostic_attributes'].get('plev', None)
    statistic = config['diagnostic_attributes'].get('statistic', 'mean')
    seasonal_bias_bool = config['diagnostic_attributes'].get('seasonal_bias', True)
    compare_datasets_plev_bool = config['diagnostic_attributes'].get('compare_datasets_plev', False)
    plot_map_with_stats_bool = config['diagnostic_attributes'].get('plot_map_with_stats', False)
    start_date1 = config['diagnostic_attributes'].get('start_date1', None)
    end_date1 = config['diagnostic_attributes'].get('end_date1', None)
    start_date2 = config['diagnostic_attributes'].get('start_date2', "1980-01-01")
    end_date2 = config['diagnostic_attributes'].get('end_date2', "2010-12-31")

    model_label = model+'_'+exp
    model_label_obs = model_obs+'_'+exp_obs

    try:
        reader_obs = Reader(model=model_obs, exp=exp_obs, source=source_obs, loglevel=loglevel)
        data_obs = reader_obs.retrieve()
    except Exception as e:
        logger.error(f"No observation data found: {e}")

    try:
        reader = Reader(model=model, exp=exp, source=source, loglevel=loglevel)
        data = reader.retrieve()
    except Exception as e:
        logger.error(f"No model data found: {e}")
        logger.critical("Atmospheric global mean biases diagnostic is terminated.")
        sys.exit(0)

    if seasonal_bias_bool:
        for var_name in variables_no_plev:
            logger.info(f"Running seasonal bias diagnostic for {var_name}...")

            # Getting variable specific attributes
            var_attributes = config['seasonal_bias'].get(var_name, {})
            vmin = var_attributes.get('vmin', None)
            vmax = var_attributes.get('vmax', None)
            logger.debug(f"var: {var_name}, vmin: {vmin}, vmax: {vmax}")

            try:
                seasonal_bias(dataset1=data, dataset2=data_obs,
                              var_name=var_name, plev=plev, statistic=statistic,
                              model_label1=model_label, model_label2=model_label_obs,
                              start_date1=start_date1, end_date1=end_date1,
                              start_date2=start_date2, end_date2=end_date2,
                              outputdir=outputdir, outputfig=outputfig,
                              vmin=vmin, vmax=vmax,
                              loglevel=loglevel)
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    if compare_datasets_plev_bool:
        for var_name in variables_with_plev:
            logger.info(f"Running compare datasets plev diagnostic for {var_name}...")
            
            # Getting variable specific attributes
            var_attributes = config['compare_datasets_plev'].get(var_name, {})
            vmin = var_attributes.get('vmin', None)
            vmax = var_attributes.get('vmax', None)
            logger.debug(f"var: {var_name}, vmin: {vmin}, vmax: {vmax}")
            
            try:
                compare_datasets_plev(dataset1=data, dataset2=data_obs, var_name=var_name,
                                      model_label1=model_label, model_label2=model_label_obs,
                                      start_date1=start_date1, end_date1=end_date1,
                                      start_date2=start_date2, end_date2=end_date2,
                                      outputdir=outputdir, outputfig=outputfig,
                                      vin=vmin, vmax=vmax,
                                      loglevel=loglevel)
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    if plot_map_with_stats_bool:
        for var_name in variables_no_plev:
            logger.info(f"Running plot map with stats diagnostic for {var_name}...")
            try:
                plot_map_with_stats(dataset=data, var_name=var_name,  model_label=model_label,
                                    outputdir=outputdir, outputfig=outputfig, loglevel=loglevel)
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    logger.info("Atmospheric global mean biases diagnostic is terminated.")
