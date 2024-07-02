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

    parser = argparse.ArgumentParser(description='Atmospheric global biases CLI')
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
    parser.add_argument('--startdate_data', type=str, help='start date for dataset',
                        required=False)
    parser.add_argument('--enddate_data', type=str, help='end date for dataset',
                        required=False)
    parser.add_argument('--startdate_ref', type=str, help='start date for reference dataset',
                        required=False)
    parser.add_argument('--enddate_ref', type=str, help='end date for reference dataset',
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
        from atmglobalmean import AtmosphericGlobal
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

    # Acquiring model, experiment and source
    model = get_arg(args, 'model', config['data']['model'])
    exp = get_arg(args, 'exp', config['data']['exp'])
    source = get_arg(args, 'source', config['data']['source'])

    # Acquiring model, experiment and source for reference data
    model_obs= config['data']['model_obs']
    exp_obs = config['data']['exp_obs']
    source_obs = config['data']['source_obs']

    logger.debug(f"Running for {model} {exp} {source}.")

    logger.debug(f"Comparing with {model_obs} {exp_obs} {source_obs}.")

    variables= config['diagnostic_attributes'].get('variables', [])
    plev = config['diagnostic_attributes'].get('plev', None)
    seasons_bool = config['diagnostic_attributes'].get('seasons', False)
    seasons_stat = config['diagnostic_attributes'].get('seasons_stat', 'mean')
    vertical = config['diagnostic_attributes'].get('vertical', False)
    startdate_data = config['diagnostic_attributes'].get('startdate_data', None)
    enddate_data = config['diagnostic_attributes'].get('enddate_data', None)
    startdate_obs = config['diagnostic_attributes'].get('startdate_obs', "1980-01-01")
    enddate_obs = config['diagnostic_attributes'].get('enddate_obs', "2010-12-31")

    model_label = model+'_'+exp
    model_label_obs = model_obs+'_'+exp_obs

    try:
        reader = Reader(model=model, exp=exp, source=source, startdate=startdate_data, enddate=enddate_data, loglevel=loglevel)
        data = reader.retrieve()
    except Exception as e:
        logger.error(f"No model data found: {e}")
        logger.critical("Atmospheric global mean biases diagnostic is terminated.")
        sys.exit(0)

    try:
        reader_obs = Reader(model=model_obs, exp=exp_obs, source=source_obs, startdate=startdate_obs, enddate=enddate_obs, loglevel=loglevel)
        data_obs = reader_obs.retrieve()
    except Exception as e:
        logger.error(f"No observation data found: {e}")

    for var_name in variables:
        logger.info(f"Running seasonal bias diagnostic for {var_name}...")

        if vertical:
            # Getting variable specific attributes
            var_attributes = config['vertical_plev'].get(var_name, {})
            vmin = var_attributes.get('vmin', None)
            vmax = var_attributes.get('vmax', None)
            logger.debug(f"var: {var_name}, vmin: {vmin}, vmax: {vmax}")

        try:
            atmos_global = AtmosphericGlobal(data=data, data_ref = data_obs, var_name=var_name, seasons=seasons_bool, plev=plev,
                                             vertical=vertical, vmin=vmin, vmax=vmax)
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")

    logger.info("Atmospheric global mean biases diagnostic is terminated.")
