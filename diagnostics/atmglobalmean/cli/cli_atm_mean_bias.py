# All necessary import for a cli diagnostic
import sys
import os
import argparse

from aqua.util import load_yaml, get_arg
from aqua import Reader
from aqua.logger import log_configure


def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Atmospheric global mean biases CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
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

    print('Running atmospheric global mean biases diagnostic...')

    # change the current directory to the one of the CLI so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        print(f'Moving from current directory to {dname} to run!')

    try:
        sys.path.insert(0, '../../')
        from atmglobalmean import compare_datasets_plev, seasonal_bias, plot_map_with_stats
    except ImportError as import_error:
        # Handle ImportError
        print(f"ImportError occurred: {import_error}")
        sys.exit(0)
    except Exception as custom_error:
        # Handle other custom exceptions if needed
        print(f"CustomError occurred: {custom_error}")
        sys.exit(0)
    else:
        # Code to run if the import was successful (optional)
        print("Modules imported successfully.")

    # Aquiring arguments and configuration
    args = parse_arguments(sys.argv[1:])
    file = get_arg(args, 'config', 'config/atm_mean_bias_config.yml')
    print('Reading configuration yaml file..')
    config = load_yaml(file)

    # Configure logging
    loglevel = get_arg(args, 'loglevel', config['loglevel'])
    logger = log_configure(log_level=loglevel, log_name='Atmglobalmean CLI')

    # Acquiring model, experiment and source
    model = get_arg(args, 'model', config['data']['model'])
    exp = get_arg(args, 'exp', config['data']['exp'])
    source = get_arg(args, 'source', config['data']['source'])

    logger.debug(f"model: {model}")
    logger.debug(f"exp: {exp}")
    logger.debug(f"source: {source}")

    path_to_output = get_arg(
        args, 'outputdir', config['path']['path_to_output'])
    if path_to_output is not None:
        outputdir = os.path.join(path_to_output, 'netcdf/')
        outputfig = os.path.join(path_to_output, 'pdf/')

    logger.debug(f"outputdir: {outputdir}")
    logger.debug(f"outputfig: {outputfig}")

    # This is the model to compare with
    model_obs = config['data']['model_obs']
    exp_obs = config['data']['exp_obs']
    source_obs = config['data']['source_obs']

    logger.debug(f"Comparing with {model_obs} {exp_obs} {source_obs}.")

    variables_no_plev = config['diagnostic_attributes']['variables_no_plev']
    variables_with_plev = config['diagnostic_attributes']['variables_with_plev']
    plev = config['diagnostic_attributes']['plev']
    statistic = config['diagnostic_attributes']['statistic']
    seasonal_bias_bool = config['diagnostic_attributes']['seasonal_bias']
    compare_datasets_plev_bool = config['diagnostic_attributes']['compare_datasets_plev']
    plot_map_with_stats_bool = config['diagnostic_attributes']['plot_map_with_stats']

    model_label = model+'_'+exp+'_'+source
    model_label_obs = model_obs+'_'+exp_obs+'_'+source_obs

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
            try:
                seasonal_bias(dataset1=data, dataset2=data_obs, var_name=var_name, plev=plev, statistic=statistic,
                              model_label1=model_label, model_label2=model_label_obs,
                              outputdir=outputdir, outputfig=outputfig)
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    if compare_datasets_plev_bool:
        for var_name in variables_with_plev:
            try:
                compare_datasets_plev(dataset1=data, dataset2=data_obs, var_name=var_name,
                                      model_label1=model_label, model_label2=model_label_obs,
                                      outputdir=outputdir, outputfig=outputfig)
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    if plot_map_with_stats_bool:
        for var_name in variables_no_plev:
            try:
                plot_map_with_stats(dataset=data, var_name=var_name,  model_label=model_label,
                                    outputdir=outputdir, outputfig=outputfig)
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    logger.info("Atmospheric global mean biases diagnostic is terminated.")
