# All necessary import for a cli diagnostic
import sys
try:
    from aqua.util import load_yaml, get_arg
    import os
    import argparse
    from aqua import Reader
    from aqua.logger import log_configure
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
    args = parse_arguments(sys.argv[1:])

    # Configure logging
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='Atmglobalmean CLI')

    file = get_arg(args, 'config', 'config/atm_mean_bias_config.yml')
    logger.info('Reading configuration yaml file..')
    config = load_yaml(file)

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
    model2 = config['data']['model2']
    exp2 = config['data']['exp2']
    source2 = config['data']['source2']

    logger.debug(f"Comparing with {model2} {exp2} {source2}.")

    start_date1 = config['time_frame']['start_date1']
    end_date1 = config['time_frame']['end_date1']
    start_date2 = config['time_frame']['start_date2']
    end_date2 = config['time_frame']['end_date2']

    var_name = config['diagnostic_attributes']['var_name']
    variables = config['diagnostic_attributes']['variables']
    plev = config['diagnostic_attributes']['plev']
    statistic = config['diagnostic_attributes']['statistic']
    seasonal_bias_bool = config['diagnostic_attributes']['seasonal_bias']
    compare_datasets_plev_bool = config['diagnostic_attributes']['compare_datasets_plev']
    plot_map_with_stats_bool = config['diagnostic_attributes']['plot_map_with_stats']

    model_label1 = model+'_'+exp+'_'+source
    model_label2 = config['plot']['model_label2']

    try:
        reader_obs = Reader(model=model2, exp=exp2, source=source2, loglevel=loglevel)
        data_obs = reader_obs.retrieve()
    except Exception as e:
        logger.error(f"No observation data found: {e}")

    try:
        reader = Reader(model=model, exp=exp, source=source, loglevel=loglevel)
        data = reader.retrieve()
    except Exception as e:
        logger.error(f"No model data found: {e}")
        logger.info("Atmospheric global mean biases diagnostic is terminated.")
        sys.exit(0)

    dataset1 = data
    dataset2 = data_obs

    if seasonal_bias_bool:
        for var_name in variables:
            try:
                seasonal_bias(dataset1, dataset2, var_name, plev, statistic,
                              model_label1, model_label2,
                              start_date1, end_date1, start_date2, end_date2,
                              outputdir, outputfig)
                logger.warning(
                    f"The seasonal bias maps were calculated and plotted for {var_name} variable.")
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    if compare_datasets_plev_bool:
        for var_name in variables:
            try:
                compare_datasets_plev(dataset1, dataset2, var_name,
                                      start_date1, end_date1,
                                      start_date2, end_date2,
                                      model_label1, model_label2,
                                      outputdir, outputfig)
                logger.warning(
                    f"The comparison of the two datasets is calculated and plotted for {var_name} variable.")
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    if plot_map_with_stats_bool:
        for var_name in variables:
            try:
                plot_map_with_stats(dataset1, var_name, start_date1,
                                    end_date1, model_label1,
                                    outputdir, outputfig)
                logger.warning(
                    f"The map of a chosen variable from a dataset is calculated and plotted for {var_name} variable.")
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    logger.warning("Atmospheric global mean biases diagnostic is terminated.")
