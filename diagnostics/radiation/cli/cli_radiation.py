# All necessary import for a cli diagnostic
import sys
try:
    import os
    import argparse
    from aqua.util import load_yaml, get_arg
    from aqua.logger import log_configure

    # Setting the path to this directory
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        print(f'Moving from current directory to {dname} to run!')
    sys.path.insert(0, '../../')
    from radiation import process_ceres_data, process_model_data
    from radiation import boxplot_model_data, plot_mean_bias, gregory_plot, plot_model_comparison_timeseries
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

    parser = argparse.ArgumentParser(description='Radiation Budget Diagnostic CLI')
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

    print('Running Radiation Budget Diagnostic ...')
    args = parse_arguments(sys.argv[1:])

    file = get_arg(args, 'config', 'config/radiation_config.yml')
    print('Reading configuration yaml file..')
    config = load_yaml(file)

    # Configure logging
    loglevel = get_arg(args, 'loglevel', config['loglevel'])
    logger = log_configure(log_level=loglevel, log_name='Radiation CLI')

    model = get_arg(args, 'model', config['data']['model'])
    exp = get_arg(args, 'exp', config['data']['exp'])
    source = get_arg(args, 'source', config['data']['source'])

    logger.debug(f"model: {model}")
    logger.debug(f"exp: {exp}")
    logger.debug(f"source: {source}")

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
    gregory_bool = config['diagnostic_attributes']['gregory']
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
            datasets = [ceres, model_data]
            boxplot_model_data(datasets=datasets, outputdir=outputdir, outputfig=outputfig, loglevel=loglevel)
            logger.info("The boxplot with provided model and CERES was created and saved. Variables ttr and tsr are plotted to show imbalances.")
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

    if gregory_bool:
        try:
            gregory_plot(obs_data=era5, models=model_data,
                         outputdir=outputdir, outputfig=outputfig, loglevel=loglevel)
            logger.info(
                "Gregory Plot was created and saved with various models and an observational dataset.")
        except Exception as e:
            # Handle other exceptions
            logger.error(f"An unexpected error occurred: {e}")

    if time_series_bool:
        try:
            plot_model_comparison_timeseries(models=model_data, ceres=ceres,
                                             outputdir=outputdir, outputfig=outputfig,
                                             ylim=15, loglevel=loglevel)
            logger.info(
                "The time series bias plot with various models and CERES was created and saved.")
        except Exception as e:
            # Handle other exceptions
            logger.error(f"An unexpected error occurred: {e}")

    logger.info("Radiation Budget Diagnostic is terminated.")
