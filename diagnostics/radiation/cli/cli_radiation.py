import sys
import os
import argparse

from dask.distributed import Client, LocalCluster
from aqua.util import load_yaml, get_arg
from aqua.logger import log_configure

def parse_arguments(args):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Radiation Budget Diagnostic CLI')
    parser.add_argument('-c', '--config', type=str, help='yaml configuration file')
    parser.add_argument('-n', '--nworkers', type=int, help='number of dask distributed workers')
    parser.add_argument('--loglevel', '-l', type=str, help='loglevel', required=False)
    parser.add_argument('--variables', nargs='+', help='List of variables to be plotted', required=False)

    # These will override the first one in the config file if provided
    parser.add_argument("--catalog", type=str,
                        required=False, help="catalog name")
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source", type=str,
                        required=False, help="source name")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")

    return parser.parse_args(args)

if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='Radiation CLI')

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

    nworkers = get_arg(args, 'nworkers', None)
    if nworkers:
        cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
        client = Client(cluster)
        logger.info(f"Running with {nworkers} dask distributed workers.")

    # Load configuration file
    file = get_arg(args, 'config', 'config/radiation_config.yml')
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    models = config['models']
    models[0]['catalog'] = get_arg(args, 'catalog', models[0]['catalog'])
    models[0]['model'] = get_arg(args, 'model', models[0]['model'])
    models[0]['exp'] = get_arg(args, 'exp', models[0]['exp'])
    models[0]['source'] = get_arg(args, 'source', models[0]['source'])

    outputdir = get_arg(args, "outputdir", config["outputdir"])

    logger.debug("Analyzing models:")
    catalogs_list = []
    models_list = []
    exp_list = []
    source_list = []

    exp_ceres = config['exp_ceres']
    source_ceres = config['source_ceres']

    output_nc = os.path.join(outputdir, 'netcdf/') if outputdir else None
    output_pdf = os.path.join(outputdir, 'pdf/') if outputdir else None

    logger.debug(f"output_nc: {output_nc}")
    logger.debug(f"output_pdf: {output_pdf}")

    box_plot_bool = config['diagnostic_attributes']['box_plot']
    bias_maps_bool = config['diagnostic_attributes']['bias_maps']
    time_series_bool = config['diagnostic_attributes']['time_series']


    try:
            ceres = process_ceres_data(
                exp=exp_ceres,
                source=source_ceres,
                fix=True,
                loglevel=loglevel
            )
            
    except Exception as e:
        logger.warning(f"No observation data found: {e}")
        
    for model in models:
        logger.debug(f"  - {model['catalog']} {model['model']} {model['exp']} {model['source']}")
        try:
            model_data = process_model_data(
                model=model['model'],
                exp=model['exp'],
                source=model['source'],
                fix=model.get('fix', True),
                start_date=model.get('start_date'),
                end_date=model.get('end_date'),
                loglevel=loglevel
            )
            models_list.append(model_data)
            datasets = [ceres] + models_list
        except Exception as e:
            logger.error(f"No model data found: {e}")
            continue

    if box_plot_bool:
        box_plot_vars = config['diagnostic_attributes'].get('box_plot_vars', [])
        variables = get_arg(args, 'variables', box_plot_vars)
        try:
            boxplot_model_data(
                datasets=datasets,
                outputdir=output_nc,
                outputfig=output_pdf,
                loglevel=loglevel,
                variables=variables
            )
            logger.info("The boxplot with provided models and observation was created and saved. Variables are plotted to show imbalances.")
        except Exception as e:
            logger.error(f"An unexpected error occurred while processing data: {e}")

    if bias_maps_bool:
        bias_maps_vars = config['diagnostic_attributes'].get('bias_maps_vars', [])
        variables = get_arg(args, 'variables', bias_maps_vars)
        for var in variables:
            try:
                for model in models_list:  # Iterate over all model datasets
                    plot_mean_bias(
                        model=model,
                        var=var,
                        ceres=ceres,
                        outputdir=output_nc,
                        outputfig=output_pdf,
                        loglevel=loglevel
                    )
                logger.info(
                    f"The mean bias of the data over the specified time range is calculated, plotted, and saved for {var} variable for all models."
                )
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")

    if time_series_bool:
        try:
            plot_model_comparison_timeseries(
                models=models_list,
                ceres=ceres,
                outputdir=output_nc,
                outputfig=output_pdf,
                loglevel=loglevel
            )
            logger.info("The time series bias plot with various models and CERES was created and saved.")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")

    logger.info("Radiation Budget Diagnostic is terminated.")
