# Imports for CLI diagnostic
import sys
import os
import argparse
from dask.distributed import Client, LocalCluster
import pandas as pd

from aqua.util import load_yaml, get_arg, OutputSaver, ConfigPath
from aqua import Reader
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure
from aqua.diagnostics import GlobalBiases

def parse_arguments(args):
    """Parse command line arguments for Global Biases CLI."""
    parser = argparse.ArgumentParser(description='Global Biases CLI')
    parser.add_argument('-c', '--config', type=str, required=False, help='YAML configuration file')
    parser.add_argument('-n', '--nworkers', type=int, help='Number of Dask distributed workers')
    parser.add_argument("--loglevel", "-l", type=str, help="Logging level")

    # Arguments to override configuration file settings
    parser.add_argument("--catalog", type=str, required=False, help="Catalog name")
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--outputdir', type=str, help='Output directory')
    parser.add_argument("--cluster", type=str, required=False, help="dask cluster address")

    return parser.parse_args(args)


def main():
    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, 'CLI Global Biases')
    logger.info("Starting Global Biases diagnostic")

    # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    cluster = get_arg(args, 'cluster', None)
    private_cluster = False
    if nworkers or cluster:
        if not cluster:
            cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
            logger.info(f"Initializing private cluster {cluster.scheduler_address} with {nworkers} workers.")
            private_cluster = True
        else:
            logger.info(f"Connecting to cluster {cluster}.")
        client = Client(cluster)
    else:
        client = None

    # Load configuration file
    configdir = ConfigPath(loglevel=loglevel).configdir
    default_config = os.path.join(configdir, "diagnostics", "global_biases",
                                  "config_global_biases.yaml")
    file = get_arg(args, "config", default_config)
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    catalog_data = get_arg(args, 'catalog', config['data']['catalog'])
    model_data = get_arg(args, 'model', config['data']['model'])
    exp_data = get_arg(args, 'exp', config['data']['exp'])
    source_data = get_arg(args, 'source', config['data']['source'])
    startdate_data = config['diagnostic_attributes'].get('startdate_data')
    enddate_data = config['diagnostic_attributes'].get('enddate_data')

    catalog_obs = config['obs']['catalog']
    model_obs = config['obs']['model']
    exp_obs = config['obs']['exp']
    source_obs = config['obs']['source']
    startdate_obs = config['diagnostic_attributes'].get('startdate_obs')
    enddate_obs = config['diagnostic_attributes'].get('enddate_obs')

    outputdir = get_arg(args, "outputdir", config['output'].get("outputdir"))
    rebuild = config['output'].get("rebuild")
    filename_keys = config['output'].get("filename_keys")
    save_netcdf = config['output'].get("save_netcdf")
    save_pdf = config['output'].get("save_pdf")
    save_png = config['output'].get("save_png")
    dpi = config['output'].get("dpi")

    variables = config['diagnostic_attributes'].get('variables', [])
    plev = config['diagnostic_attributes'].get('plev')
    seasons_bool = config['diagnostic_attributes'].get('seasons', False)
    seasons_stat = config['diagnostic_attributes'].get('seasons_stat', 'mean')
    vertical = config['diagnostic_attributes'].get('vertical', False)


    # Retrieve data and handle potential errors
    try:
        reader = Reader(catalog=catalog_data, model=model_data, exp=exp_data, source=source_data,
                        startdate=startdate_data, enddate=enddate_data)
        data = reader.retrieve()

        # Calculate 'tnr' if applicable
        if 'tnr' in variables:
            data['tnr'] = data['tnlwrf'] + data['tnswrf']

    except Exception as e:
        logger.error(f"No model data found: {e}")
        sys.exit("Global biases diagnostic terminated.")

    try:
        reader_obs = Reader(catalog=catalog_obs, model=model_obs, exp=exp_obs, source=source_obs,
                            startdate=startdate_obs, enddate=enddate_obs, loglevel=loglevel)
        data_obs = reader_obs.retrieve()

        # Calculate 'tnr' for observations if applicable
        if 'tnr' in variables:
            data_obs['tnr'] = data_obs['tnlwrf'] + data_obs['tnswrf']

    except Exception as e:
        logger.error(f"No observation data found: {e}")

    # Update date attributes with data if not set in config
    startdate_data = startdate_data or pd.to_datetime(data.time[0].values).strftime('%Y-%m-%d')
    enddate_data = enddate_data or pd.to_datetime(data.time[-1].values).strftime('%Y-%m-%d')
    startdate_obs = startdate_obs or pd.to_datetime(data_obs.time[0].values).strftime('%Y-%m-%d')
    enddate_obs = enddate_obs or pd.to_datetime(data_obs.time[-1].values).strftime('%Y-%m-%d')

    output_saver = OutputSaver(diagnostic='global_biases', catalog=reader.catalog, model=model_data, exp=exp_data, loglevel=loglevel,
                               default_path=outputdir, rebuild=rebuild, filename_keys=filename_keys)

    # Loop over variables for diagnostics
    for var_name in variables:
        logger.info(f"Running Global Biases diagnostic for variable: {var_name}")
        var_attributes = config["biases_plot_params"]['bias_maps'].get(var_name, {})
        vmin, vmax = var_attributes.get('vmin'), var_attributes.get('vmax')

        try:
            global_biases = GlobalBiases(data=data, data_ref=data_obs, var_name=var_name, plev=plev, loglevel=loglevel,
                                         model=model_data, exp=exp_data, startdate_data=startdate_data,
                                         enddate_data=enddate_data, model_obs=model_obs,
                                         startdate_obs=startdate_obs, enddate_obs=enddate_obs)

            # Define common save arguments
            common_save_args = {'var': var_name, 'dpi': dpi,
                                'catalog_2': reader_obs.catalog, 'model_2': model_obs, 'exp_2': exp_obs,
                                'time_start': startdate_data, 'time_end': enddate_data}

            # Total bias plot
            result = global_biases.plot_bias(vmin=vmin, vmax=vmax)
            if result:
                fig, ax, netcdf = result
                description = (
                        f"Spatial map of the total bias of the variable {var_name} from {startdate_data} to {enddate_data} "
                        f"for the {model_data} model, experiment {exp_data} from the {reader.catalog} catalog, with {model_obs} "
                        f"(experiment {exp_obs}, catalog {reader_obs.catalog}) used as reference data. "
                    )
                metadata = {"Description": description}
                if save_netcdf:
                    output_saver.save_netcdf(dataset=netcdf, diagnostic_product='total_bias_map', metadata=metadata, **common_save_args)
                if save_pdf:
                    output_saver.save_pdf(fig=fig, diagnostic_product='total_bias_map', metadata=metadata, **common_save_args)
                if save_png:
                    output_saver.save_png(fig=fig, diagnostic_product='total_bias_map', metadata=metadata, **common_save_args)
            else:
                logger.warning(f"Total bias plot not generated for {var_name}.")

            # Seasonal bias plot
            if seasons_bool:
                result = global_biases.plot_seasonal_bias(vmin=vmin, vmax=vmax)
                if result:
                    fig, ax, netcdf = result
                    description = (
                        f"Seasonal bias map of the variable {var_name} for the {model_data} model, experiment {exp_data} "
                        f"from the {reader.catalog} catalog, using {model_obs} (experiment {exp_obs}, catalog {reader_obs.catalog}) as reference data. "
                        f"The bias is computed for each season over the period from {startdate_data} to {enddate_data}, "
                        f"providing insights into seasonal discrepancies between the model and the reference. "
                    )
                    metadata = {"Description": description}
                    if save_netcdf:
                        output_saver.save_netcdf(dataset=netcdf, diagnostic_product='seasonal_bias_map', metadata=metadata, **common_save_args)
                    if save_pdf:
                        output_saver.save_pdf(fig=fig, diagnostic_product='seasonal_bias_map', metadata=metadata, **common_save_args)
                    if save_png:
                        output_saver.save_png(fig=fig, diagnostic_product='seasonal_bias_map', metadata=metadata, **common_save_args)
                else:
                    logger.warning(f"Seasonal bias plot not generated for {var_name}.")

            # Vertical bias plot
            if vertical and 'plev' in data[var_name].dims:
                var_attributes_vert = config["biases_plot_params"]['vertical_plev'].get(var_name, {})
                vmin, vmax = var_attributes_vert.get('vmin'), var_attributes_vert.get('vmax')

                result = global_biases.plot_vertical_bias(var_name=var_name, vmin=vmin, vmax=vmax)
                if result:
                    fig, ax, netcdf = result
                    description = (
                        f"Vertical bias plot of the variable {var_name} across pressure levels, from {startdate_data} to {enddate_data} "
                        f"for the {model_data} model, experiment {exp_data} from the {reader.catalog} catalog, with {model_obs} "
                        f"(experiment {exp_obs}, catalog {reader_obs.catalog}) used as reference data. "
                        f"The vertical bias shows differences in the model's vertical representation compared to the reference, "
                        f"highlighting biases across different pressure levels to assess the accuracy of vertical structures."
                    )
                    metadata = {"Description": description}
                    if save_netcdf:
                        output_saver.save_netcdf(dataset=netcdf, diagnostic_product='vertical_bias', metadata=metadata, **common_save_args)
                    if save_pdf:
                        output_saver.save_pdf(fig=fig, diagnostic_product='vertical_bias', metadata=metadata, **common_save_args)
                    if save_png:
                        output_saver.save_png(fig=fig, diagnostic_product='vertical_bias', metadata=metadata, **common_save_args)
                else:
                    logger.warning(f"Vertical bias plot not generated for {var_name}.")

        except Exception as e:
            logger.error(f"Error processing {var_name}: {e}")

    if client:
        client.close()
        logger.debug("Dask client closed.")

    if private_cluster:
        cluster.close()
        logger.debug("Dask cluster closed.")

    logger.info("Global Biases diagnostic completed.")

if __name__ == '__main__':
    main()
