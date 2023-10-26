import os
import sys
import warnings
import numpy as np
import dask.distributed as dd
from dask.utils import format_bytes
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from dateutil.parser import parse
from aqua import Reader, util, logger
from datetime import datetime
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
import pandas as pd

logger = logger.log_configure(log_level='INFO', log_name='ssh_logger')

def check_time_span(ds, start, end):
    """
    Check if the required time span defined by start and end is within the 
    time span of the xarray data.

    Parameters:
    - ds: xarray Dataset or DataArray with a time coordinate.
    - start: Start of the required time span.
    - end: End of the required time span.

    Returns:
    - bool: True if the required time span is within the xarray data's time span, False otherwise.
    """

    # Convert start and end strings to datetime objects
    start_date = datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.strptime(end, '%Y-%m-%d')

    logger.debug("start_date: %s", start_date)
    logger.debug("end_date: %s", end_date)

    # Extract the time coordinate from the xarray data
    time_coord = ds['time']

    # Ensure the min and max time coordinates are in datetime format for comparison
    time_min = pd.to_datetime(time_coord.min().values)
    time_max = pd.to_datetime(time_coord.max().values)

    logger.debug("time_min: %s", time_min)
    logger.debug("time_max: %s", time_max)

    # Check if the required time span is within the xarray data's time span
    return start_date >= time_min and end_date <= time_max

class sshVariability():
    def __init__(self, config_file):
        """
        Initialize the sshVariability.

        Args:
            config_file (str): Path to the YAML configuration file.
        """
        self.config = util.load_yaml(config_file)

    # static method is a method that belongs to the class itself rather than an instance of the class. Unlike regular methods, static methods don't have access to the instance or its attributes and don't require the self parameter.

    @staticmethod
    def validate_time_ranges(config):
        """
        Validate the time ranges for each model in the configuration.
        Raises a warning if the time ranges are not equal across models.

        Args:
            config (dict): Configuration dictionary.
        """
        # time_ranges is a list that contains the time differences for each model's time range.
        # time_ranges = [parse(model['timespan'][0]) - parse(model['timespan'][1]) for model in config['models']]
        time_ranges = []
        for model in config['models']:
            if model.get('timespan'):
                start_time = parse(model['timespan'][0])
                end_time = parse(model['timespan'][1])
                time_ranges.append(start_time - end_time)

        # set(time_ranges) creates a set from the time_ranges list, removing any duplicate values. A set is an unordered collection of unique elements.
        # len(set(time_ranges)) gives the number of unique elements in the set, which indicates the number of different time ranges present in the time_ranges list.
        # len(set(time_ranges)) > 1 checks if there is more than one unique time range in the time_ranges list.
        # If the condition is true, it means that the time ranges are not equal across models, and further action can be taken based on this information.
        if len(set(time_ranges)) > 1:
            logger.warning("Time ranges are not equal across models.")

    @staticmethod
    def save_standard_deviation_to_file(output_directory, model_name, std_dev_data):
        """
        Save the standard deviation data to a NetCDF file.

        Args:
            output_directory (str): Directory to save the output file.
            model_name (str): Name of the model.
            std_dev_data (xarray.DataArray): Computed standard deviation data.
        """
        # Create the file type folder within the output directory
        file_type_folder = os.path.join(output_directory, "netcdf")
        os.makedirs(file_type_folder, exist_ok=True)

        # Set the output file path
        output_file = os.path.join(file_type_folder, f"{model_name}_std.nc")
        # output_file = os.path.join(output_directory, f"{model_name}_std.nc")
        std_dev_data.to_netcdf(output_file)

    @staticmethod
    def visualize_subplots(config, ssh_data_dict, fig, axes):
        """
        Visualize the SSH variability data as subplots using Cartopy.

        Args:
            config (dict): The configuration dictionary containing the flags for masking the boundaries.
            ssh_data_dict (dict): Dictionary of SSH variability data arrays with model names to visualize.
            fig (plt.Figure): The figure object for the subplots.
            axes (list): List of subplot axes.
        """
        # Retrieve the masking flags and boundary latitudes from the configuration
        mask_northern_boundary = config.get("mask_northern_boundary", False)
        mask_southern_boundary = config.get("mask_southern_boundary", False)
        northern_boundary_latitude = config.get(
            "northern_boundary_latitude", None)
        southern_boundary_latitude = config.get(
            "southern_boundary_latitude", None)

        # cmap_reversed = plt.cm.inferno.reversed()

        for i, (model_name, data) in enumerate(ssh_data_dict.items()):
            if i < len(axes):
                ax = axes[i]

                # Apply masking if the model is "ICON" and the flags are enabled with boundary latitudes provided
                if "ICON" in model_name and mask_northern_boundary and northern_boundary_latitude:
                    data = data.where(data.lat < northern_boundary_latitude)
                if "ICON" in model_name and mask_southern_boundary and southern_boundary_latitude:
                    data = data.where(data.lat > southern_boundary_latitude)

                data.plot(ax=ax, transform=ccrs.PlateCarree(
                ), vmin=config["subplot_options"]["scale_min"], vmax=config["subplot_options"]["scale_max"], cmap=config["subplot_options"]["cmap"])
                # data.plot(ax=ax, transform=ccrs.PlateCarree(), vmin=config["subplot_options"]["scale_min"], vmax=config["subplot_options"]["scale_max"], cmap=cmap_reversed)
                ax.set_title(f"{model_name}")
                ax.coastlines()

        if len(ssh_data_dict) < len(axes):
            for j in range(len(ssh_data_dict), len(axes)):
                fig.delaxes(axes[j])
        fig.tight_layout()

    @staticmethod
    def create_output_directory(config):
        # Check if the output_directory key exists in the config dictionary
        if 'output_directory' in config:
            output_directory = config['output_directory']
            os.makedirs(output_directory, exist_ok=True)
        else:
            logger.warning(
                "Output directory not found in config file. Outputs will be saved in a directory named 'output' in your current working directory.")
            # Create a directory named 'output' in the current working directory
            output_directory = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_directory, exist_ok=True)

        return output_directory

    @staticmethod
    def save_subplots_as_jpeg(output_directory, filename, fig):
        """
        Saves the subplots as a JPEG image file.

        Parameters:
            config (dict): The configuration dictionary containing the output directory.
            filename (str): The name of the output file.
            fig (plt.Figure): The figure object containing the subplots.
        """

        # Set the output file path
        output_file = os.path.join(output_directory, filename)

        # Save the figure as a JPEG file. fig.savefig() or plt.savefig() should accomplish the same task of saving the figure to a file. (DPI = dots per inch)
        fig.savefig(output_file, dpi=500, format='jpeg')

    @staticmethod
    def save_subplots_as_pdf(output_directory, filename, fig):
        """
        Saves the subplots as a PDF file.

        Parameters:
            config (dict): The configuration dictionary containing the output directory.
            filename (str): The name of the output file.
            fig (plt.Figure): The figure object containing the subplots.
        """
        # Create the file type folder within the output directory
        file_type_folder = os.path.join(output_directory, "pdf")
        os.makedirs(file_type_folder, exist_ok=True)

        # Set the output file path
        output_file = os.path.join(file_type_folder, filename)

        # Save the figure as a PDF file. fig.savefig() or plt.savefig() should accomplish the same task of saving the figure to a file. (DPI = dots per inch)
        fig.savefig(output_file, dpi=500, format='pdf')

    def run(self):
        """
        Run the sshVariability.
        """
        # Load the configuration - reading and parsing the YAML configuration file.
        config = self.config

        # Comparing user timespan inputs across the models
        self.validate_time_ranges(config)

        # Initialize the Dask cluster
        cluster = dd.LocalCluster(**config['dask_cluster'])
        client = dd.Client(cluster)
        # Get the Dask dashboard URL
        logger.info("Dask Dashboard URL: %s", client.dashboard_link)

        # logger.info(f"Dask Dashboard URL: {client.dashboard_link}")

        workers = client.scheduler_info()["workers"]
        worker_count = len(workers)
        total_memory = format_bytes(
            sum(w["memory_limit"] for w in workers.values() if w["memory_limit"]))
        memory_text = f"Workers={worker_count}, Memory={total_memory}"
        logger.info(memory_text)

        # Load AVISO data and get its time span
        # idea: think in context of streaming data.
        try:
            reader = Reader(model=config['base_model']['name'], exp=config['base_model']
                        ['experiment'], source=config['base_model']['source'], fix=True)
        except:
            raise NoObservationError("AVISO data not found.")

        aviso_cat = reader.retrieve()

        aviso_time_min = np.datetime64(aviso_cat.time.min().values)
        aviso_time_max = np.datetime64(aviso_cat.time.max().values)
        logger.info("AVISO data spans from %s to %s",
                    aviso_time_min, aviso_time_max)

        # Absolute dynamic topography, sea_surface_height_above_geoid
        aviso_ssh = aviso_cat['adt']

        logger.info("Now computing std on AVISO ssh for the provided timespan")
        # Get the user-defined timespan from the configuration
        timespan_start = config['timespan']['start']
        timespan_end = config['timespan']['end']

        if not check_time_span(aviso_ssh, timespan_start, timespan_end):
            raise NotEnoughDataError("The time span is not within the range of time steps in the xarray object.")
            sys.exit(0)

        logger.info("Now computing std on AVISO ssh for the provided timespan")


        aviso_ssh_std = aviso_ssh.sel(time=slice(
            timespan_start, timespan_end)).std(axis=0).persist()
        # saving the computation in output files
        logger.info("computation for AVISO ssh complete, saving output file")
        self.save_standard_deviation_to_file(self.create_output_directory(
            config), "AVISO_ssh-L4_daily", aviso_ssh_std)

        ssh_data_dict = {}
        # ssh_data_dict[config['base_model']['name']] = aviso_ssh_std
        ssh_data_dict[f"{config['base_model']['name']}:{config['base_model']['experiment']} {timespan_start} to {timespan_end}"] = aviso_ssh_std

        # Create a figure and axes for subplots
        fig, axes = plt.subplots(nrows=len(config['models'])+1, ncols=1, figsize=(
            12, 8), subplot_kw={'projection': ccrs.PlateCarree()})
        fig.suptitle("SSH Variability")

        # By applying np.ravel() to the axes object, it flattens the 2-dimensional array into a 1-dimensional array. This means that each subplot is now accessible through a single index, rather than using row and column indices.
        # This reshaping of the axes object allows for easier iteration over the subplots when visualizing or modifying them, as it simplifies the indexing and looping operations.
        axes = np.ravel(axes)

        # Load data and calculate standard deviation for each model
        logger.info(
            "Now loading data for other models to compare against AVISO ssh variability")
        for model_name in config['models']:

            logger.info(
                "initializing AQUA reader to read the model inputs for %s", model_name)

            try:
                reader = Reader(model=model_name['name'], exp=model_name['experiment'],
                            source=model_name['source'], regrid=model_name['regrid'], zoom=model_name['zoom'], fix=True)
            except:
                raise NoDataError("Model data not found.")
            
            model_data = reader.retrieve()

            ssh_data = model_data[model_name['variable']]

            model_data_time_min = np.datetime64(model_data.time.min().values)
            model_data_time_max = np.datetime64(model_data.time.max().values)
            logger.info("%s data spans from %s to %s",
                        model_name['name'], model_data_time_min, model_data_time_max)

            logger.info("Getting SSH data complete for %s, now computing standard deviation on the default timestamp",
                        model_name['name'])
            # computing std
            if 'timespan' in model_name and model_name['timespan']:
                if not check_time_span(ssh_data, model_name['timespan'][0], model_name['timespan'][1]):
                    raise NotEnoughDataError("The time span is not within the range of time steps in the xarray object.")
                    sys.exit(0)
                timespan_start = parse(model_name['timespan'][0])
                timespan_end = parse(model_name['timespan'][1])

            else:
                warnings.warn(
                    "Model does not have a custom timespan, using default.", UserWarning)
                if not check_time_span(ssh_data, config['timespan']['start'], config['timespan']['end']):
                    raise NotEnoughDataError("The time span is not within the range of time steps in the xarray object.")
                    sys.exit(0)
                timespan_start = config['timespan']['start']
                timespan_end = config['timespan']['end']

            ssh_std_dev_data = ssh_data.sel(time=slice(timespan_start, timespan_end)).std(
                axis=0, keep_attrs=True).persist()
            


            logger.info("computation complete, saving output file")
            # saving the computation in output files
            model_info = f"{model_name['name']}_{model_name['experiment']}_{model_name['source']}"
            self.save_standard_deviation_to_file(
                self.create_output_directory(config), model_info, ssh_std_dev_data)
            # self.save_standard_deviation_to_file(config['output_directory'], model_name['name'], ssh_std_dev_data)

            logger.info(
                "output saved, now regridding using the aqua regridder")
            # regridding the data and plotting for visualization
            ssh_std_dev_regrid = reader.regrid(ssh_std_dev_data)
            # ssh_data_dict[model_name['name']] = ssh_std_dev_regrid
            ssh_data_dict[f"{model_name['name']}:{model_name['experiment']} {model_name['timespan'][0]} to {model_name['timespan'][1]}"] = ssh_std_dev_regrid

        logger.info("visualizing the data in subplots")
        # self.visualize_subplots(config, ssh_data_list, fig, axes)
        self.visualize_subplots(config, ssh_data_dict, fig, axes)

        logger.info("Saving plots as a PDF output file")
        # self.save_subplots_as_jpeg(config, "subplots_output.jpeg", fig)
        self.save_subplots_as_pdf(self.create_output_directory(
            config), "ssh_all_models_ssh-variablity.pdf", fig)

        # Close the Dask client and cluster
        client.close()
        cluster.close()


if __name__ == '__main__':
    analyzer = sshVariability('config.yml')
    analyzer.run()
