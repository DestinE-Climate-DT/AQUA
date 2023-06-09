import yaml
import xarray as xr
import numpy as np
import os
import dask.distributed as dd
from dask.utils import ensure_dict, format_bytes

import cartopy.crs as ccrs
import matplotlib.pyplot as plt

import warnings
from dateutil.parser import parse

# hack to access aqua
import sys
# command below takes to the parent directory
sys.path.append("../..") 
import aqua
from aqua import Reader, catalogue


class sshVariability():
    def __init__(self, config_file):
        """
        Initialize the sshVariability.

        Args:
            config_file (str): Path to the YAML configuration file.
        """
        self.config = self.load_config(config_file)

    # static method is a method that belongs to the class itself rather than an instance of the class. Unlike regular methods, static methods don't have access to the instance or its attributes and don't require the self parameter.
    
    @staticmethod
    def load_config(file_path):
        """
        Load the configuration from a YAML file (read and parse the file).
        
        Args:
            file_path (str): Path to the YAML configuration file.
        
        Returns:
            dict: Configuration dictionary.
        """
        with open(file_path, "r") as config_file:
            config = yaml.safe_load(config_file)
        return config

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
        # len(set(time_ranges)) > 1 checks if there is more than one unique time range in the time_ranges list. If the condition is true, it means that the time ranges are not equal across models, and further action can be taken based on this information.
        if len(set(time_ranges)) > 1:
            warnings.warn("Time ranges are not equal across models.", UserWarning)

    
    @staticmethod
    def save_standard_deviation_to_file(output_directory, model_name, std_dev_data):
        """
        Save the standard deviation data to a NetCDF file.

        Args:
            output_directory (str): Directory to save the output file.
            model_name (str): Name of the model.
            std_dev_data (xarray.DataArray): Computed standard deviation data.
        """
        output_file = os.path.join(output_directory, f"{model_name}_std_dev.nc")
        std_dev_data.to_netcdf(output_file)

    @staticmethod
    def visualize_subplots(config, ssh_data_list, fig, axes):
        """
        Visualize the SSH variability data as subplots using Cartopy.

        Args:
            ssh_data_list (list): List of SSH variability data arrays to visualize.
            axes (list): List of subplot axes.
        """
        for i, data in enumerate(ssh_data_list):
            if i < len(axes):
                ax = axes[i]
                ax.set_title(f"Model {i+1}")
                ax.coastlines()
                data.plot(ax=ax, transform=ccrs.PlateCarree(), vmin=config["subplot_options"]["scale_min"], vmax=config["subplot_options"]["scale_max"])

        if len(ssh_data_list) < len(axes):
            for j in range(len(ssh_data_list), len(axes)):
                fig.delaxes(axes[j])
        fig.tight_layout()

    @staticmethod
    def save_subplots_as_jpeg(config, filename, fig):
        """
        Saves the subplots as a JPEG image file.

        Parameters:
            config (dict): The configuration dictionary containing the output directory.
            filename (str): The name of the output file.
            fig (plt.Figure): The figure object containing the subplots.
        """
        output_directory = config['output_directory']
        
        # Create the output directory if it doesn't exist
        # os.makedirs(output_directory, exist_ok=True)
        
        # Set the output file path
        output_file = os.path.join(output_directory, filename)
        
        # Save the figure as a JPEG file. fig.savefig() or plt.savefig() should accomplish the same task of saving the figure to a file. (DPI = dots per inch)
        fig.savefig(output_file, dpi=300, format='jpeg')

    def run(self):
        """
        Run the sshVariability.
        """
        # Load the configuration - reading and parsing the YAML configuration file.
        config = self.load_config("../config.yml")
        
        # Comparing user timespan inputs across the models
        self.validate_time_ranges(config)

        # Initialize the Dask cluster
        cluster = dd.LocalCluster(**config['dask_cluster'])
        client = dd.Client(cluster)
        print(f"Dask Dashboard URL: {client.dashboard_link}")

        workers = client.scheduler_info()["workers"]
        worker_count = len(workers)
        total_memory = format_bytes(sum(w["memory_limit"] for w in workers.values() if w["memory_limit"]))
        memory_text = f"Workers={worker_count}, Memory={total_memory}"
        print(memory_text)

        # Load AVISO data and get its time span
        # idea: think in context of streaming data.
        reader = Reader(model=config['base_model']['name'], exp=config['base_model']['experiment'], source=config['base_model']['source'])
        aviso_cat = reader.retrieve(fix=False)
        aviso_time_min = np.datetime64(aviso_cat.time.min().values)
        aviso_time_max = np.datetime64(aviso_cat.time.max().values)
        print("AVISO data spans from ",aviso_time_min, "to ", aviso_time_max)
        
        aviso_ssh = aviso_cat['adt']
        
        print("Now computing std on AVISO ssh for the provided timespan")
        # Get the user-defined timespan from the configuration
        timespan_start = config['timespan']['start']
        timespan_end = config['timespan']['end']
        aviso_ssh_std = aviso_ssh.sel(time=slice(timespan_start, timespan_end)).std(axis=0).persist()
        # saving the computation in output files
        print("computation for AVISO ssh complete, saving output file")
        self.save_standard_deviation_to_file(config['output_directory'], "AVISO", aviso_ssh_std)
        
        ssh_data_list = []
        ssh_data_list.append(aviso_ssh_std)

        # Create a figure and axes for subplots
        fig, axes = plt.subplots(nrows=len(config['models'])+1, ncols=1, figsize=(10, 6), subplot_kw={'projection': ccrs.PlateCarree()})

        # By applying np.ravel() to the axes object, it flattens the 2-dimensional array into a 1-dimensional array. This means that each subplot is now accessible through a single index, rather than using row and column indices.
        # This reshaping of the axes object allows for easier iteration over the subplots when visualizing or modifying them, as it simplifies the indexing and looping operations.
        axes = np.ravel(axes)

        # Load data and calculate standard deviation for each model
        print("Now loading data for other models to compare against AVISO ssh variability")
        for model_name in config['models']:
            
            print("initializing AQUA reader to read the model inputs for {}".format(model_name))
            reader = Reader(model=model_name['name'], exp=model_name['experiment'], source=model_name['source'], regrid=model_name['regrid'], zoom=model_name['zoom'])
            model_data = reader.retrieve(fix=False)
            
            ssh_data = model_data[model_name['variable']]
            
            print("Getting SSH data complete for {}, now computing standard deviation on the default timestamp".format(model_name['name']))
            # computing std
            if 'timespan' in model_name and model_name['timespan']:
                timespan_start = parse(model_name['timespan'][0])
                timespan_end = parse(model_name['timespan'][1])
            else:
                warnings.warn("Model does not have a custom timespan, using default.", UserWarning)
                timespan_start = config['timespan']['start']
                timespan_end = config['timespan']['end']
            ssh_std_dev_data = ssh_data.sel(time=slice(timespan_start, timespan_end)).std(axis=0).persist()
            
            print("computation complete, saving output file")
            # saving the computation in output files
            self.save_standard_deviation_to_file(config['output_directory'], model_name['name'], ssh_std_dev_data)
            
            print("output saved, now regridding using the aqua regridder")
            # regridding the data and plotting for visualization
            ssh_std_dev_regrid = reader.regrid(ssh_std_dev_data)
            ssh_data_list.append(ssh_std_dev_regrid)

        print("visualizing the data in subplots")
        self.visualize_subplots(ssh_data_list, fig, axes)

        print("Saving plots as JPEG output file")
        self.save_subplots_as_jpeg(config, "subplots_output.jpeg", fig)

        # Close the Dask client and cluster
        client.close()
        cluster.close()


if __name__ == '__main__':
    analyzer = sshVariability('config.yml')
    analyzer.run()

