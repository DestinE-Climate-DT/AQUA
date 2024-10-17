###edited

import os
import sys
import warnings
import numpy as np
import xarray as xr
import dask.distributed as dd
from dask.utils import format_bytes
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
from dateutil.parser import parse
from aqua import Reader, util, logger
from datetime import datetime
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.util import create_folder, coord_names
import pandas as pd
from aqua.util import area_selection
from aqua import Reader, plot_single_map


def check_time_span(config, ds, start, end):
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
    aqua_logger = logger.log_configure(log_level=config['log_level'], log_name=config['log_name'])

    # Convert start and end strings to datetime objects
    start_date = datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.strptime(end, '%Y-%m-%d')

    aqua_logger.debug("start_date: %s", start_date)
    aqua_logger.debug("end_date: %s", end_date)

    # Extract the time coordinate from the xarray data
    time_coord = ds['time']

    # Ensure the min and max time coordinates are in datetime format for comparison
    time_min = pd.to_datetime(time_coord.min().values)
    #time_min = time_min.replace(hour=0, minute=0, second=0, microsecond=0)
    time_max = pd.to_datetime(time_coord.max().values)
    #time_max = time_max.replace(hour=0, minute=0, second=0, microsecond=0)


    aqua_logger.debug("time_min: %s", time_min)
    aqua_logger.debug("time_max: %s", time_max)

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
        aqua_logger = logger.log_configure(log_level=config['log_level'], log_name=config['log_name'])
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
            aqua_logger.warning("Time ranges are not equal across models.")

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
    def visualize_subplots(config, ssh_data_dict, fig, axes, contours=21):
        """
        Visualize the SSH variability data as subplots using Cartopy.

        Args:
            config (dict): The configuration dictionary containing the flags for masking the boundaries.
            ssh_data_dict (dict): Dictionary of SSH variability data arrays with model names to visualize.
            fig (plt.Figure): The figure object for the subplots.
            axes (list): List of subplot axes.
            contours (int): Number of contours for plots (default: 21). If 0 or None the pcolormesh is used.
        """
        # Retrieve the masking flags and boundary latitudes from the configuration
        mask_northern_boundary = config.get("mask_northern_boundary", False)
        mask_southern_boundary = config.get("mask_southern_boundary", False)
        northern_boundary_latitude = config.get("northern_boundary_latitude", None)
        southern_boundary_latitude = config.get("southern_boundary_latitude", None)

        for i, (model_name, data) in enumerate(ssh_data_dict.items()):
            if i < len(axes):
                ax = axes[i]
    
                # Apply masking if the model is "ICON" and the flags are enabled with boundary latitudes provided
                if "ICON" in model_name and mask_northern_boundary and northern_boundary_latitude:
                    data = data.where(data.lat < northern_boundary_latitude)
                if "ICON" in model_name and mask_southern_boundary and southern_boundary_latitude:
                    data = data.where(data.lat > southern_boundary_latitude)
               
                lonname, latname = coord_names(data)

                if contours:
                    lon, lat = np.meshgrid(data[lonname].values, data[latname].values)
                    levels = np.linspace(config["subplot_options"]["scale_min"],
                                         config["subplot_options"]["scale_max"], contours)  # XXX num of levels hardcode, could be selected from config
                    contf = ax.contourf(lon, lat, data, transform=ccrs.PlateCarree(), 
                                        levels=levels, extend='both',
                                        cmap=config["subplot_options"]["cmap"],
                                        transform_first=True)  # It was failing with transform_first=False
                else:
                    contf = ax.pcolormesh(data[lonname].values, data[latname].values, data,
                                          transform=ccrs.PlateCarree(), 
                                          vmin=config["subplot_options"]["scale_min"],
                                          vmax=config["subplot_options"]["scale_max"], 
                                          cmap=config["subplot_options"]["cmap"])
          
                ax.set_title(f"{model_name}")
                ax.coastlines()               
                
                # Add a colorbar for each subplot
                cbar = fig.colorbar(contf, ax=ax, orientation='vertical', shrink=0.9)
                cbar.set_label('SSH Variability (m)')


        if len(ssh_data_dict) < len(axes):
            for j in range(len(ssh_data_dict), len(axes)):
                fig.delaxes(axes[j])
        fig.tight_layout()


    @staticmethod
    def visualize_difference(config, ssh_data_dict, fig, axes, contours=21):
        """
        Visualize the difference in SSH variability data between each model and the AVISO model.

        Args:
            config (dict): The configuration dictionary containing the flags for masking the boundaries.
            ssh_data_dict (dict): Dictionary of SSH variability data arrays with model names to visualize.
            fig (plt.Figure): The figure object for the subplots.
            axes (list): List of subplot axes.
            contours (int): Number of contours for plots (default: 21). If 0 or None the pcolormesh is used.
        """
        # Retrieve the masking flags and boundary latitudes from the configuration
        mask_northern_boundary = config.get("mask_northern_boundary", False)
        mask_southern_boundary = config.get("mask_southern_boundary", False)
        northern_boundary_latitude = config.get("northern_boundary_latitude", None)
        southern_boundary_latitude = config.get("southern_boundary_latitude", None)

        # Track whether any non-empty subplot is encountered
        non_empty_subplot_encountered = False
        
        # Get the first model as the reference
        ref_model_name, ref_data = next(iter(ssh_data_dict.items()))

        #to remove large fill values
        ref_data = xr.where(ref_data < 100, ref_data, np.nan)

        for i, (model_name, data) in enumerate(ssh_data_dict.items()):
            if i < len(axes) and model_name != ref_model_name:
                ax = axes[i]
                diff_data = ref_data - data

                if "ICON" in model_name:
                    if mask_northern_boundary and northern_boundary_latitude:
                        diff_data = diff_data.where(data.lat < northern_boundary_latitude)
                    if mask_southern_boundary and southern_boundary_latitude:
                        diff_data = diff_data.where(data.lat > southern_boundary_latitude)

                lonname, latname = coord_names(data)

                if contours:
                    lon, lat = np.meshgrid(data[lonname].values, data[latname].values)
                    levels = np.linspace(-0.4, 0.4, contours)

                    contf = ax.contourf(lon, lat, diff_data, transform=ccrs.PlateCarree(), 
                                        levels=levels, extend='both',
                                        cmap='RdBu', transform_first=True)
                else:
                    contf = ax.pcolormesh(data[lonname], data[latname], diff_data,
                                        transform=ccrs.PlateCarree(),
                                        vmin=-0.4,
                                        vmax=0.4,
                                        cmap="RdBu")

                # ax.set_title(f"{model_name} - Difference from {ref_model_name}")
                ax.coastlines()
                model_name_letters = model_name.split(':')[0]
                ax.set_title(f"{model_name_letters} - Difference from AVISO")
                # Add a colorbar for each subplot
                cbar = fig.colorbar(contf, ax=ax, orientation='vertical', shrink=0.9)
                cbar.set_label('SSH Variability Difference (m)')

        # if len(ssh_data_dict) < len(axes):
        #     for j in range(len(ssh_data_dict), len(axes)):
        #         fig.delaxes(axes[j])
        if not non_empty_subplot_encountered:
            fig.delaxes(axes[0])
            
        fig.tight_layout()

    def region_selection(self, config, ssh_data_dict, fig_format='pdf'):
        
        mask_northern_boundary = config.get("mask_northern_boundary", False)
        mask_southern_boundary = config.get("mask_southern_boundary", False)
        northern_boundary_latitude = config.get("northern_boundary_latitude", None)
        southern_boundary_latitude = config.get("southern_boundary_latitude", None)
    
        for i, (model_name, data) in enumerate(ssh_data_dict.items(), 1):  # Start subplot index from 1
            fig = plt.figure(figsize=(12, 8))  # Create the figure inside the loop
            
            # Apply masking if necessary
            if "ICON" in model_name and mask_northern_boundary and northern_boundary_latitude:
                data = data.where(data.lat < northern_boundary_latitude)
            if "ICON" in model_name and mask_southern_boundary and southern_boundary_latitude:
                data = data.where(data.lat > southern_boundary_latitude)
            
            lon_lim = config.get("region_selection_limits", {}).get("lon_lim")
            lat_lim = config.get("region_selection_limits", {}).get("lat_lim")
    
            ssh_sel = area_selection(data, lon=lon_lim, lat=lat_lim, drop=True)
                
            ax = fig.add_subplot(len(ssh_data_dict), 1, i)  # Use the current value of i

            save_path = os.path.join(self.create_output_directory(config), fig_format)
                
            plot_single_map(ssh_sel, ax=ax, title=f"{config['region name']} - {model_name}", 
                            vmin=config["subplot_options"]["scale_min"], vmax=config["subplot_options"]["scale_max"], 
                            cmap=config["subplot_options"]["cmap"], save=True,
                            outputdir=save_path,
                            filename=f"ssh.region_selection.plot_{i}.{fig_format}",
                            format=fig_format)
                
            # Add a colorbar for each subplot
            # cbar = fig.colorbar(contf, ax=ax, orientation='vertical', shrink=0.9)
            # cbar.set_label('SSH Variability (m)')

            # Save the figure after each iteration
            #save_path = config.get("output_directory", "")  # Retrieve the save path from the config

            #plt.savefig(f"{save_path}/pdf/region_selection_plot_{i}.png")

            #if fig_format == 'pdf':
            #    self.save_subplots_as_pdf(save_path, f"region_selection_plot_{i}.pdf", contf)
            #else:
            #    self.save_subplots_as_png(save_path, f"region_selection_plot_{i}.png", contf)

            #plt.close(fig)


    @staticmethod
    def create_output_directory(config):
        aqua_logger = logger.log_configure(log_level=config['log_level'], log_name=config['log_name'])
        # Check if the output_directory key exists in the config dictionary
        if 'output_directory' in config:
            output_directory = config['output_directory']
            os.makedirs(output_directory, exist_ok=True)
        else:
            aqua_logger.warning(
                "Output directory not found in config file. Outputs will be saved in a directory named 'output' in your current working directory.")
            # Create a directory named 'output' in the current working directory
            output_directory = os.path.join(os.getcwd(), 'output')
            os.makedirs(output_directory, exist_ok=True)

        return output_directory

    @staticmethod
    def save_subplots_as_pdf(output_directory, filename, fig):
        """
        Saves the subplots as a PDF image file.

        Parameters:
            config (dict): The configuration dictionary containing the output directory.
            filename (str): The name of the output file.
            fig (plt.Figure): The figure object containing the subplots.
        """

        file_type_folder = os.path.join(output_directory, "pdf")
        create_folder(file_type_folder)  # Create the folder if it doesn't exist
        output_file = os.path.join(file_type_folder, filename)

        # Save the figure as a PDF file. fig.savefig() or plt.savefig() should accomplish the same task of saving the figure to a file. (DPI = dots per inch)
        fig.savefig(output_file, format='pdf')
    
    @staticmethod
    def save_subplots_as_png(output_directory, filename, fig):
        """
        Saves the subplots as a png file.

        Parameters:
            config (dict): The configuration dictionary containing the output directory.
            filename (str): The name of the output file.
            fig (plt.Figure): The figure object containing the subplots.
        """
        file_type_folder = os.path.join(output_directory, "png")
        create_folder(file_type_folder)  # Create the folder if it doesn't exist
        output_file = os.path.join(file_type_folder, filename)

        # Save the figure as a PNG file. fig.savefig() or plt.savefig() should accomplish the same task of saving the figure to a file. (DPI = dots per inch)
        fig.savefig(output_file, dpi=200, format='png')

    def run(self, nworkers=None,args=None):
        """
        Run the sshVariability.
        """
        # Load the configuration - reading and parsing the YAML configuration file.
        config = self.config
        
        aqua_logger = logger.log_configure(log_level=config['log_level'], log_name=config['log_name'])

        # Now you can use the logger from the aqua module
        #logger = logger.getLogger(config['log_name'])

        # Comparing user timespan inputs across the models
        self.validate_time_ranges(config)

        # Get the format for saving the plots
        fig_format = config.get('fig_format', 'pdf')

        # Get the contours for the plots
        contours = config.get('contours', 0)

        # If nworkers is not provided, use the value from the config
        if nworkers is None:
            nworkers = config.get('nworkers', None)

        # Initialize the Dask cluster
        cluster_config = config['dask_cluster'].copy()
        if nworkers is not None:
            cluster_config['n_workers'] = nworkers
        cluster = dd.LocalCluster(**cluster_config)
        client = dd.Client(cluster)
        # Get the Dask dashboard URL
        aqua_logger.info("Dask Dashboard URL: %s", client.dashboard_link)

        # logger.info(f"Dask Dashboard URL: {client.dashboard_link}")

        workers = client.scheduler_info()["workers"]
        worker_count = len(workers)
        total_memory = format_bytes(
            sum(w["memory_limit"] for w in workers.values() if w["memory_limit"]))
        # total_memory = format_bytes(
        #     sum(config["dask_cluster"]["memory_limit"] for w in workers.values() if "memory_limit" in config["dask_cluster"]))
        memory_text = f"Workers={worker_count}, Memory={total_memory}"
        aqua_logger.info(memory_text)


        regrid_option = getattr(args, 'regrid', None) if args else None
    
        if regrid_option is None:
            # Use the value from the config if args.regrid is not provided
            regrid_option = config['base_model']['regrid']
        reader = Reader(model=config['base_model']['name'], exp=config['base_model']['experiment'],
                source=config['base_model']['source'], regrid=regrid_option, fix=True)
    
                #Assuming aviso_ssh_std_file_path contains the path to the "AVISO_ssh-L4_daily" file
        aviso_ssh_std_file_path = os.path.join(config['output_directory'],"netcdf", "AVISO_ssh-L4_daily_std.nc")

        #taking the dates from the config file
        manual_aviso_dates = config.get('enter_aviso_dates_manually', False)
        # Check if the file exists
        if not manual_aviso_dates and os.path.exists(aviso_ssh_std_file_path):
            aqua_logger.info("aviso data already exists from 1993-01-01 to 2022-06-23 ")
            # Load the precomputed AVISO standard deviation
            aviso_ssh_std = xr.open_dataarray(aviso_ssh_std_file_path)
            aviso_ssh_std_r = reader.regrid(aviso_ssh_std)

            timespan_start = '1993-01-01'
            timespan_end = '2022-06-23'
        
            # Update the SSH data dictionary with the precomputed AVISO standard deviation
            ssh_data_dict = {}
            ssh_data_dict[f"{config['base_model']['name']}:{config['base_model']['experiment']} {timespan_start} to {timespan_end}"] = aviso_ssh_std_r

        else:   
            #Load AVISO data and get its time span
            #idea: think in context of streaming data.
          #Get the value of regrid_option from args or config

            try:
                # regrid_option = args.regrid if args.regrid is not None else config['base_model']['regrid']  
                reader = Reader(model=config['base_model']['name'], exp=config['base_model']['experiment'],
                                source=config['base_model']['source'], regrid=regrid_option, fix=True)
                regrid=regrid_option
            except:
                raise NoObservationError("AVISO data not found.")
    
    
            aviso_cat = reader.retrieve()
    
            aviso_time_min = np.datetime64(aviso_cat.time.min().values)
            aviso_time_max = np.datetime64(aviso_cat.time.max().values)
            aqua_logger.info("AVISO data spans from %s to %s",
                        aviso_time_min, aviso_time_max)
    
            # Absolute dynamic topography, sea_surface_height_above_geoid
            aviso_ssh = aviso_cat['adt']
            #aviso_ssh = reader.regrid(aviso_ssh)
    
            aqua_logger.info("Now computing std on AVISO ssh for the provided timespan")
            # Get the user-defined timespan from the configuration
            timespan_start = config['timespan']['start']
            timespan_end = config['timespan']['end']
            
            if config.get('check_complete_timespan_data', False):
                timespan_start = aviso_time_min
                timespan_end = aviso_time_max
            else:
                
                if not check_time_span(config, aviso_ssh, timespan_start, timespan_end):
                    raise NotEnoughDataError("The time span is not within the range of time steps in the xarray object.")
                    sys.exit(0)
    
            aqua_logger.info("Now computing std on AVISO ssh for the provided timespan")
    
    
            aviso_ssh_std = aviso_ssh.sel(time=slice(
                timespan_start, timespan_end)).std(axis=0).persist()
            print(aviso_ssh_std)
            aviso_ssh_std_r = reader.regrid(aviso_ssh_std)
            # saving the computation in output files
            aqua_logger.info("computation for AVISO ssh complete, saving output file")
            self.save_standard_deviation_to_file(self.create_output_directory(
                config), "AVISO_ssh-L4_daily", aviso_ssh_std)
    
            ssh_data_dict = {}
            # ssh_data_dict[config['base_model']['name']] = aviso_ssh_std
            ssh_data_dict[f"{config['base_model']['name']}:{config['base_model']['experiment']} {timespan_start} to {timespan_end}"] = aviso_ssh_std_r

        # Create a figure and axes for subplots
        fig, axes = plt.subplots(nrows=len(config['models'])+1, ncols=1, figsize=(
            16, 12), subplot_kw={'projection': ccrs.PlateCarree()})
        fig.suptitle("SSH Variability")

        # By applying np.ravel() to the axes object, it flattens the 2-dimensional array into a 1-dimensional array. This means that each subplot is now accessible through a single index, rather than using row and column indices.
        # This reshaping of the axes object allows for easier iteration over the subplots when visualizing or modifying them, as it simplifies the indexing and looping operations.
        axes = np.ravel(axes)

        # Load data and calculate standard deviation for each model
        aqua_logger.info(
            "Now loading data for other models to compare against AVISO ssh variability")
        for model_name in config['models']:

            aqua_logger.info(
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
            aqua_logger.info("%s data spans from %s to %s",
                        model_name['name'], model_data_time_min, model_data_time_max)

            aqua_logger.info("Getting SSH data complete for %s, now computing standard deviation on the default timestamp",
                        model_name['name'])
            # computing std
            
            if config.get('check_complete_timespan_models', False):
                timespan_start = model_data_time_min
                timespan_end = model_data_time_max 
                
            else:
            
                if 'timespan' in model_name and model_name['timespan']:
                    if not check_time_span(config, ssh_data, model_name['timespan'][0], model_name['timespan'][1]):
                        raise NotEnoughDataError("The time span is not within the range of time steps in the xarray object.")
                        sys.exit(0)
                    timespan_start = parse(model_name['timespan'][0])
                    timespan_end = parse(model_name['timespan'][1])

                else:
                    warnings.warn(
                        "Model does not have a custom timespan, using default.", UserWarning)
                    if not check_time_span(config, ssh_data, config['timespan']['start'], config['timespan']['end']):
                        raise NotEnoughDataError("The time span is not within the range of time steps in the xarray object.")
                        sys.exit(0)
                    timespan_start = config['timespan']['start']
                    timespan_end = config['timespan']['end']
            ssh_std_dev_data = ssh_data.sel(time=slice(timespan_start, timespan_end)).std(
                axis=0, keep_attrs=True).persist()
            
            aqua_logger.info("computation complete, saving output file")
            # saving the computation in output files
            model_info = f"{model_name['name']}_{model_name['experiment']}_{model_name['source']}"
            self.save_standard_deviation_to_file(
                self.create_output_directory(config), model_info, ssh_std_dev_data)
            # self.save_standard_deviation_to_file(config['output_directory'], model_name['name'], ssh_std_dev_data)

            aqua_logger.info(
                "output saved, now regridding using the aqua regridder")
            # regridding the data and plotting for visualization
            ssh_std_dev_regrid = reader.regrid(ssh_std_dev_data)
            # ssh_data_dict[model_name['name']] = ssh_std_dev_regrid
            ssh_data_dict[f"{model_name['name']}:{model_name['experiment']} {timespan_start} to {timespan_end}"] = ssh_std_dev_regrid
    
        aqua_logger.info("visualizing the data in subplots")
        # self.visualize_subplots(config, ssh_data_list, fig, axes)
        self.visualize_subplots(config, ssh_data_dict, fig, axes, contours=contours)

        if fig_format == 'pdf':
            aqua_logger.info("Saving plots as a PDF output file")
            self.save_subplots_as_pdf(self.create_output_directory(
                config), "ssh.variability.all_models.pdf", fig)
        else:
            aqua_logger.info("Saving plots as a PNG output file")
            self.save_subplots_as_png(self.create_output_directory(
                config), "ssh.variability.all_models.png", fig)
    
        if config.get('difference_plots', False):
            # Create a figure and axes for subplots
            fig_diff, axes = plt.subplots(nrows=len(config['models'])+1, ncols=1, figsize=(
                16, 12), subplot_kw={'projection': ccrs.PlateCarree()})
            fig.suptitle("SSH Variability difference")
            aqua_logger.info("visualizing the difference data in subplots")
            self.visualize_difference(config, ssh_data_dict, fig_diff, axes, contours=contours)
        
            if fig_format == 'pdf':
                aqua_logger.info("Saving difference plots as a PDF output file")
                self.save_subplots_as_pdf(self.create_output_directory(
                    config), "ssh.variability_difference.all_models.pdf", fig_diff)
            else:
                aqua_logger.info("Saving difference plots as a PNG output file")
                self.save_subplots_as_png(self.create_output_directory(
                    config), "ssh.variability_difference.all_models.png", fig_diff)

        if config.get('region_selection', False):
            # Create a figure and axes for subplots
            aqua_logger.info("visualizing the selected region data and saving plots")
            self.region_selection(config, ssh_data_dict, fig_format=fig_format)

        aqua_logger.info("Finished SSH diagnostic.")

        # Close the Dask client and cluster
        client.close()
        cluster.close()


if __name__ == '__main__':
    analyzer = sshVariability('config.yaml')
    analyzer.run()
