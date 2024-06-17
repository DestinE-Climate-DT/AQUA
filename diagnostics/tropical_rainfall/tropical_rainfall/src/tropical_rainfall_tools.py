import re
import os
import math
import seaborn as sns
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
from typing import Union
from aqua.util import ConfigPath, load_multi_yaml
from aqua.reader.fixer import FixerMixin
from aqua.logger import log_configure
import yaml
from os.path import isfile, join, exists, isdir
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from collections import defaultdict
from importlib import resources

regrid_dict = {
    'r250': {'deg': 2.5},
    'r200': {'deg': 2.0},
    'r100': {'deg': 1.0},
    'r050': {'deg': 0.5},
    'r025': {'deg': 0.25},
    'r020': {'deg': 0.2},
    'r010': {'deg': 0.1},
    'r005': {'deg': 0.05}
}

class ToolsClass:
    def __init__(self, config_file: str = None, loglevel: str = 'WARNING'):
        """
        Initialize the class.
        """
        if config_file:
            self.config_file = config_file
        else:
            # Default to 'config-tropical-rainfall.yml'
            self.config_file = 'config-tropical-rainfall.yml'
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Tropical Rainfall Tools')

    def split_time(self, time_str: str) -> str:
        """
        Split the time string into parts and recombine them using hyphens.

        Args:
            time_str (str): The input time string.

        Returns:
            str: The time string with parts recombined using hyphens.
        """
        parts = re.split(r"[^a-zA-Z0-9\s]", time_str)
        if len(parts) <= 5:
            time_str = '-'.join(parts[:len(parts)])
        return time_str

    def get_machine(self):
        """
        Retrieves the machine information from the ConfigPath instance.

        Returns:
            str: The machine information retrieved from the ConfigPath object.

        Raises:
            SomeException: If the ConfigPath object is not properly initialized or if there is an issue with
                        retrieving the machine information.
        """
        return ConfigPath().catalog

    def get_netcdf_path(self) -> tuple:
        """
        Load paths from a YAML configuration file based on the specified configuration name.

        Args:
            self: The instance of the class.

        Returns:
            tuple: A tuple containing the paths to the netCDF file, PDF file, and mean file, respectively.

        Raises:
            FileNotFoundError: If the specified configuration file does not exist.
        """
        root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Get the root folder
        config_path = os.path.join(root_folder, self.config_file)  # Construct the absolute path to the config file
        if not os.path.exists(config_path):
            self.logger.error(f"The configuration file '{self.config_file}' does not exist.")
            raise FileNotFoundError(f"The configuration file '{self.config_file}' does not exist.")
        try:
            with open(config_path, 'r') as file:
                data = yaml.safe_load(file)
            machine = ConfigPath().machine
            path_to_netcdf = data[machine]['path_to_netcdf']
        except FileNotFoundError as e:
            # Handle FileNotFoundError exception
            self.logger.error(f"An unexpected error occurred: {e}")
            raise e
        except Exception as e:
            # Handle other exceptions
            self.logger.error(f"An unexpected error occurred: {e}")
            path_to_netcdf = None
        self.logger.info(f"NetCDF folder: {path_to_netcdf}")
        return path_to_netcdf

    def adjust_bins(self, ds, factor):
        """
        Adjusts the histogram bins by a specified factor, recalculating the center of each bin based on the assumption
        that the first bin center is (center_of_bin - 0.5 * width). If factor is None, the function returns a copy of the 
        dataset unchanged.

        Args:
            ds (xarray.Dataset): The dataset containing the histogram.
            factor (float or None): The factor by which to adjust bin widths. Values > 1 increase bin width, 
                                    values < 1 decrease it. None leaves the bin width and counts unchanged.

        Returns:
            xarray.Dataset: A new dataset with adjusted 'counts' and possibly 'center_of_bin' if factor is not None.
        """
        if factor is None:
            # If factor is None, return a copy of the original dataset
            return ds.copy()

        if factor <= 0:
            raise ValueError("Factor must be positive.")

        original_width = ds.width.values[0]  # Assuming uniform width for all bins
        new_width = original_width * factor
        original_centers = ds.center_of_bin.values
        
        # Calculate new bin centers based on the adjusted first bin center
        new_centers = np.array([original_centers[0] - 0.5 * original_width + (0.5 * new_width) + i * new_width for i in range(len(original_centers))])

        # If the factor is meant to decrease bin size, this might result in more bins than originally
        if factor < 1:
            additional_bins = int((original_centers[-1] - new_centers[-1]) / new_width)
            for i in range(1, additional_bins + 1):
                new_centers = np.append(new_centers, new_centers[-1] + new_width)
        
        # Linear interpolation for counts
        new_counts = np.interp(new_centers, original_centers, ds.counts.values, left=0, right=0)

        # Create the adjusted dataset
        adjusted_ds = xr.Dataset({
            'counts': ('center_of_bin', new_counts),
        }, coords={
            'center_of_bin': ('center_of_bin', new_centers),
            'width': ('center_of_bin', np.full(len(new_centers), new_width)),
        })

        # Preserve global attributes
        adjusted_ds.attrs = ds.attrs.copy()
        adjusted_ds.counts.attrs = ds.counts.attrs.copy()
        adjusted_ds.center_of_bin.attrs = ds.center_of_bin.attrs.copy()
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        history_update = str(current_time)+f" the histogram bins adjusted by a specified factor {factor} ;\n "
        if 'history' not in adjusted_ds.attrs:
            adjusted_ds.attrs['history'] = ' '
        history_attr = adjusted_ds.attrs['history'] + history_update
        adjusted_ds.attrs['history'] = history_attr
        return adjusted_ds

    def get_pdf_path(self) -> tuple:
        """
        Load paths from a YAML configuration file based on the specified configuration name.

        Args:
            self: The instance of the class.

        Returns:
            tuple: A tuple containing the paths to the netCDF file, PDF file, and mean file, respectively.

        Raises:
            FileNotFoundError: If the specified configuration file does not exist.
        """
        root_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Get the root folder
        config_path = os.path.join(root_folder, self.config_file)  # Construct the absolute path to the config file
        if not os.path.exists(config_path):
            self.logger.error(f"The configuration file '{self.config_file}' does not exist.")
            raise FileNotFoundError(f"The configuration file '{self.config_file}' does not exist.")
        try:
            with open(config_path, 'r') as file:
                data = yaml.safe_load(file)
            machine = ConfigPath().machine
            path_to_pdf = data[machine]['path_to_pdf']
        except FileNotFoundError as e:
            # Handle FileNotFoundError exception
            self.logger.error(f"An unexpected error occurred: {e}")
            raise e
        except Exception as e:
            # Handle other exceptions
            self.logger.error(f"An unexpected error occurred: {e}")
            path_to_pdf = None
        self.logger.info(f"PDF folder: {path_to_pdf}")
        return path_to_pdf

    def get_config(self, config_file):
        """Load the configuration from a YAML file."""
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
        
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        
        return config

    def get_config_value(self, config, *keys, default=None):
        """Retrieve a nested configuration value."""
        value = config
        try:
            for key in keys:
                value = value[key]
        except KeyError:
            return default
        return value

    def open_dataset(self, path_to_netcdf: str) -> object:
        """
        Function to load a histogram dataset from a file using pickle.

        Args:
            path_to_netcdf (str): The path to the dataset file.

        Returns:
            object: The loaded histogram dataset.

        Raises:
            FileNotFoundError: If the specified dataset file is not found.
        """
        try:
            dataset = xr.open_dataset(path_to_netcdf)
            return dataset
        except FileNotFoundError:
            raise FileNotFoundError(
                "The specified dataset file was not found.")

    def select_files_by_year_and_month_range(self, path_to_histograms: str, start_year: int = None, end_year: int = None,
                                             start_month: int = None, end_month: int = None, flag: str = None) -> list:
        """
        Select files within a specific year and optional month range from a given directory that also contain a certain flag in their filename.
        If no year range is provided, return all files sorted alphabetically that match the flag condition.

        Args:
            path_to_histograms (str): Directory path containing the histogram files.
            start_year (int, optional): Start year of the range (inclusive). Defaults to None.
            end_year (int, optional): End year of the range (inclusive). Defaults to None.
            start_month (int, optional): Start month of the range (inclusive). Defaults to None.
            end_month (int, optional): End month of the range (inclusive). Defaults to None.
            flag (str, optional): A specific flag to look for in the filenames. Defaults to None.

        Returns:
            list: A list of file paths matching the specified year, month range, and flag or all files if no year range is specified and match the flag condition.
        """
        files = [join(path_to_histograms, f) for f in os.listdir(path_to_histograms) if isfile(join(path_to_histograms, f))]
        files.sort()
        if start_year is None and end_year is None and flag is None:
            # If no year range and flag are provided, return all files sorted alphabetically
            return files

        selected_files = []
        for file_path in files:
            # Extract the year and month from the filename
            date_match = re.search(r'(\d{4})-(\d{2})-\d{2}T', file_path)
            # Check if flag is present in the filename if a flag is specified
            flag_present = flag is None or flag in file_path
            if date_match and flag_present:
                year, month = map(int, date_match.groups())
                # Check if the year and optionally month falls within the specified range
                if ((start_year is None or start_year <= year) and 
                    (end_year is None or year <= end_year) and 
                    (not start_month or start_month <= month <= (end_month or 12))):
                    selected_files.append(file_path)
                elif start_year is None and end_year is None:
                    # This line seems to be redundant in the context of flag checking,
                    # since it's already handled by the flag_present check.
                    selected_files.append(file_path)
        return selected_files

    def find_files_with_keys(self, folder_path: str = None, keys: list = None, get_path: bool = False):
        """
        Searches a specified folder for any file names that contain all the provided keys.

        Parameters:
            folder_path (str): The path to the folder where the search for files will be conducted. If None, the current directory is assumed.
            keys (list of str): A list of string keys that must all be present in a file name for it to satisfy the search criteria.

        Returns:
            bool: True if at least one file meeting all specified criteria is found, False otherwise.
        """
        # Check if the folder path exists
        if folder_path is None or not exists(folder_path):
            self.logger.warning(f"Folder path '{folder_path}' does not exist yet or was not provided.")
            return False
        else:
            self.logger.debug(f"The provided path is {folder_path}")

        files = [join(folder_path, f) for f in os.listdir(folder_path) if isfile(join(folder_path, f)) or isdir(join(folder_path, f))]
        files.sort()
        keys = [str(key) for key in keys]
        for filename in files:
            if all(key in filename for key in keys):
                self.logger.debug(f"A file {filename} meeting all specified criteria ({', '.join(keys)}) exists")
                if get_path:
                    self.logger.info(f"Returning the full path of the file {filename}")
                    return filename
                else:
                    self.logger.warning(f"A file {filename} exists. Skipping calculations.")
                    return True
        return False

    def remove_file_if_exists_with_keys(self, folder_path: str, keys: list):
        """
        Searches for and removes the first file in the specified folder that contains all the provided keys in its name.

        Parameters:
            folder_path (str): The path to the folder where to search for and remove the file.
            keys (list of str): A list of string keys that must all be present in the file name for it to be removed.

        Returns:
            bool: True if a file was found and removed, False otherwise.
        """
         # Check if the folder path exists
        if folder_path is None or not exists(folder_path):
            self.logger.warning(f"Folder path '{folder_path}' does not exist yet or was not provided.")
            return False

        files = [join(folder_path, f) for f in os.listdir(folder_path) if isfile(join(folder_path, f)) or isdir(join(folder_path, f))]
        files.sort()
        keys = [str(key) for key in keys]
        for filename in files:
            if all(key in filename for key in keys):
                os.remove(filename)
                self.logger.warning(f"Removed file {filename} that met all specified criteria: {', '.join(keys)}.")
                return True
        return False

    def zoom_in_data(self, trop_lat: float = None,
                     pacific_ocean: bool = False, atlantic_ocean: bool = False, indian_ocean: bool = False,
                     tropical: bool = False) -> tuple:
        """
        Zooms into specific geographical regions or the tropics in the data.

        Args:
            trop_lat (float, optional): The tropical latitude. Defaults to None.
            pacific_ocean (bool, optional): Whether to zoom into the Pacific Ocean. Defaults to False.
            atlantic_ocean (bool, optional): Whether to zoom into the Atlantic Ocean. Defaults to False.
            indian_ocean (bool, optional): Whether to zoom into the Indian Ocean. Defaults to False.
            tropical (bool, optional): Whether to zoom into the tropical region. Defaults to False.

        Returns:
            tuple: A tuple containing the longitude and latitude bounds after zooming.

        Note:
            The longitude and latitude boundaries will be adjusted based on the provided ocean or tropical settings.

        Example:
            lonmin, lonmax, latmin, latmax = zoom_in_data(trop_lat=23.5, atlantic_ocean=True)
        """
        if pacific_ocean:
            latmax = 65
            latmin = -70
            lonmin = -120
            lonmax = 120
        elif atlantic_ocean:
            latmax = 70
            latmin = -60
            lonmin = -70
            lonmax = 20
        elif indian_ocean:
            latmax = 30
            latmin = -60
            lonmin = 20
            lonmax = 120

        if tropical and trop_lat is not None:
            latmax = trop_lat
            latmin = trop_lat
        self.logger.info('The data was zoomed in.')
        return lonmin, lonmax, latmin, latmax

    def improve_time_selection(self, data: Union[xr.DataArray, None] = None, time_selection: Union[str, None] = None) -> str:
        """
        Perform time selection based on the provided criteria.

        Args:
            data (xarray): The input data to be processed.
            time_selection (str): The time selection criteria.

        Returns:
            str: The updated time selection value.

        The function checks if the time selection criteria contains a year and a date in the format 'YYYY-MM-DD'.
        If the input string doesn't include a year or a date, the function appends the necessary values to the string.
        The processed time selection value is then returned.

        Examples:
            >>> time_selection(data=data, time_selection='2023-09-25')
            '2023-09-25'
        """
        if time_selection is not None:
            if not isinstance(time_selection, str):
                time_selection = str(time_selection)

            year_pattern = re.compile(r'\b\d{4}\b')
            match_year = re.search(year_pattern, time_selection)

            if match_year:
                self.logger.debug(f'The input time value for selection contains a year: {time_selection}')
                try:
                    data.sel(time=time_selection)
                except KeyError:
                    self.logger.error('The dataset does not contain the input time value. Choose a different time value.')
            else:
                self.logger.debug(f'The input time value for selection does not contain a year: {time_selection}')
                time_selection = str(data['time.year'][0].values) + '-' + time_selection
                self.logger.debug(f'The new time value for selection is: {time_selection}')

                date_pattern = re.compile(r'\b\d{4}-\d{2}-\d{2}\b')
                match_date = re.search(date_pattern, time_selection)

                if match_date:
                    self.logger.debug(f'The input time value for selection contains a month and a day: {time_selection}')
                    try:
                        data.sel(time=time_selection)
                    except KeyError:
                        self.logger.error('The dataset does not contain the input time value. Choose a different time value.')
                else:
                    time_selection = time_selection + '-' + str(data.sel(time=time_selection)['time.day'][0].values)
                    self.logger.debug(f'The input time value for selection does not contain a day. The new time value \
                                      for selection is: {time_selection}')
        self.logger.info(f'The time value for selection is: {time_selection}')
        return time_selection

    def convert_units(self, value, from_unit, to_unit):
        """
        Convert a length measurement from one unit to another.
        
        Args:
            value (float): The numerical value of the length measurement to be converted.
            from_unit (str): The unit of the original length measurement (e.g., 'm', 'cm', 'mm').
            to_unit (str): The desired unit for the converted length (e.g., 'cm', 'in', 'ft').

        Returns:
            float: The converted length value in the specified unit, accounting for
            the factor and offset needed for the conversion.
        """
        fix = FixerMixin()
        fix.logger = self.logger
        fix.fixer_folder, _ = (ConfigPath().get_reader_filenames())
        fix.fixes_dictionary = load_multi_yaml(fix.fixer_folder)

        factor, offset = fix.convert_units(from_unit, to_unit)

        converted_value = (value * factor) + offset

        return converted_value

    def get_local_time_decimal(self, utc_decimal_hour, longitude):
        # Each degree of longitude corresponds to 4 minutes of time difference
        # Convert that into hours for the calculation (4 minutes = 4/60 hours)
        time_zone_offset_hours = longitude * 4 / 60
        
        # Calculate the local time in decimal hours
        local_decimal_hour = (utc_decimal_hour + time_zone_offset_hours) % 24
        
        return local_decimal_hour

    def update_dict_of_loaded_analyses(self, loaded_dict: dict = None) -> Union[dict, None]:
        """
        Updates a dictionary with loaded data and assigns colors to each entry.

        Args:
            loaded_dict (dict): Dictionary with paths to datasets.

        Returns:
            dict: Updated dictionary with loaded data and colors assigned.
            None: If the provided object is not of type 'dict'.
        """
        if not isinstance(loaded_dict, dict):
            self.logger.error("The provided object must be a 'dict' type.")
            return None

        for key, value in loaded_dict.items():
            if 'path' not in value:
                print(f"Error: 'path' key is missing in the entry with key {key}")

        # Define the number of entries
        num_entries = len(loaded_dict)
        
        # Generate a palette starting at a hue past red (e.g., starting at 30 degrees out of 360)
        palette = sns.husl_palette(n_colors=num_entries, h=0.25) 

        # Assign colors to dictionary entries
        for i, (key, value) in enumerate(loaded_dict.items()):
            loaded_dict[key]["data"] = self.open_dataset(path_to_netcdf=value["path"])
            loaded_dict[key]["color"] = palette[i]

        return loaded_dict


    def add_colors_to_dict(self, loaded_dict: dict = None) -> Union[dict, None]:
        """
        Updates a dictionary with loaded data and assigns colors to each entry.

        Args:
            loaded_dict (dict): Dictionary with paths to datasets.

        Returns:
            dict: Updated dictionary with loaded data and colors assigned.
            None: If the provided object is not of type 'dict'.
        """
        if not isinstance(loaded_dict, dict):
            self.logger.error("The provided object must be a 'dict' type.")
            return None
        
        # Use a custom palette excluding red hues
        num_entries = len(loaded_dict)
        # Exclude red by setting hue range to avoid red (hue near 0)
        palette = sns.husl_palette(n_colors=num_entries, h=0.25)
        # You can also use other palettes like 'viridis', 'Blues', or 'Greens' depending on your needs

        # Loop through the dictionary and assign colors
        for i, (key, value) in enumerate(loaded_dict.items()):
            loaded_dict[key]["color"] = palette[i]

        return loaded_dict

    def time_interpreter(self, dataset):
        """Identifying unit of timestep in the Dataset

        Args:
            dataset (xarray):       The Dataset

        Returns:
            str:                    The unit of timestep in input Dataset
        """

        if dataset['time'].size == 1:
            return 'False. Load more timesteps then one'
        try:
            if np.count_nonzero(dataset['time.second'] == dataset['time.second'][0]) == dataset.time.size:
                if np.count_nonzero(dataset['time.minute'] == dataset['time.minute'][0]) == dataset.time.size:
                    if np.count_nonzero(dataset['time.hour'] == dataset['time.hour'][0]) == dataset.time.size:
                        if np.count_nonzero(dataset['time.day'] == dataset['time.day'][0]) == dataset.time.size or \
                                np.count_nonzero([dataset['time.day'][i] in [1, 28, 29, 30, 31]
                                                  for i in range(0, len(dataset['time.day']))]) == dataset.time.size:

                            if np.count_nonzero(dataset['time.month'] == dataset['time.month'][0]) == dataset.time.size:
                                return 'Y'
                            else:
                                return 'M'
                        else:
                            return 'D'
                    else:
                        timestep = dataset.time[1] - dataset.time[0]
                        n_hours = int(timestep/(60 * 60 * 10**9))
                        return str(n_hours)+'H'
                else:
                    timestep = dataset.time[1] - dataset.time[0]
                    n_minutes = int(timestep/(60 * 10**9))
                    return str(n_minutes)+'m'
            else:
                return 1

        except KeyError and AttributeError:
            timestep = dataset.time[1] - dataset.time[0]
            if timestep >= 28 and timestep <= 31:
                return 'M'

    def check_need_for_time_averaging(self, dataset, target_freq):
        """
        Check if the dataset needs to be time-averaged.

        Args:
            dataset: The dataset to check. This should be an object that the `time_interpreter`
                     method of `ToolsClass` can handle.
            target_freq (str): The target frequency to check against (e.g., 'monthly').

        Returns:
            bool: True if time averaging is needed, False otherwise.

        """
        original_freq = self.time_interpreter(dataset)
        if original_freq == target_freq:
            self.logger.warning('The original dataset does not need to be time-averaged.')
            return False
        else:
            self.logger.warning('The original dataset needs to be time-averaged.')
            return True

    def check_need_for_regridding(self, dataset, regrid, tolerance=0.01):
        """
        Check if the xarray dataset needs to be regridded.

        Args:
            dataset (xarray.Dataset): The dataset to check.
            regrid (str): The regrid key to check against (e.g., 'r200').
            tolerance (float): The tolerance for comparison in degrees.

        Returns:
            bool: True if regridding is needed, False otherwise.

        """
        if 'lat' in dataset.dims and 'lon' in dataset.dims:
            # Assuming the dataset is 2D, calculate the difference in degrees between adjacent latitude and longitude points
            del_lon = abs(dataset.lon[1].values - dataset.lon[0].values)
            del_lat = abs(dataset.lat[1].values - dataset.lat[0].values)

            # Check if both latitude and longitude differences are within the tolerance of the desired regrid resolution
            if math.isclose(del_lon, regrid_dict[regrid]['deg'], abs_tol=tolerance) and \
               math.isclose(del_lat, regrid_dict[regrid]['deg'], abs_tol=tolerance):
                self.logger.warning('The original dataset does not need to be regridded as it already' +
                                    'has the necessary spatial resolution.')
                return False
            else:
                self.logger.warning('The original dataset needs to be regridded.')
                return True
        else:
            self.logger.warning('The original dataset does not contain latitude and longitude coordinates,' +
                                'and needs to be regridded.')
            return True

    def convert_24hour_to_12hour_clock(self, data, ind):
        """ Function to convert 24 hour clock to 12 hour clock

        Args:
            data (xarray):                  The Dataset
            ind (int):                      The index of the timestep

        Returns:
            str:                            The converted timestep
        """
        if data['time.hour'][ind] > 12:
            return str(data['time.hour'][ind].values - 12)+'PM'
        else:
            return str(data['time.hour'][ind].values)+'AM'

    def convert_monthnumber_to_str(self, data, ind):
        """ Function to convert month number to string

        Args:
            data (xarray):                  The Dataset
            ind (int):                      The index of the timestep

        Returns:
            str:                            The converted timestep
        """
        if int(data['time.month'][ind]) == 1:
            return 'J'
        elif int(data['time.month'][ind]) == 2:
            return 'F'
        elif int(data['time.month'][ind]) == 3:
            return 'M'
        elif int(data['time.month'][ind]) == 4:
            return 'A'
        elif int(data['time.month'][ind]) == 5:
            return 'M'
        elif int(data['time.month'][ind]) == 6:
            return 'J'
        elif int(data['time.month'][ind]) == 7:
            return 'J'
        elif int(data['time.month'][ind]) == 8:
            return 'A'
        elif int(data['time.month'][ind]) == 9:
            return 'S'
        elif int(data['time.month'][ind]) == 10:
            return 'O'
        elif int(data['time.month'][ind]) == 11:
            return 'N'
        elif int(data['time.month'][ind]) == 12:
            return 'D'

    def extract_directory_path(self, string):
        """
        Extracts the directory path from a string.
        """
        return "/".join(string.split("/")[:-1]) + "/"

    def parse_time_band(self, time_band):
        """Parse the time_band string into start time, end time, and frequency."""
        parts = time_band.split(', ')
        start_time = np.datetime64(parts[0])
        end_time = start_time  # Assume single time point initially
        freq = None  # Default frequency is None

        # If there's more than one part, it might include end time or frequency
        if len(parts) > 1:
            # Try to identify and set the frequency
            if 'freq=' in parts[-1]:
                freq = parts[-1].split('=')[1]
                end_time = np.datetime64(parts[1]) if len(parts) == 3 else start_time
            else:
                end_time = np.datetime64(parts[1])

        return start_time, end_time, freq

    def determine_common_frequency(self, freq_1, freq_2):
        """Determine the most granular common frequency between two frequencies."""
        # For simplicity, let's assume the only possible frequencies are 'D', 'M', 'Y'
        freq_order = {'D': 1, 'M': 2, 'Y': 3}
        if freq_1 == freq_2:
            return freq_1
        elif not freq_1 or not freq_2:
            # If one is None, return the other
            return freq_1 or freq_2
        else:
            # Return the less granular frequency (e.g., 'M' is less granular than 'D')
            return freq_1 if freq_order[freq_1] > freq_order[freq_2] else freq_2

    def merge_time_bands(self, dataset_1, dataset_2):
        """Merge time bands from two datasets, considering start, end times, and frequency."""
        start_1, end_1, freq_1 = self.parse_time_band(dataset_1.attrs['time_band'])
        start_2, end_2, freq_2 = self.parse_time_band(dataset_2.attrs['time_band'])

        # Determine the earliest start and latest end times
        start_min = min(start_1, start_2)
        end_max = max(end_1, end_2)

        # Determine the most granular common frequency
        common_freq = self.determine_common_frequency(freq_1, freq_2)

        # Construct the merged time_band attribute
        merged_time_band = f"{start_min}"
        if end_max > start_min:  # Add end time if it's a range
            merged_time_band += f", {end_max}"
        if common_freq:  # Add frequency if available
            merged_time_band += f", freq={common_freq}"

        return merged_time_band

    def parse_filename_to_datetime(self, filename):
        """
        Extracts datetimes from the filename, accommodating both single and range date formats.
        """
        pattern = r"(\d{4}-\d{2}-\d{2}T\d{2})_?(\d{4}-\d{2}-\d{2}T\d{2})?(?:_\d+H)?\.nc"
        match = re.search(pattern, filename)
        if match:
            start_time_str, end_time_str = match.groups()
            start_time = datetime.strptime(start_time_str, "%Y-%m-%dT%H")
            end_time = datetime.strptime(end_time_str, "%Y-%m-%dT%H") if end_time_str else start_time
            return start_time, end_time
        else:
            raise ValueError(f"Time information not found in filename: {filename}")

    def check_time_continuity(self, filenames, freq='M'):
        """
        Checks if the time coordinate is continuous for the given filenames and frequency.
        """
        filenames = [os.path.basename(file) for file in filenames]
        times = [self.parse_filename_to_datetime(filename) for filename in filenames]
        times.sort(key=lambda x: x[0])  # Sort by start time
        
        for i in range(len(times) - 1):
            current_start_time, current_end_time = times[i]
            next_start_time, _ = times[i + 1]
            
            if freq == 'M':  # Monthly data
                expected_next_start = current_end_time + relativedelta(months=+1)
                # Adjust for the end of the month
                expected_next_start = expected_next_start.replace(day=1, hour=0)
            elif '3H' in freq:  # 3-hourly data
                expected_next_start = current_end_time + relativedelta(hours=+3)
            
            if next_start_time != expected_next_start:
                self.logger.error(f"Discontinuity in results found: Expected {expected_next_start}, got {next_start_time}")
                return False
            else:
                self.logger.info(f"Continuity of results confirmed: {next_start_time} follows {expected_next_start}")
            
        return True
    
    def check_incomplete_months(self, files):
        filenames = [os.path.basename(file) for file in files]
        for file in filenames:
            # Updated regex to accommodate both filename patterns
            match = re.search(r"(\d{4})-(\d{2})-(\d{2})T\d{2}(?:_(\d{4})-(\d{2})-(\d{2})T\d{2})?_?(?:\dH)?\.nc", file)
            if match:
                start_year, start_month, start_day, end_year, end_month, end_day = match.groups()

                if end_year and end_month and end_day:
                    # If the file has an end date, use it to check completeness
                    start_date = datetime(int(start_year), int(start_month), int(start_day))
                    end_date = datetime(int(end_year), int(end_month), int(end_day))

                    # Calculate the last day of the end month
                    last_day = monthrange(end_date.year, end_date.month)[1]

                    # If the end date is before the last day of the month, it's incomplete
                    if end_date < datetime(int(end_year), int(end_month), last_day):
                        self.logger.debug(f"The month {end_year}-{end_month} may be incomplete.")
            else:
                self.logger.warning(f"Could not match the file name format: {file}.")

        return files

    def check_and_remove_incomplete_months(self, files):
        filenames = [os.path.basename(file) for file in files]
        complete_files_by_month = defaultdict(list)
        incomplete_files_by_month = defaultdict(list)

        # Adjusted regex to match both file name formats
        for file, full_path in zip(filenames, files):
            match = re.search(r"(\d{4})-(\d{2})-(\d{2})T\d{2}(?:_(\d{4})-(\d{2})-(\d{2})T\d{2})?_?(?:\dH)?\.nc", file)
            if match:
                start_year, start_month, start_day, end_year, end_month, end_day = match.groups()

                if end_year and end_month and end_day:
                    # Handle files with both start and end timestamps
                    end_date = datetime(int(end_year), int(end_month), int(end_day))
                    last_day = monthrange(int(end_year), int(end_month))[1]
                    if int(end_day) == last_day:  # Complete file
                        complete_files_by_month[f"{start_year}-{start_month}"].append(full_path)
                    else:  # Incomplete file
                        incomplete_files_by_month[f"{start_year}-{start_month}"].append(full_path)
                else:
                    # Assuming files with a single date are complete month summaries
                    # Add additional checks here if needed
                    complete_files_by_month[f"{start_year}-{start_month}"].append(full_path)
            else:
                self.logger.error(f"Could not match the file name format: {file}")

        # Logic to prioritize complete months and prepare the final list of files
        final_files = []
        for month, paths in complete_files_by_month.items():
            final_files.extend(paths)
            if month in incomplete_files_by_month:
                self.logger.warning(f"Warning: Removing incomplete records for {month} because a complete month file is present.")

        for month, paths in incomplete_files_by_month.items():
            if month not in complete_files_by_month:
                final_files.extend(paths)

        return final_files

    def sanitize_attributes(self, ds, max_attr_length=500):
        """
        Sanitize dataset attributes by truncating long values.

        Parameters:
        - ds: The xarray dataset or data array to be sanitized.
        - max_attr_length: Maximum length of attribute values to retain.
        """
        for attr, value in ds.attrs.items():
            if len(str(value)) > max_attr_length:
                ds.attrs[attr] = str(value)[:max_attr_length] + "... [truncated]"

    def new_time_coordinate(self, data, dummy_data, freq=None, time_length=None, factor=None):
        """ Function to create new time coordinate

        Args:
            data (xarray):                  The Dataset
            dummy_data (xarray):            The Dataset
            freq (str):                     The frequency of the time coordinate
            time_length (int):              The length of the time coordinate
            factor (int):                   The factor of the time coordinate

        Returns:
            pd.date_range:                  The time coordinate
        """
        if data.time.size > 1 and dummy_data.time.size > 1:
            if data['time'][0] > dummy_data['time'][0]:
                starting_time = str(data['time'][0].values)
            elif data['time'][0] <= dummy_data['time'][0]:
                starting_time = str(dummy_data['time'][0].values)

            if freq is None:
                if self.time_interpreter(data) == self.time_interpreter(dummy_data):
                    freq = self.time_interpreter(data)
                else:
                    if (data['time'][1] - data['time'][0]) > (dummy_data['time'][1] - dummy_data['time'][0]):
                        freq = self.time_interpreter(data)
                    else:
                        freq = self.time_interpreter(dummy_data)

            if time_length is None:
                if factor is None:
                    if data['time'][-1] < dummy_data['time'][-1]:
                        final_time = str(data['time'][-1].values)
                    elif data['time'][-1] >= dummy_data['time'][-1]:
                        final_time = str(dummy_data['time'][-1].values)
                    return pd.date_range(start=starting_time, end=final_time, freq=freq)
                elif isinstance(factor, int) or isinstance(factor, float):
                    time_length = data.time.size*abs(factor)
                    return pd.date_range(start=starting_time, freq=freq, periods=time_length)

            else:
                return pd.date_range(start=starting_time, freq=freq, periods=time_length)
        else:
            if data.time == dummy_data.time:
                return data.time
            else:
                raise ValueError(
                    'The two datasets have different time coordinates')

    def new_space_coordinate(self, data, coord_name, new_length):
        """ Function to create new space coordinate

        Args:
            data (xarray):                  The Dataset
            coord_name (str):               The name of the space coordinate
            new_length (int):               The length of the space coordinate

        Returns:
            list:                          The space coordinate
        """
        if data[coord_name][0] > 0:
            old_lenght = data[coord_name][0].values-data[coord_name][-1].values
            delta = (old_lenght-1) / (new_length-1)
            new_coord = [data[coord_name][0].values - i*delta for i in range(0, new_length)]
        else:
            old_lenght = data[coord_name][-1].values - data[coord_name][0].values
            delta = (old_lenght-1) / (new_length-1)
            new_coord = [data[coord_name][0].values + i*delta for i in range(0, new_length)]
        return new_coord

    def space_regrider(self, data, space_grid_factor=None, lat_length=None, lon_length=None):
        """ Function to regrid the space coordinate

        Args:
            data (xarray):                  The Dataset
            space_grid_factor (int):        The factor of the space coordinate
            lat_length (int):               The length of the space coordinate
            lon_length (int):               The length of the space coordinate

        Returns:
            xarray:                        The regridded Dataset
        """
        # work only for lat and lon only for now. Check the line with interpolation command and modify it in the future
        if isinstance(space_grid_factor, (int, float)):
            if space_grid_factor > 0:
                new_dataset = data
                lon_length = int(data.lon.size * space_grid_factor)
                lat_length = int(data.lat.size * space_grid_factor)
                new_lon_coord = self.new_space_coordinate(
                    new_dataset, coord_name='lon', new_length=lon_length)
                new_lat_coord = self.new_space_coordinate(
                    new_dataset, coord_name='lat', new_length=lat_length)
                new_dataset = new_dataset.interp(lon=new_lon_coord, method="linear", kwargs={
                                                "fill_value": "extrapolate"})
                new_dataset = new_dataset.interp(lat=new_lat_coord, method="linear", kwargs={
                                                "fill_value": "extrapolate"})

            elif space_grid_factor < 0:
                space_grid_factor = abs(space_grid_factor)
                new_dataset = data.isel(
                    lat=[i for i in range(0, data.lat.size, int(space_grid_factor))])
                new_dataset = new_dataset.isel(
                    lon=[i for i in range(0, data.lon.size, int(space_grid_factor))])
        else:
            new_dataset = data

        if lon_length is not None and lat_length is not None:
            new_lon_coord = self.new_space_coordinate(
                new_dataset, coord_name='lon', new_length=lon_length)
            new_lat_coord = self.new_space_coordinate(
                new_dataset, coord_name='lat', new_length=lat_length)
            new_dataset = new_dataset.interp(lon=new_lon_coord, method="linear", kwargs={
                                            "fill_value": "extrapolate"})
            new_dataset = new_dataset.interp(lat=new_lat_coord, method="linear", kwargs={
                                            "fill_value": "extrapolate"})

        return new_dataset

    def mirror_dummy_grid(self, data, dummy_data, space_grid_factor=None, time_freq=None, time_length=None,
                          time_grid_factor=None):
        """ Function to mirror the dummy grid

        Args:
            data (xarray):                  The Dataset
            dummy_data (xarray):            The Dataset
            space_grid_factor (int):        The factor of the space coordinate
            time_freq (str):                The frequency of the time coordinate
            time_length (int):              The length of the time coordinate
            time_grid_factor (int):         The factor of the time coordinate

        Returns:
            xarray:                        The regridded Dataset
        """
        # work only for lat and lon only for now. Check the line with interpolation command and modify it in the future
        if 'xarray' in str(type(dummy_data)):
            new_dataset_lat_lon = self.space_regrider(
                data, space_grid_factor=space_grid_factor, lat_length=dummy_data.lat.size, lon_length=dummy_data.lon.size)

            if data.time.size > 1 and dummy_data.time.size > 1:
                new_time_coord = self.new_time_coordinate(data=data, dummy_data=dummy_data, freq=time_freq,
                                                          time_length=time_length, factor=time_grid_factor)
                new_data = new_dataset_lat_lon.interp(
                    time=new_time_coord, method="linear", kwargs={"fill_value": "extrapolate"})
                new_dummy_data = dummy_data.interp(
                    time=new_time_coord, method="linear", kwargs={"fill_value": "extrapolate"})

                return new_data, new_dummy_data
            else:
                return new_dataset_lat_lon, dummy_data

    def data_size(self, data):
        """ Function to get the size of the data

        Args:
            data (xarray):                  The Dataset

        Returns:
            int:                           The size of the data
        """
        if 'DataArray' in str(type(data)):
            _size = data.size
        elif 'Dataset' in str(type(data)):
            _names = list(data.dims)
            _size = 1
            for i in _names:
                _size *= data[i].size
        return _size
