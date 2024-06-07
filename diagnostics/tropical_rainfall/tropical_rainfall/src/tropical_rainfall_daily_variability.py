import xarray as xr
from typing import Union, Tuple, Optional, Any, List
import matplotlib.pyplot as plt
from aqua.logger import log_configure

from .tropical_rainfall_plots import PlottingClass
from .tropical_rainfall_tools import ToolsClass
from .tropical_rainfall_data_manager import TropicalPrecipitationDataManager

class DailyVariabilityClass: 
    """This class is a minimal version of the Tropical Precipitation Diagnostic."""

    def __init__(self,
                 trop_lat: Optional[float] = None,
                 s_time: Union[str, int, None] = None,
                 f_time: Union[str, int, None] = None,
                 s_year: Optional[int] = None,
                 f_year: Optional[int] = None,
                 s_month: Optional[int] = None,
                 f_month: Optional[int] = None,
                 num_of_bins: Optional[int] = None,
                 first_edge: Optional[float] = None,
                 width_of_bin: Optional[float] = None,
                 bins: Optional[list] = None,
                 new_unit: Optional[str] = None,
                 model_variable: Optional[str] = None,
                 path_to_netcdf: Optional[str] = None,
                 path_to_pdf: Optional[str] = None,
                 loglevel: str = 'WARNING'):
        """ The constructor of the class.

        Args:
            trop_lat (float, optional): The latitude of the tropical zone. Defaults to 10.
            s_time (Union[str, int, None], optional): The start time of the time interval. Defaults to None.
            f_time (Union[str, int, None], optional): The end time of the time interval. Defaults to None.
            s_year (Union[int, None], optional): The start year of the time interval. Defaults to None.
            f_year (Union[int, None], optional): The end year of the time interval. Defaults to None.
            s_month (Union[int, None], optional): The start month of the time interval. Defaults to None.
            f_month (Union[int, None], optional): The end month of the time interval. Defaults to None.
            num_of_bins (Union[int, None], optional): The number of bins. Defaults to None.
            first_edge (float, optional): The first edge of the bin. Defaults to 0.
            width_of_bin (Union[float, None], optional): The width of the bin. Defaults to None.
            bins (list, optional): The bins. Defaults to 0.
            new_unit (str, optional): The unit for the new data. Defaults to 'mm/day'.
            model_variable (str, optional): The name of the model variable. Defaults to 'mtpr'.
            path_to_netcdf (Union[str, None], optional): The path to the netCDF file. Defaults to None.
            path_to_pdf (Union[str, None], optional): The path to the PDF file. Defaults to None.
            loglevel (str, optional): The log level for logging. Defaults to 'WARNING'.
        """

        self.trop_lat = trop_lat
        self.s_time = s_time
        self.f_time = f_time
        self.s_year = s_year
        self.f_year = f_year
        self.s_month = s_month
        self.f_month = f_month
        self.num_of_bins = num_of_bins
        self.first_edge = first_edge
        self.bins = bins
        self.new_unit = new_unit
        self.model_variable = model_variable
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Trop. Rainfall')
        self.plots = PlottingClass(loglevel=loglevel)
        self.tools = ToolsClass(loglevel=loglevel)
        self.path_to_netcdf = self.tools.get_netcdf_path() if path_to_netcdf is None else path_to_netcdf
        self.path_to_pdf = self.tools.get_pdf_path() if path_to_pdf is None else path_to_pdf
        self.datamanager = TropicalPrecipitationDataManager(trop_lat=self.trop_lat, s_time=self.s_time, f_time=self.f_time,
                                                            s_year=self.s_year, f_year=self.f_year, s_month=self.s_month, f_month=self.f_month,
                                                            num_of_bins=self.num_of_bins, first_edge=self.first_edge,
                                                            width_of_bin=None, bins=self.bins, new_unit=self.new_unit, model_variable=self.model_variable,
                                                            path_to_netcdf=self.path_to_netcdf, path_to_pdf=self.path_to_pdf, loglevel=self.loglevel)
        
        
    def add_localtime(self, data, model_variable: str = None, space_grid_factor: int = None,
                      time_length: int = None, trop_lat: float = None, new_unit: str = None,
                      path_to_netcdf: str = None, name_of_file: str = None, rebuild: bool = False,
                      tqdm_enabled: bool = False) -> Union[xr.Dataset, None]:
        """
        Add a new dataset with local time based on the provided data.

        The function processes the data by selecting specific dimensions, calculating means, and applying space regridding.
        It then computes the local time for each longitude value and adds it to the dataset.
        It also converts the data to a new unit if specified and saves the dataset to a NetCDF file.

        Args:
            data: The input data to be processed.
            model_variable (str): The variable from the model to be used in the process.
            space_grid_factor (int): The factor for space regridding.
            time_length (int): The length of the time dimension to be selected.
            trop_lat (float): The tropical latitude value to be used.
            new_unit (str): The new unit to which the data should be converted.
            path_to_netcdf (str): The path to the NetCDF file to be saved.
            name_of_file (str): The name of the file to be saved.
            tqdm_enabled (bool): A flag indicating whether to display the progress bar.

        Returns:
            xr.Dataset: The new dataset with added local time.
            None: If the path_to_netcdf or name_of_file is not provided.
        """
        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        try:
            data = data[self.model_variable]
        except KeyError:
            pass

        # Extract latitude range and calculate mean
        data_final_grid = data.sel(lat=slice(-self.trop_lat, self.trop_lat))
        data = data_final_grid.mean('lat')

        # Slice time dimension if specified
        if time_length is not None:
            data = data.isel(time=slice(0, time_length))

        # Perform space regridding if space_grid_factor is specified
        if space_grid_factor is not None:
            data = self.tools.space_regrider(data, lon_length=space_grid_factor * data.lon.size)

        local_data = []

        # Display progress bar if tqdm_enabled
        progress_bar_template = "[{:<40}] {}%"
        for time_ind in range(data.time.size):
            local_data.append([])
            for lon_ind in range(data.lon.size):
                total_ind = time_ind * data.lon.size + lon_ind
                ratio = total_ind / (data.lon.size * data.time.size)
                progress = int(40 * ratio)
                if tqdm_enabled:
                    print(progress_bar_template.format("=" * progress, int(ratio * 100)), end="\r")

                utc_time = data.time[time_ind]
                longitude = data.lon[lon_ind].values - 180
                local_time = float(utc_time['time.hour'].values + utc_time['time.minute'].values / 60)
                local_element = self.tools.get_local_time_decimal(longitude=longitude, utc_decimal_hour=local_time)
                local_data[time_ind].append(local_element)

        # Create an xarray DataArray for utc_data
        local_data_array = xr.DataArray(local_data, dims=('time', 'lon'), coords={'time': data.time, 'lon': data.lon})

        # Create a new dataset with mtpr and utc_time
        new_dataset = xr.Dataset({'mtpr': data, 'local_time': local_data_array})
        new_dataset.attrs = data.attrs
        new_dataset = self.datamanager.grid_attributes(data=data_final_grid, mtpr_dataset=new_dataset)
        # Calculate relative mtpr and add to the dataset
        mean_val = new_dataset['mtpr'].mean()
        new_dataset['mtpr_relative'] = (new_dataset['mtpr'] - mean_val) / mean_val
        new_dataset['mtpr_relative'].attrs = new_dataset.attrs

        if path_to_netcdf is None and self.path_to_netcdf is not None:
                path_to_netcdf = self.path_to_netcdf+'daily_variability/'
        
        if name_of_file is not None:
            self.datamanager.dataset_to_netcdf(
                new_dataset, path_to_netcdf=path_to_netcdf, name_of_file=name_of_file+'_daily_variability', rebuild=rebuild)
        return new_dataset

    def daily_variability_plot(self, ymax: int = 12, trop_lat: float = None, relative: bool = True, save: bool = True,
                               legend: str = '_Hidden', figsize: int = None, linestyle: str = None, color: str = 'tab:blue',
                               model_variable: str = None, loc: str = 'upper right', fontsize: int = None,
                               add: Any = None, fig: Any = None, plot_title: str = None, path_to_pdf: str = None,
                               new_unit: str = None, name_of_file: str = '', pdf_format: bool = True,
                               path_to_netcdf: str = None) -> List[Union[plt.Figure, plt.Axes]]:
        """
        Plot the daily variability of the dataset.

        This function generates a plot showing the daily variability of the provided dataset.
        It allows customization of various plot parameters such as color, scale, and legends.

        Args:
            ymax (int): The maximum y-value for the plot.
            trop_lat (float): The tropical latitude value to be used.
            relative (bool): A flag indicating whether the plot should be relative.
            legend (str): The legend for the plot.
            figsize (int): The size of the figure.
            ls (str): The linestyle for the plot.
            maxticknum (int): The maximum number of ticks for the plot.
            color (str): The color of the plot.
            model_variable (str): The variable name to be used.
            loc (str): The location for the legend.
            add: Additional parameters for the plot.
            fig: The figure to be used for the plot.
            plot_title (str): The title for the plot.
            path_to_pdf (str): The path to the PDF file to be saved.
            new_unit (str): The new unit to which the data should be converted.
            name_of_file (str): The name of the file to be saved.
            pdf_format (bool): A flag indicating whether the file should be saved in PDF format.
            path_to_netcdf (str): The path to the NetCDF file to be used.

        Returns:
            list: A list containing the figure and axis objects.

        """

        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf

        if path_to_netcdf is None:
            raise Exception('The path needs to be provided')
        else:
            data = self.tools.open_dataset(path_to_netcdf=path_to_netcdf)
        if 'Dataset' in str(type(data)):
            y_lim_max = self.datamanager.precipitation_rate_units_converter(ymax, old_unit=data.units, new_unit=self.new_unit)
            data[self.model_variable] = self.datamanager.precipitation_rate_units_converter(data[self.model_variable],
                                                                                old_unit=data.units,
                                                                                new_unit=self.new_unit)
            data.attrs['units'] = self.new_unit

        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'tropical_rainfall_' + name_of_file + '_daily_variability.pdf'

        return self.plots.daily_variability_plot(data, ymax=y_lim_max, relative=relative, save=save,
                                                 legend=legend, figsize=figsize, linestyle=linestyle, color=color,
                                                 model_variable=self.model_variable, loc=loc, fontsize=fontsize,
                                                 add=add, fig=fig, plot_title=None, path_to_pdf=path_to_pdf,
                                                 pdf_format=pdf_format)

    def concat_two_datasets(self, dataset_1: xr.Dataset = None, dataset_2: xr.Dataset = None) -> xr.Dataset:
        """
        Function to concatenate two datasets along the time dimension.

        Args:
            dataset_1 (xarray.Dataset, optional): The first dataset. Defaults to None.
            dataset_2 (xarray.Dataset, optional): The second dataset. Defaults to None.

        Returns:
            xarray.Dataset: The xarray.Dataset resulting from concatenating dataset_1 and dataset_2 along the time dimension.
        """

        if not isinstance(dataset_1, xr.Dataset) or not isinstance(dataset_2, xr.Dataset):
            raise ValueError("Both dataset_1 and dataset_2 must be xarray.Dataset instances")

        # Ensure both datasets have a 'time' coordinate to concatenate along
        if 'time' not in dataset_1.coords or 'time' not in dataset_2.coords:
            raise ValueError("Both datasets must have a 'time' coordinate for concatenation")

        # Concatenate datasets along the time dimension
        concatenated_dataset = xr.concat([dataset_1, dataset_2], dim='time')
        concatenated_dataset.attrs['time_band_history'] = str(dataset_1.time_band)+'; '+str(dataset_2.time_band)
        concatenated_dataset.attrs['time_band'] = self.tools.merge_time_bands(dataset_1, dataset_2)
                        
        return concatenated_dataset


    def merge_list_of_daily_variability(self, path_to_output: str = None, start_year: int = None, end_year: int = None,
                             start_month: int = None, end_month: int = None,
                             test: bool = False, tqdm: bool = False, flag: str = None) -> xr.Dataset:
        """
        Function to merge a list of histograms based on specified criteria. It supports merging by seasonal 
        categories or specific year and month ranges.
        
        Args:
            path_to_output (str, optional): Path to the list of daily_variability data.
            start_year (int, optional): Start year of the range (inclusive).
            end_year (int, optional): End year of the range (inclusive).
            start_month (int, optional): Start month of the range (inclusive).
            end_month (int, optional): End month of the range (inclusive).
            test (bool, optional): Runs function in test mode.
            tqdm (bool, optional): Displays a progress bar during merging.
            flag (str, optional): A specific flag to look for in the filenames. Defaults to None.
        
        Returns:
            xr.Dataset: Merged xarray Dataset.
        """

        list_to_load = self.tools.select_files_by_year_and_month_range(path_to_histograms=path_to_output,
                                                                       start_year=start_year, end_year=end_year,
                                                                       start_month=start_month, end_month=end_month,
                                                                       flag=flag)
        
        self.tools.check_time_continuity(list_to_load)
        self.tools.check_incomplete_months(list_to_load)
        list_to_load = self.tools.check_and_remove_incomplete_months(list_to_load)
        
        self.logger.debug(f"List of files to merge:")
        for i in range(0, len(list_to_load)):
            self.logger.debug(f"{list_to_load[i]}")

        if len(list_to_load) > 0:
            progress_bar_template = "[{:<40}] {}%"
            try:
                # Initialize the merged dataset with the first histogram
                merged_dataset = self.tools.open_dataset(path_to_netcdf=list_to_load[0])
                
                # Loop through the rest of the histograms and merge them one by one
                for i in range(1, len(list_to_load)):
                    if tqdm:
                        ratio = i / len(list_to_load)
                        progress = int(40 * ratio)
                        print(progress_bar_template.format("=" * progress, int(ratio * 100)), end="\r")
                    
                    self.logger.debug(f"Merging histogram: {list_to_load[i]}")
                    next_dataset = self.tools.open_dataset(path_to_netcdf=list_to_load[i])
                    merged_dataset = self.concat_two_datasets(dataset_1=merged_dataset, dataset_2=next_dataset)
                return merged_dataset
            except Exception as e:
                self.logger.error(f"An unexpected error occurred while merging histograms: {e}") 
        else:
            self.logger.error("No histograms to load and merge.")