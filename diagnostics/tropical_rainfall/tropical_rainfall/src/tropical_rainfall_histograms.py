import numpy as np
import xarray as xr
from typing import Union, Tuple, Optional, Any, List
import fast_histogram
from aqua.logger import log_configure

from .tropical_rainfall_plots import PlottingClass
from .tropical_rainfall_tools import ToolsClass
from .tropical_rainfall_data_manager import TropicalPrecipitationDataManager

class HistogramClass: 
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
        
        self.width_of_bin = width_of_bin
        
    def dataset_into_1d(self, data: xr.Dataset, model_variable: Optional[str] = None, sort: bool = False) -> xr.Dataset:
        """
        Function to convert Dataset into a 1D array.

        Args:
            data (xarray.Dataset): The input Dataset.
            model_variable (str, optional): The variable of the Dataset. Defaults to 'mtpr'.
            sort (bool, optional): The flag to sort the array. Defaults to False.

        Returns:
            xarray.Dataset: The 1D array.
        """
        self.datamanager.class_attributes_update(model_variable=model_variable)
        coord_lat, coord_lon = self.datamanager.coordinate_names(data)

        try:
            data = data[self.model_variable]
        except KeyError:
            pass

        try:
            data_1d = data.stack(total=['time', coord_lat, coord_lon])
        except KeyError:
            data_1d = data.stack(total=[coord_lat, coord_lon])
        if sort:
            data_1d = data_1d.sortby(data_1d)
        return data_1d
    
    
    def histogram(self, data: xr.Dataset, data_with_global_atributes: Optional[xr.Dataset] = None,
                  weights: Optional[Any] = None, preprocess: bool = True, trop_lat: Optional[float] = None,
                  model_variable: Optional[str] = None, s_time: Optional[Union[str, int]] = None,
                  f_time: Optional[Union[str, int]] = None, s_year: Optional[int] = None, save: bool = True,
                  f_year: Optional[int] = None, s_month: Optional[int] = None, f_month: Optional[int] = None,
                  num_of_bins: Optional[int] = None, first_edge: Optional[float] = None,
                  width_of_bin: Optional[float] = None, bins: Union[int, List[float]] = 0,
                  path_to_histogram: Optional[str] = None, name_of_file: Optional[str] = None,
                  positive: bool = True, new_unit: Optional[str] = None, threshold: int = 2,
                  test: bool = False, seasons_bool: Optional[bool] = None,
                  rebuild: bool = False) -> Union[xr.Dataset, np.ndarray]:
        """
        Function to calculate a histogram of the high-resolution Dataset.

        Args:
            data (xarray.Dataset):          The input Dataset.
            preprocess (bool, optional):    If True, preprocesses the Dataset.              Defaults to True.
            trop_lat (float, optional):     The maximum absolute value of tropical latitude in the Dataset. Defaults to 10.
            model_variable (str, optional): The variable of interest in the Dataset.        Defaults to 'mtpr'.
            data_with_global_attributes (xarray.Dataset, optional): The Dataset with global attributes. Defaults to None.
            s_time (str/int, optional):     The starting time value/index in the Dataset.   Defaults to None.
            f_time (str/int, optional):     The final time value/index in the Dataset.      Defaults to None.
            s_year (int, optional):         The starting year in the Dataset.               Defaults to None.
            f_year (int, optional):         The final year in the Dataset.                  Defaults to None.
            s_month (int, optional):        The starting month in the Dataset.              Defaults to None.
            f_month (int, optional):        The final month in the Dataset.                 Defaults to None.
            num_of_bins (int, optional):    The number of bins for the histogram.           Defaults to None.
            first_edge (float, optional):   The starting edge value for the bins.           Defaults to None.
            width_of_bin (float, optional): The width of each bin.                          Defaults to None.
            bins (int, optional):           The number of bins for the histogram (alternative argument to 'num_of_bins').
                                            Defaults to 0.
            create_xarray (bool, optional): If True, creates an xarray dataset from the histogram counts. Defaults to True.
            path_to_histogram (str, optional):   The path to save the xarray dataset.       Defaults to None.

        Returns:
            xarray.Dataset or numpy.ndarray: The histogram of the Dataset.
        """
        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit,
                                     s_time=s_time, f_time=f_time, s_year=s_year, f_year=f_year, s_month=s_month,
                                     f_month=f_month, first_edge=first_edge, num_of_bins=num_of_bins,
                                     width_of_bin=width_of_bin)
        data_original = data

        if preprocess:
            data = self.datamanager.preprocessing(data, preprocess=preprocess,
                                      model_variable=self.model_variable, trop_lat=self.trop_lat,
                                      s_time=self.s_time, f_time=self.f_time, s_year=self.s_year, f_year=self.f_year,
                                      s_month=None, f_month=None, dask_array=False, new_unit=self.new_unit)
        #data = data.dropna(dim='time')
        size_of_the_data = self.tools.data_size(data)

        if self.new_unit is not None:
            data = self.datamanager.precipitation_rate_units_converter(
                data, model_variable=self.model_variable, new_unit=self.new_unit)
        data_with_final_grid = data

        if seasons_bool is not None:
            if seasons_bool:
                seasons_or_months = self.get_seasonal_or_monthly_data(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                                      model_variable=self.model_variable, trop_lat=trop_lat,
                                                                      new_unit=self.new_unit)
            else:
                seasons_or_months = self.get_seasonal_or_monthly_data(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                                      model_variable=self.model_variable, trop_lat=trop_lat,
                                                                      new_unit=self.new_unit)
        if isinstance(self.bins, int):
            bins = [self.first_edge + i *
                    self.width_of_bin for i in range(0, self.num_of_bins+1)]
            width_table = [
                self.width_of_bin for j in range(0, self.num_of_bins)]
            center_of_bin = [bins[i] + 0.5*width_table[i]
                             for i in range(0, len(bins)-1)]
        else:
            bins = self.bins
            width_table = [self.bins[i+1]-self.bins[i]
                           for i in range(0, len(self.bins)-1)]
            center_of_bin = [self.bins[i] + 0.5*width_table[i]
                             for i in range(0, len(self.bins)-1)]

        if positive:
            data = np.maximum(data, 0.)
            if seasons_bool is not None:
                for i in range(0, len(seasons_or_months)):
                    seasons_or_months[i] = np.maximum(seasons_or_months[i], 0.)
        if isinstance(self.bins, int):
            hist_fast = fast_histogram.histogram1d(data.values, range=[self.first_edge,
                                                                       self.first_edge + (self.num_of_bins)*self.width_of_bin],
                                                   bins=self.num_of_bins)
            hist_seasons_or_months = []
            if seasons_bool is not None:
                for i in range(0, len(seasons_or_months)):
                    hist_seasons_or_months.append(fast_histogram.histogram1d(seasons_or_months[i],
                                                                             range=[self.first_edge,
                                                                                    self.first_edge +
                                                                                    (self.num_of_bins)*self.width_of_bin],
                                                                             bins=self.num_of_bins))

        else:
            hist_np = np.histogram(data,  weights=weights, bins=self.bins)
            hist_fast = hist_np[0]
            hist_seasons_or_months = []
            if seasons_bool is not None:
                for i in range(0, len(seasons_or_months)):
                    hist_seasons_or_months.append(np.histogram(
                        seasons_or_months[i],  weights=weights, bins=self.bins)[0])
        self.logger.info('Histogram of the data is created')
        self.logger.debug('Size of data after preprocessing/Sum of Counts: {}/{}'
                          .format(self.tools.data_size(data), int(sum(hist_fast))))
        if int(sum(hist_fast)) != size_of_the_data:
            self.logger.warning(
                'Amount of counts in the histogram is not equal to the size of the data')
            self.logger.warning('Check the data and the bins')
        counts_per_bin = xr.DataArray(
            hist_fast, coords=[center_of_bin], dims=["center_of_bin"])
        counts_per_bin = counts_per_bin.assign_coords(
            width=("center_of_bin", width_table))
        counts_per_bin.attrs = data.attrs

        counts_per_bin.center_of_bin.attrs['units'] = data.units
        counts_per_bin.center_of_bin.attrs['history'] = 'Units are added to the bins to coordinate'
        counts_per_bin.attrs['size_of_the_data'] = size_of_the_data

        if data_with_global_atributes is None:
            data_with_global_atributes = data_original

        mtpr_dataset = counts_per_bin.to_dataset(name="counts")
        mtpr_dataset.attrs = data_with_global_atributes.attrs
        mtpr_dataset = self.add_frequency_and_pdf(
            mtpr_dataset=mtpr_dataset, test=test)

        if seasons_bool is not None:
            if seasons_bool:
                seasonal_or_monthly_labels = [
                    'DJF', 'MMA', 'JJA', 'SON', 'glob']
            else:
                seasonal_or_monthly_labels = [
                    'J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'J']
            for i in range(0, len(seasons_or_months)):
                mtpr_dataset['counts'+seasonal_or_monthly_labels[i]
                               ] = hist_seasons_or_months[i]
                mtpr_dataset = self.add_frequency_and_pdf(
                    mtpr_dataset=mtpr_dataset, test=test, label=seasonal_or_monthly_labels[i])

        mean_from_hist, mean_original, mean_modified = self.mean_from_histogram(hist=mtpr_dataset, data=data_with_final_grid,
                                                                                model_variable=self.model_variable,
                                                                                trop_lat=self.trop_lat, positive=positive)
        relative_discrepancy = (mean_original - mean_from_hist)*100/mean_original
        self.logger.debug('The difference between the mean of the data and the mean of the histogram: {}%'
                          .format(round(relative_discrepancy, 4)))
        if self.new_unit is None:
            unit = data.units
        else:
            unit = self.new_unit
        self.logger.debug('The mean of the data: {}{}'.format(mean_original, unit))
        self.logger.debug('The mean of the histogram: {}{}'.format(mean_from_hist, unit))
        if relative_discrepancy > threshold:
            self.logger.warning('The difference between the mean of the data and the mean of the histogram is greater \
                                than the threshold. \n Increase the number of bins and decrease the width of the bins.')
        for variable in (None, 'counts', 'frequency', 'pdf'):
            mtpr_dataset = self.datamanager.grid_attributes(
                data=data_with_final_grid, mtpr_dataset=mtpr_dataset, variable=variable)
            if variable is None:
                mtpr_dataset.attrs['units'] = mtpr_dataset.counts.units
                mtpr_dataset.attrs['mean_of_original_data'] = float(mean_original)
                mtpr_dataset.attrs['mean_of_histogram'] = float(mean_from_hist)
                mtpr_dataset.attrs['relative_discrepancy'] = float(relative_discrepancy)

            else:
                mtpr_dataset[variable].attrs['mean_of_original_data'] = float(mean_original)
                mtpr_dataset[variable].attrs['mean_of_histogram'] = float(mean_from_hist)
                mtpr_dataset[variable].attrs['relative_discrepancy'] = float(relative_discrepancy)
        if save:
            if path_to_histogram is None and self.path_to_netcdf is not None:
                path_to_histogram = self.path_to_netcdf+'histograms/'

            if path_to_histogram is not None and name_of_file is not None:
                bins_info = self.get_bins_info()
                self.datamanager.dataset_to_netcdf(
                    mtpr_dataset, path_to_netcdf=path_to_histogram, name_of_file=name_of_file+'_histogram_'+bins_info,
                    rebuild=rebuild)

        return mtpr_dataset
    
    def add_frequency_and_pdf(self, mtpr_dataset: Optional[xr.Dataset] = None, path_to_histogram: Optional[str] = None,
                              name_of_file: Optional[str] = None, test: Optional[bool] = False,
                              label: Optional[str] = None) -> xr.Dataset:
        """
        Function to convert the histogram to xarray.Dataset.

        Args:
            mtpr_dataset (xarray, optional):     The Dataset with the histogram. Defaults to None.
            path_to_histogram (str, optional):     The path to save the histogram. Defaults to None.
            name_of_file (str, optional):          The name of the file to save. Defaults to None.
            test (bool, optional):                 If True, performs a test. Defaults to False.
            label (str, optional):                 The label for the dataset. Defaults to None.

        Returns:
            xarray: The xarray.Dataset with the histogram.
        """
        if path_to_histogram is None and self.path_to_netcdf is not None:
            path_to_histogram = self.path_to_netcdf+'histograms/'

        hist_frequency = self.convert_counts_to_frequency(mtpr_dataset.counts,  test=test)
        mtpr_dataset['frequency'] = hist_frequency

        hist_pdf = self.convert_counts_to_pdf(mtpr_dataset.counts,  test=test)
        mtpr_dataset['pdf'] = hist_pdf

        hist_pdfP = self.convert_counts_to_pdfP(mtpr_dataset.counts,  test=test)
        mtpr_dataset['pdfP'] = hist_pdfP

        if label is not None:
            hist_frequency = self.convert_counts_to_frequency(mtpr_dataset['counts'+label],  test=test)
            mtpr_dataset['frequency'+label] = hist_frequency

            hist_pdf = self.convert_counts_to_pdf(mtpr_dataset['counts'+label],  test=test)
            mtpr_dataset['pdf'+label] = hist_pdf
        if path_to_histogram is not None and name_of_file is not None:
            bins_info = self.get_bins_info()
            self.datamanager.dataset_to_netcdf(
                dataset=mtpr_dataset, path_to_netcdf=path_to_histogram, name_of_file=name_of_file+'_histogram_'+bins_info)
        return mtpr_dataset

    def get_bins_info(self) -> str:
        """
        Constructs a string with information about the bins.

        Returns:
            str: A string representing the bins' first value, last value, and the count of bins - 1,
                 with periods replaced by dashes.
        """
        if isinstance(self.bins, int):
            # Dynamically generate bin edges if bins is an integer
            bins = [self.first_edge + i * self.width_of_bin for i in range(self.num_of_bins + 1)]
        else:
            bins = self.bins
        bins_info = f"{bins[0]}_{bins[-1]}_{len(bins)-1}".replace('.', '-')
        return bins_info

    def merge_two_datasets(self, dataset_1: xr.Dataset = None, dataset_2: xr.Dataset = None,
                           test: bool = False) -> xr.Dataset:
        """
        Function to merge two datasets.

        Args:
            dataset_1 (xarray.Dataset, optional): The first dataset. Defaults to None.
            dataset_2 (xarray.Dataset, optional): The second dataset. Defaults to None.
            test (bool, optional): Whether to run the function in test mode. Defaults to False.

        Returns:
            xarray.Dataset: The xarray.Dataset with the merged data.
        """

        if isinstance(dataset_1, xr.Dataset) and isinstance(dataset_2, xr.Dataset):
            dataset_3 = dataset_1.copy(deep=True)
            dataset_3.attrs = {**dataset_1.attrs, **dataset_2.attrs}

            for attribute in dataset_1.attrs:
                try:
                    if dataset_1.attrs[attribute] != dataset_2.attrs[attribute] and attribute not in 'time_band':
                        dataset_3.attrs[attribute] = str(dataset_1.attrs[attribute])+'; '+str(dataset_2.attrs[attribute])
                    elif attribute in 'time_band':
                        dataset_3.attrs['time_band_history'] = str(dataset_1.attrs[attribute])+'; '+str(dataset_2.attrs[attribute])
                        dataset_3.attrs['time_band'] = self.tools.merge_time_bands(dataset_1, dataset_2)
                except ValueError:
                    if dataset_1.attrs[attribute].all != dataset_2.attrs[attribute].all:
                        dataset_3.attrs[attribute] = str(dataset_1.attrs[attribute])+';\n '+str(dataset_2.attrs[attribute])

            dataset_3.counts.values = dataset_1.counts.values + dataset_2.counts.values
            dataset_3.counts.attrs['size_of_the_data'] = dataset_1.counts.size_of_the_data + dataset_2.counts.size_of_the_data
            dataset_3.frequency.values = self.convert_counts_to_frequency(dataset_3.counts,  test=test)
            dataset_3.pdf.values = self.convert_counts_to_pdf(dataset_3.counts,  test=test)

            for variable in ('counts', 'frequency', 'pdf'):
                for attribute in dataset_1.counts.attrs:
                    dataset_3[variable].attrs = {
                        **dataset_1[variable].attrs, **dataset_2[variable].attrs}
                    try:
                        if dataset_1[variable].attrs[attribute] != dataset_2[variable].attrs[attribute]:
                            dataset_3[variable].attrs[attribute] = str(
                                dataset_1[variable].attrs[attribute])+';\n ' + str(dataset_2[variable].attrs[attribute])
                    except ValueError:
                        if dataset_1[variable].attrs[attribute].all != dataset_2[variable].attrs[attribute].all:
                            dataset_3[variable].attrs[attribute] = str(
                                dataset_1[variable].attrs[attribute])+';\n ' + str(dataset_2[variable].attrs[attribute])
                dataset_3[variable].attrs['size_of_the_data'] = dataset_1[variable].size_of_the_data + \
                    dataset_2[variable].size_of_the_data
            if self.loglevel=='debug':
                self.tools.sanitize_attributes(dataset_3)
            return dataset_3

    def merge_list_of_histograms(self, path_to_histograms: str = None, start_year: int = None, end_year: int = None,
                             start_month: int = None, end_month: int = None, seasons_bool: bool = False,
                             test: bool = False, tqdm: bool = False, flag: str = None) -> xr.Dataset:
        """
        Function to merge a list of histograms based on specified criteria. It supports merging by seasonal 
        categories or specific year and month ranges.
        
        Args:
            path_to_histograms (str, optional): Path to the list of histograms.
            start_year (int, optional): Start year of the range (inclusive).
            end_year (int, optional): End year of the range (inclusive).
            start_month (int, optional): Start month of the range (inclusive).
            end_month (int, optional): End month of the range (inclusive).
            seasons_bool (bool, optional): True to merge based on seasonal categories.
            test (bool, optional): Runs function in test mode.
            tqdm (bool, optional): Displays a progress bar during merging.
            flag (str, optional): A specific flag to look for in the filenames. Defaults to None.
        
        Returns:
            xr.Dataset: Merged xarray Dataset.
        """

        if seasons_bool:
            seasons = {
                "DJF": ([12, 1, 2], []),
                "MAM": ([3, 4, 5], []),
                "JJA": ([6, 7, 8], []),
                "SON": ([9, 10, 11], [])
            }

            # Assuming you have a way to select files for each season
            for season, (months, _) in seasons.items():
                # Populate the files list for each season
                for month in months:
                    # This is a placeholder for how you might select files; adjust according to your actual file selection method
                    files_for_month = self.tools.select_files_by_year_and_month_range(
                        path_to_histograms=path_to_histograms,
                        start_year=start_year,
                        end_year=end_year,
                        start_month=month,
                        end_month=month,
                        flag=flag
                    )
                    seasons[season][1].extend(files_for_month)

            seasonal_datasets = []
            season_names = []  # Keep track of the season names for labeling during concatenation

            for season, (_, files) in seasons.items():
                seasonal_dataset = None
                for file in files:
                    if seasonal_dataset is None:
                        seasonal_dataset = self.tools.open_dataset(path_to_netcdf=file)
                    else:
                        seasonal_dataset = self.merge_two_datasets(
                            dataset_1=seasonal_dataset,
                            dataset_2=self.tools.open_dataset(path_to_netcdf=file)
                        )
                if seasonal_dataset:
                    seasonal_datasets.append(seasonal_dataset)
                    season_names.append(season)

            # Concatenate all seasonal datasets into a single dataset
            if seasonal_datasets:
                combined_dataset = xr.concat(seasonal_datasets, dim='season')
                combined_dataset = combined_dataset.assign_coords(season=('season', season_names))  # Correctly assign season names
                return combined_dataset
            else:
                self.logger.info("No data available for merging.")
                return None
        else:
            histograms_to_load = self.tools.select_files_by_year_and_month_range(path_to_histograms=path_to_histograms,
                                                                                 start_year=start_year, end_year=end_year,
                                                                                 start_month=start_month, end_month=end_month,
                                                                                 flag=flag)
            
            self.tools.check_time_continuity(histograms_to_load)
            self.tools.check_incomplete_months(histograms_to_load)
            histograms_to_load = self.tools.check_and_remove_incomplete_months(histograms_to_load)
            
            self.logger.debug(f"List of files to merge:")
            for i in range(0, len(histograms_to_load)):
                self.logger.debug(f"{histograms_to_load[i]}")

            if len(histograms_to_load) > 0:
                progress_bar_template = "[{:<40}] {}%"
                try:
                    # Initialize the merged dataset with the first histogram
                    merged_dataset = self.tools.open_dataset(path_to_netcdf=histograms_to_load[0])
                    
                    # Loop through the rest of the histograms and merge them one by one
                    for i in range(1, len(histograms_to_load)):
                        if tqdm:
                            ratio = i / len(histograms_to_load)
                            progress = int(40 * ratio)
                            print(progress_bar_template.format("=" * progress, int(ratio * 100)), end="\r")
                        
                        self.logger.debug(f"Merging histogram: {histograms_to_load[i]}")
                        next_dataset = self.tools.open_dataset(path_to_netcdf=histograms_to_load[i])
                        merged_dataset = self.merge_two_datasets(dataset_1=merged_dataset, dataset_2=next_dataset)
                    return merged_dataset
                except Exception as e:
                    self.logger.error(f"An unexpected error occurred while merging histograms: {e}") 
            else:
                self.logger.error("No histograms to load and merge.")

    def convert_counts_to_frequency(self, data: xr.Dataset, test: bool = False) -> xr.DataArray:
        """
        Function to convert the counts to the frequency.

        Args:
            data (xarray.Dataset): The counts.
            test (bool, optional): Whether to run the function in test mode. Defaults to False.

        Returns:
            xarray.DataArray: The frequency.
        """
        frequency = data[0:]/data.size_of_the_data
        frequency_per_bin = xr.DataArray(
            frequency, coords=[data.center_of_bin],    dims=["center_of_bin"])
        frequency_per_bin = frequency_per_bin.assign_coords(
            width=("center_of_bin", data.width.values))
        frequency_per_bin.attrs = data.attrs
        sum_of_frequency = sum(frequency_per_bin[:])

        if test:
            if sum(data[:]) == 0 or abs(sum_of_frequency - 1) < 10**(-4):
                pass
            else:
                self.logger.debug('Sum of Frequency: {}'
                                  .format(abs(sum_of_frequency.values)))
                raise AssertionError("Test failed.")
        return frequency_per_bin

    def convert_counts_to_pdf(self, data: xr.Dataset, test: bool = False) -> xr.DataArray:
        """
        Function to convert the counts to the pdf.

        Args:
            data (xarray.Dataset): The counts.
            test (bool, optional): Whether to run the function in test mode. Defaults to False.

        Returns:
            xarray.DataArray: The pdf.
        """
        pdf = data[0:]/(data.size_of_the_data*data.width[0:])
        pdf_per_bin = xr.DataArray(
            pdf, coords=[data.center_of_bin],    dims=["center_of_bin"])
        pdf_per_bin = pdf_per_bin.assign_coords(
            width=("center_of_bin", data.width.values))
        pdf_per_bin.attrs = data.attrs
        sum_of_pdf = sum(pdf_per_bin[:]*data.width[0:])

        if test:
            if sum(data[:]) == 0 or abs(sum_of_pdf-1.) < 10**(-4):
                pass
            else:
                self.logger.debug('Sum of PDF: {}'
                                  .format(abs(sum_of_pdf.values)))
                raise AssertionError("Test failed.")
        return pdf_per_bin

    def convert_counts_to_pdfP(self, data: xr.Dataset, test: bool = False) -> xr.DataArray:
        """
        Function to convert the counts to the pdf multiplied by the center of bin.

        Args:
            data (xarray.Dataset): The counts.
            test (bool, optional): Whether to run the function in test mode. Defaults to False.

        Returns:
            xarray.DataArray: The pdfP.
        """
        pdfP = data[0:]*data.center_of_bin[0:] / \
            (data.size_of_the_data*data.width[0:])
        pdfP_per_bin = xr.DataArray(
            pdfP, coords=[data.center_of_bin],    dims=["center_of_bin"])
        pdfP_per_bin = pdfP_per_bin.assign_coords(
            width=("center_of_bin", data.width.values))
        pdfP_per_bin.attrs = data.attrs
        sum_of_pdfP = sum(pdfP_per_bin[:]*data.width[0:])

        if test:
            if sum(data[:]) == 0 or abs(sum_of_pdfP-data.mean()) < 10**(-4):
                pass
            else:
                self.logger.debug('Sum of PDF: {}'
                                  .format(abs(sum_of_pdfP.values)))
                raise AssertionError("Test failed.")
        return pdfP_per_bin

    def mean_from_histogram(self, hist: xr.Dataset, data: xr.Dataset = None, old_unit: str = None, new_unit: str = None,
                            model_variable: str = None, trop_lat: float = None,
                            positive: bool = True) -> (float, float, float):
        """
        Function to calculate the mean from the histogram.

        Args:
            hist (xarray.Dataset): The histogram.
            data (xarray.Dataset): The data.
            old_unit (str): The old unit.
            new_unit (str): The new unit.
            model_variable (str): The model variable.
            trop_lat (float): The tropical latitude.
            positive (bool): The flag to indicate if the data should be positive.

        Returns:
            float: The mean from the histogram.
            float: The mean from the original data.
            float: The mean from the modified data.
        """
        self.datamanager.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        if data is not None:
            try:
                data = data[self.model_variable]
            except KeyError:
                pass
            # try:
            #    mean_of_original_data = data.sel(lat=slice(-self.trop_lat, self.trop_lat)).mean().values
            # except KeyError:
            #    mean_of_original_data = data.mean().values
            mean_of_original_data = data.mean().values
            if positive:
                _data = np.maximum(data, 0.)
                # try:
                #    mean_of_modified_data = _data.sel(lat=slice(-self.trop_lat, self.trop_lat)).mean().values
                # except KeyError:
                #    mean_of_modified_data = _data.mean().values
                mean_of_modified_data = _data.mean().values
            mean_of_original_data, mean_of_modified_data = float(mean_of_original_data), float(mean_of_modified_data)
        else:
            mean_of_original_data, mean_of_modified_data = None, None

        mean_from_freq = float((hist.frequency*hist.center_of_bin).sum().values)

        if self.new_unit is not None:
            try:
                mean_from_freq = self.datamanager.precipitation_rate_units_converter(mean_from_freq, old_unit=hist.counts.units,
                                                                         new_unit=self.new_unit)
            except AttributeError:
                mean_from_freq = self.datamanager.precipitation_rate_units_converter(mean_from_freq, old_unit=old_unit,
                                                                         new_unit=self.new_unit)
            if data is not None:
                mean_of_original_data = self.datamanager.precipitation_rate_units_converter(mean_of_original_data,
                                                                                old_unit=data.units, new_unit=self.new_unit)
                mean_of_modified_data = self.datamanager.precipitation_rate_units_converter(mean_of_modified_data,
                                                                                old_unit=data.units, new_unit=self.new_unit)

        return mean_from_freq, mean_of_original_data, mean_of_modified_data
    
    def histogram_plot(self, data: xr.Dataset, new_unit: str = None, pdfP: bool = False, positive: bool = True,
                       save: bool = True, weights: np.ndarray = None, frequency: bool = False, pdf: bool = True,
                       smooth: bool = False, step: bool = True, color_map: bool = False, linestyle: str = None,
                       ylogscale: bool = True, xlogscale: bool = False, color: str = 'tab:blue', figsize: float = None,
                       legend: str = '_Hidden', plot_title: str = None, loc: str = 'upper right', model_variable: str = None,
                       add: tuple = None, fig: object = None, path_to_pdf: str = None, name_of_file: str = '',
                       pdf_format: str = None, xmax: float = None, test: bool = False, linewidth: float = None,
                       fontsize: float = None,
                       factor=None) -> (object, object):
        """
        Function to generate a histogram figure based on the provided data.

        Args:
            data (xarray.Dataset): The data for the histogram.
            new_unit (str, optional): The new unit. Default is None.
            pdfP (bool, optional): Whether to plot the PDFP. Default is False.
            positive (bool, optional): The flag to indicate if the data should be positive. Default is True.
            save (bool, optional): Whether to save the plot. Default is True.
            weights (np.ndarray, optional): An array of weights for the data. Default is None.
            frequency (bool, optional): Whether to plot frequency. Default is False.
            pdf (bool, optional): Whether to plot the probability density function (PDF). Default is True.
            smooth (bool, optional): Whether to plot a smooth line. Default is True.
            step (bool, optional): Whether to plot a step line. Default is False.
            color_map (bool or str, optional): Whether to apply a color map to the histogram bars. Default is False.
            linestyle (str, optional): The line style for the plot. Default is None.
            ylogscale (bool, optional): Whether to use a logarithmic scale for the y-axis. Default is True.
            xlogscale (bool, optional): Whether to use a logarithmic scale for the x-axis. Default is False.
            color (str, optional): The color of the plot. Default is 'tab:blue'.
            figsize (float, optional): The size of the figure. Default is None.
            legend (str, optional): The legend label for the plot. Default is '_Hidden'.
            model_variable (str, optional): The name of the variable for the x-axis label. Default is None.
            add (tuple, optional): Tuple of (fig, ax) to add the plot to an existing figure. Default is None.
            fig (object, optional): The figure object to plot on. If provided, ignores the 'add' argument. Default is None.
            path_to_pdf (str, optional): The path to save the figure. If provided, saves the figure at the specified path.
                                         Default is None.
            name_of_file (str, optional): The name of the file. Default is ''.
            pdf_format (str, optional): The format for the PDF. Default is None.
            xmax (float, optional): The maximum value for the x-axis. Default is None.
            test (bool, optional): Whether to run the test. Default is False.
            linewidth (float, optional): The width of the line. Default is None.
            fontsize (float, optional): The font size for the plot. Default is None.
            factor (float or None): The factor by which to adjust bin widths. Values > 1 increase bin width, 
                                    values < 1 decrease it. None leaves the bin width unchanged.


        Returns:
            A tuple (fig, ax) containing the figure and axes objects.
        """
        self.datamanager.class_attributes_update(model_variable=model_variable, new_unit=new_unit)

        if path_to_pdf is None and self.path_to_pdf is not None:
            path_to_pdf = self.path_to_pdf
        if 'Dataset' in str(type(data)):
            data = self.tools.adjust_bins(data, factor=factor)
            data = data['counts']
        if not pdf and not frequency and not pdfP:
            pass
            self.logger.debug("Generating a histogram to visualize the counts...")
        elif pdf and not frequency and not pdfP:
            data = self.convert_counts_to_pdf(data,  test=test)
            self.logger.debug("Generating a histogram to visualize the PDF...")
        elif not pdf and frequency and not pdfP:
            data = self.convert_counts_to_frequency(data,  test=test)
            self.logger.debug("Generating a histogram to visualize the frequency...")
        elif pdfP:
            data = self.convert_counts_to_pdfP(data,  test=test)
            self.logger.debug("Generating a histogram to visualize the PDFP...")

        x = self.datamanager.precipitation_rate_units_converter(data.center_of_bin, new_unit=self.new_unit).values
        if self.new_unit is None:
            xlabel = self.model_variable+", ["+str(data.attrs['units'])+"]"
        else:
            xlabel = self.model_variable+", ["+str(self.new_unit)+"]"

        if pdf and not frequency and not pdfP:
            ylabel = 'PDF'
            _name = '_PDF_histogram'
        elif not pdf and frequency and not pdfP:
            ylabel = 'Frequency'
            _name = '_frequency_histogram'
        elif not frequency and not pdfP and not pdf:
            ylabel = 'Counts'
            _name = '_counts_histogram'
        elif pdfP:
            ylabel = 'PDF * P'
            _name = '_PDFP_histogram'

        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + _name + '.pdf'

        return self.plots.histogram_plot(x=x, data=data, positive=positive, xlabel=xlabel, ylabel=ylabel,
                                         weights=weights, smooth=smooth, step=step, color_map=color_map,
                                         linestyle=linestyle, ylogscale=ylogscale, xlogscale=xlogscale,
                                         color=color, save=save, figsize=figsize, legend=legend, plot_title=plot_title,
                                         loc=loc, add=add, fig=fig, path_to_pdf=path_to_pdf, pdf_format=pdf_format,
                                         xmax=xmax, linewidth=linewidth, fontsize=fontsize)