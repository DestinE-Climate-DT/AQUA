#!/usr/bin/env python3
"""The module contains Tropical Precipitation Diagnostic:

.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""

import re
import os
from os import listdir
from os.path import isfile, join
from datetime import datetime
import numpy as np
import xarray as xr
from typing import Union, Tuple, Optional, Any, List

import matplotlib.pyplot as plt
import matplotlib.figure as figure

import dask.array as da
import fast_histogram

from aqua.util import create_folder
from aqua.logger import log_configure

from .tropical_rainfall_tools import ToolsClass
from .tropical_rainfall_plots import PlottingClass


class MainClass:
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

        self.width_of_bin = width_of_bin

    def class_attributes_update(self, trop_lat: Union[float, None] = None, s_time: Union[str, int, None] = None,
                                f_time: Union[str, int, None] = None, s_year: Union[int, None] = None,
                                f_year: Union[int, None] = None, s_month: Union[int, None] = None,
                                f_month: Union[int, None] = None, num_of_bins: Union[int, None] = None,
                                first_edge: Union[float, None] = None, width_of_bin: Union[float, None] = None,
                                bins: Union[list, int] = 0, model_variable: Union[str, None] = None,
                                new_unit: Union[str, None] = None):
        """ Update the class attributes with new values.

        Args:
            trop_lat (Union[float, None], optional): The latitude of the tropical zone. Defaults to None.
            s_time (Union[str, int, None], optional): The start time of the time interval. Defaults to None.
            f_time (Union[str, int, None], optional): The end time of the time interval. Defaults to None.
            s_year (Union[int, None], optional): The start year of the time interval. Defaults to None.
            f_year (Union[int, None], optional): The end year of the time interval. Defaults to None.
            s_month (Union[int, None], optional): The start month of the time interval. Defaults to None.
            f_month (Union[int, None], optional): The end month of the time interval. Defaults to None.
            num_of_bins (Union[int, None], optional): The number of bins. Defaults to None.
            first_edge (Union[float, None], optional): The first edge of the bin. Defaults to None.
            width_of_bin (Union[float, None], optional): The width of the bin. Defaults to None.
            bins (Union[list, int], optional): The bins. Defaults to 0.
            model_variable (Union[str, None], optional): The name of the model variable. Defaults to None.
            new_unit (Union[str, None], optional): The unit for the new data. Defaults to None.
        """
        if trop_lat is not None and isinstance(trop_lat, (int, float)):
            self.trop_lat = trop_lat
        elif trop_lat is not None and not isinstance(trop_lat, (int, float)):
            raise TypeError("trop_lat must to be integer or float")

        if s_time is not None and isinstance(s_time, (int, str)):
            self.s_time = s_time
        elif s_time is not None and not isinstance(s_time, (int, str)):
            raise TypeError("s_time must to be integer or string")

        if f_time is not None and isinstance(f_time, (int, str)):
            self.f_time = f_time
        elif f_time is not None and not isinstance(f_time, (int, str)):
            raise TypeError("f_time must to be integer or string")

        if s_year is not None and isinstance(s_year, int):
            self.s_year = s_year
        elif s_year is not None and not isinstance(s_year, int):
            raise TypeError("s_year must to be integer")

        if f_year is not None and isinstance(f_year, int):
            self.f_year = f_year
        elif f_year is not None and not isinstance(f_year, int):
            raise TypeError("f_year must to be integer")

        if s_month is not None and isinstance(s_month, int):
            self.s_month = s_month
        elif s_month is not None and not isinstance(s_month, int):
            raise TypeError("s_month must to be integer")

        if f_month is not None and isinstance(f_month, int):
            self.f_month = f_month
        elif f_month is not None and not isinstance(f_month, int):
            raise TypeError("f_month must to be integer")

        if bins != 0 and isinstance(bins, np.ndarray):
            self.bins = bins
        elif bins != 0 and not isinstance(bins, (np.ndarray, list)):
            raise TypeError("bins must to be array")

        if num_of_bins is not None and isinstance(num_of_bins, int):
            self.num_of_bins = num_of_bins
        elif num_of_bins is not None and not isinstance(num_of_bins, int):
            raise TypeError("num_of_bins must to be integer")

        if first_edge is not None and isinstance(first_edge, (int, float)):
            self.first_edge = first_edge
        elif first_edge is not None and not isinstance(first_edge, (int, float)):
            raise TypeError("first_edge must to be integer or float")

        if width_of_bin is not None and isinstance(width_of_bin, (int, float)):
            self.width_of_bin = width_of_bin
        elif width_of_bin is not None and not isinstance(width_of_bin, (int, float)):
            raise TypeError("width_of_bin must to be integer or float")

        self.new_unit = self.new_unit if new_unit is None else new_unit
        self.model_variable = self.model_variable if model_variable is None else model_variable

    def coordinate_names(self, data: Union[xr.Dataset, xr.DataArray]) -> Tuple[Optional[str], Optional[str]]:
        """
        Function to get the names of the coordinates.

        Args:
            data (xarray.Dataset or xarray.DataArray): The data to extract coordinate names from.

        Returns:
            Tuple[Optional[str], Optional[str]]: A tuple containing the names of latitude and longitude coordinates, if found.
        """

        coord_lat, coord_lon = None, None

        if 'Dataset' in str(type(data)):
            for i in data._coord_names:
                if 'lat' in i:
                    coord_lat = i
                if 'lon' in i:
                    coord_lon = i
        elif 'DataArray' in str(type(data)):
            for i in data.coords:
                if 'lat' in i:
                    coord_lat = i
                if 'lon' in i:
                    coord_lon = i
        return coord_lat, coord_lon
    
    def precipitation_rate_units_converter(self, data: Union[xr.Dataset, float, int, np.ndarray],
                                           model_variable: Optional[str] = 'mtpr', old_unit: Optional[str] = None,
                                           new_unit: Optional[str] = 'm s**-1') -> xr.Dataset:
        """
        Function to convert the units of precipitation rate.

        Args:
            data (Union[xarray.Dataset, float, int, np.ndarray]): The Dataset or data array.
            model_variable (str, optional): The name of the variable to be converted. Defaults to 'mtpr'.
            old_unit (str, optional): The old unit of the variable. Defaults to None.
            new_unit (str, optional): The new unit of the variable. Defaults to 'm s**-1'.

        Returns:
            xarray.Dataset: The Dataset with converted units.
        """
        self.class_attributes_update(model_variable=model_variable, new_unit=new_unit)
        try:
            data = data[self.model_variable]
        except (TypeError, KeyError):
            pass

        if 'xarray' in str(type(data)):
            if 'units' in data.attrs and data.units == self.new_unit:
                return data
            if old_unit is None:
                old_unit = data.units
            data.attrs['units'] = self.new_unit
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history_update = str(current_time)+' the units of precipitation are converted from ' + \
                str(data.units) + ' to ' + str(self.new_unit) + ';\n '
            if 'history' not in data.attrs:
                data.attrs['history'] = ' '
            history_attr = data.attrs['history'] + history_update
            data.attrs['history'] = history_attr
        data = self.tools.convert_units(value=data, from_unit=old_unit, to_unit=self.new_unit) 
        return data

    def latitude_band(self, data: xr.Dataset, trop_lat: Optional[Union[int, float]] = None) -> xr.Dataset:
        """
        Function to select the Dataset for the specified latitude range.

        Args:
            data (xarray.Dataset): The Dataset to be filtered.
            trop_lat (Union[int, float], optional): The maximal and minimal tropical latitude values in the Dataset.
                                                    Defaults to None.

        Returns:
            xarray.Dataset: The Dataset only for the selected latitude range.
        """

        self.class_attributes_update(trop_lat=trop_lat)

        coord_lat, _ = self.coordinate_names(data)
        self.class_attributes_update(trop_lat=trop_lat)
        return data.where(abs(data[coord_lat]) <= self.trop_lat, drop=True)

    def time_band(self, data: xr.Dataset, s_time: Optional[str] = None, f_time: Optional[str] = None,
                  s_year: Optional[str] = None, f_year: Optional[str] = None,
                  s_month: Optional[str] = None, f_month: Optional[str] = None) -> xr.Dataset:
        """
        Function to select the Dataset for the specified time range.

        Args:
            data (xarray.Dataset): The Dataset to be filtered.
            s_time (str, optional): The starting time of the Dataset. Defaults to None.
            f_time (str, optional): The ending time of the Dataset. Defaults to None.
            s_year (str, optional): The starting year of the Dataset. Defaults to None.
            f_year (str, optional): The ending year of the Dataset. Defaults to None.
            s_month (str, optional): The starting month of the Dataset. Defaults to None.
            f_month (str, optional): The ending month of the Dataset. Defaults to None.

        Returns:
            xarray.Dataset: The Dataset only for the selected time range.
        """
        self.class_attributes_update(s_time=s_time, f_time=f_time, s_year=s_year, f_year=f_year,
                                     s_month=s_month, f_month=f_month)

        if isinstance(self.s_time, int) and isinstance(self.f_time, int):
            if self.s_time is not None and self.f_time is not None:
                data = data.isel(time=slice(self.s_time, self.f_time))

        elif self.s_year is not None and self.f_year is None:
            data = data.where(data['time.year'] == self.s_year, drop=True)

        elif self.s_year is not None and self.f_year is not None:
            data = data.where(data['time.year'] >= self.s_year, drop=True)
            data = data.where(data['time.year'] <= self.f_year, drop=True)

        if self.s_month is not None and self.f_month is not None:
            data = data.where(data['time.month'] >= self.s_month, drop=True)
            data = data.where(data['time.month'] <= self.f_month, drop=True)

        if isinstance(self.s_time, str) and isinstance(self.f_time, str):
            if self.s_time is not None and self.f_time is not None:
                self.s_time = self.tools.split_time(self.s_time)
                self.f_time = self.tools.split_time(self.f_time)
            self.logger.debug("The starting and final times are {} and {}".format(self.s_time, self.f_time))
            data = data.sel(time=slice(self.s_time, self.f_time))

        elif self.s_time is not None and self.f_time is None:
            if isinstance(self.s_time, str):
                self.s_time = self.tools.split_time(self.s_time)
                self.logger.debug("The selected time is {}".format(self.s_time))
                data = data.sel(time=slice(self.s_time))

        return data

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
        self.class_attributes_update(model_variable=model_variable)
        coord_lat, coord_lon = self.coordinate_names(data)

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

    def preprocessing(self, data: xr.Dataset, trop_lat: Optional[float] = None, preprocess: bool = True,
                      model_variable: Optional[str] = None, s_time: Union[str, int, None] = None,
                      f_time: Union[str, int, None] = None, s_year: Union[int, None] = None, f_year: Union[int, None] = None,
                      new_unit: Union[str, None] = None, s_month: Union[int, None] = None, f_month: Union[int, None] = None,
                      dask_array: bool = False) -> xr.Dataset:
        """
        Function to preprocess the Dataset according to provided arguments and attributes of the class.

        Args:
            data (xarray.Dataset): The input Dataset.
            trop_lat (float, optional): The maximum and minimum tropical latitude values in the Dataset. Defaults to None.
            preprocess (bool, optional): If True, the function preprocesses the Dataset. Defaults to True.
            model_variable (str, optional): The variable of the Dataset. Defaults to 'mtpr'.
            s_time (Union[str, int, None], optional): The starting time value/index in the Dataset. Defaults to None.
            f_time (Union[str, int, None], optional): The final time value/index in the Dataset. Defaults to None.
            s_year (Union[int, None], optional): The starting year in the Dataset. Defaults to None.
            f_year (Union[int, None], optional): The final year in the Dataset. Defaults to None.
            s_month (Union[int, None], optional): The starting month in the Dataset. Defaults to None.
            f_month (Union[int, None], optional): The final month in the Dataset. Defaults to None.
            dask_array (bool, optional): If True, the function returns a dask array. Defaults to False.

        Returns:
            xarray.Dataset: Preprocessed Dataset according to the arguments of the function.
        """

        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit,
                                     s_time=s_time, f_time=f_time, s_month=s_month, s_year=s_year, f_year=f_year,
                                     f_month=f_month)
        if preprocess:
            if 'time' in data.coords:
                data_per_time_band = self.time_band(data, s_time=self.s_time, f_time=self.f_time, s_year=self.s_year,
                                                    f_year=self.f_year, s_month=self.s_month, f_month=self.f_month)
            else:
                data_per_time_band = data

            try:
                data_variable = data_per_time_band[self.model_variable]
            except KeyError:
                data_variable = data_per_time_band

            data_per_lat_band = self.latitude_band(
                data_variable, trop_lat=self.trop_lat)

            if self.new_unit is not None:
                data_per_lat_band = self.precipitation_rate_units_converter(data_per_lat_band, new_unit=self.new_unit)

            if dask_array:
                data_1d = self.dataset_into_1d(data_per_lat_band)
                dask_data = da.from_array(data_1d)
                return dask_data
            else:
                return data_per_lat_band
        else:
            return data

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
        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit,
                                     s_time=s_time, f_time=f_time, s_year=s_year, f_year=f_year, s_month=s_month,
                                     f_month=f_month, first_edge=first_edge, num_of_bins=num_of_bins,
                                     width_of_bin=width_of_bin)
        data_original = data

        if preprocess:
            data = self.preprocessing(data, preprocess=preprocess,
                                      model_variable=self.model_variable, trop_lat=self.trop_lat,
                                      s_time=self.s_time, f_time=self.f_time, s_year=self.s_year, f_year=self.f_year,
                                      s_month=None, f_month=None, dask_array=False, new_unit=self.new_unit)
        #data = data.dropna(dim='time')
        size_of_the_data = self.tools.data_size(data)

        if self.new_unit is not None:
            data = self.precipitation_rate_units_converter(
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
            mtpr_dataset = self.grid_attributes(
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
                self.dataset_to_netcdf(
                    mtpr_dataset, path_to_netcdf=path_to_histogram, name_of_file=name_of_file+'_histogram_'+bins_info,
                    rebuild=rebuild)

        return mtpr_dataset

    def dataset_to_netcdf(self, dataset: Optional[xr.Dataset] = None, path_to_netcdf: Optional[str] = None, rebuild: bool = False,
                          name_of_file: Optional[str] = None) -> str:
        """
        Function to save the histogram.

        Args:
            dataset (xarray, optional):         The Dataset with the histogram.     Defaults to None.
            path_to_netcdf (str, optional):  The path to save the histogram.     Defaults to None.

        Returns:
            str: The path to save the histogram.
        """
        if path_to_netcdf is None:
            path_to_netcdf = self.path_to_netcdf

        if isinstance(path_to_netcdf, str):
            create_folder(folder=str(path_to_netcdf), loglevel='WARNING')
            if name_of_file is None:
                name_of_file = '_'
            time_band = dataset.attrs['time_band']
            self.logger.debug('Time band is {}'.format(time_band))
            try:
                name_of_file = name_of_file + '_' + re.split(":", re.split(", ", time_band)[0])[0] + '_' + \
                    re.split(":", re.split(", ", time_band)[1])[0] + '_' + re.split("=", re.split(", ", time_band)[2])[1]
            except IndexError:
                try:
                    name_of_file = name_of_file + '_' + re.split(":", re.split(", ", time_band)[0])[0] + '_' + \
                        re.split(":", re.split(", ", time_band)[1])[0]
                except IndexError:
                    name_of_file = name_of_file + '_' + re.split(":", time_band)[0]
            path_to_netcdf = path_to_netcdf + 'trop_rainfall_' + name_of_file + '.nc'

            if os.path.exists(path_to_netcdf):
                self.logger.info(f"File {path_to_netcdf} already exists. Set `rebuild=True` if you want to update it.")
                if rebuild:
                    try:
                        # Attempt to remove the file (make sure you have permissions)
                        self.logger.warning(f"Removing existing file: {path_to_netcdf}.")
                        os.remove(path_to_netcdf)
                    except PermissionError:
                        self.logger.error(f"Permission denied when attempting to remove {path_to_netcdf}. Check file permissions.")
                        return  # Exiting the function or handling the error accordingly

                    # Proceed to save the new NetCDF file after successfully removing the old one
                    dataset.to_netcdf(path=path_to_netcdf, mode='w')
                    self.logger.info(f"Updated NetCDF file saved at {path_to_netcdf}")
                # No need for the else block here to repeat the log message about setting rebuild=True
            else:
                # If the file doesn't exist, simply save the new one
                dataset.to_netcdf(path=path_to_netcdf, mode='w')
                self.logger.info(f"NetCDF file saved at {path_to_netcdf}")
        else:
            self.logger.debug("The path to save the histogram needs to be provided.")
        return path_to_netcdf

    def grid_attributes(self, data: Optional[xr.Dataset] = None, mtpr_dataset: Optional[xr.Dataset] = None,
                        variable: Optional[str] = None) -> xr.Dataset:
        """
        Function to add the attributes with information about the space and time grid to the Dataset.

        Args:
            data (xarray, optional):            The Dataset with a final time and space grif, for which calculations
                                                were performed. Defaults to None.
            mtpr_dataset (xarray, optional):  Created Dataset by the diagnostics, which we would like to populate
                                                with attributes. Defaults to None.
            variable (str, optional):           The name of the Variable objects (not a physical variable) of the created
                                                Dataset. Defaults to None.

        Returns:
            xarray.Dataset: The updated dataset with grid attributes. The grid attributes include time_band,
                            lat_band, and lon_band.

        Raises:
            KeyError: If the obtained xarray.Dataset doesn't have global attributes.
        """
        coord_lat, coord_lon = self.coordinate_names(data)
        try:
            if data.time.size > 1:
                time_band = str(
                    data.time[0].values)+', '+str(data.time[-1].values)+', freq='+str(self.tools.time_interpreter(data))
            else:
                try:
                    time_band = str(data.time.values[0])
                except IndexError:
                    time_band = str(data.time.values)
        except KeyError:
            time_band = 'None'
        try:
            if data[coord_lat].size > 1:
                latitude_step = data[coord_lat][1].values - data[coord_lat][0].values
                lat_band = str(data[coord_lat][0].values)+', ' + str(data[coord_lat][-1].values) + ', freq='+str(latitude_step)
            else:
                lat_band = data[coord_lat].values
                latitude_step = 'None'
        except KeyError:
            lat_band = 'None'
            latitude_step = 'None'
        try:
            if data[coord_lon].size > 1:
                longitude_step = data[coord_lon][1].values - data[coord_lon][0].values
                lon_band = str(data[coord_lon][0].values)+', ' + str(data[coord_lon][-1].values) + \
                    ', freq=' + str(longitude_step)
            else:
                longitude_step = 'None'
                lon_band = data[coord_lon].values
        except KeyError:
            lon_band = 'None'
            longitude_step = 'None'

        if variable is None:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history_update = str(current_time)+' histogram is calculated for time_band: ['+str(
                time_band)+']; lat_band: ['+str(lat_band)+']; lon_band: ['+str(lon_band)+'];\n '
            try:
                history_attr = mtpr_dataset.attrs['history'] + history_update
                mtpr_dataset.attrs['history'] = history_attr
            except KeyError:
                self.logger.debug(
                    "The obtained xarray.Dataset doesn't have global attributes. Consider adding global attributes \
                    manually to the dataset.")
                pass
            mtpr_dataset.attrs['time_band'] = time_band
            mtpr_dataset.attrs['lat_band'] = lat_band
            mtpr_dataset.attrs['lon_band'] = lon_band
            mtpr_dataset.attrs['time_band_history'] = time_band
        else:
            mtpr_dataset[variable].attrs['time_band'] = time_band
            mtpr_dataset[variable].attrs['lat_band'] = lat_band
            mtpr_dataset[variable].attrs['lon_band'] = lon_band
            mtpr_dataset[variable].attrs['time_band_history'] = time_band

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
            self.dataset_to_netcdf(
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
        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

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
                mean_from_freq = self.precipitation_rate_units_converter(mean_from_freq, old_unit=hist.counts.units,
                                                                         new_unit=self.new_unit)
            except AttributeError:
                mean_from_freq = self.precipitation_rate_units_converter(mean_from_freq, old_unit=old_unit,
                                                                         new_unit=self.new_unit)
            if data is not None:
                mean_of_original_data = self.precipitation_rate_units_converter(mean_of_original_data,
                                                                                old_unit=data.units, new_unit=self.new_unit)
                mean_of_modified_data = self.precipitation_rate_units_converter(mean_of_modified_data,
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
        self.class_attributes_update(model_variable=model_variable, new_unit=new_unit)

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

        x = self.precipitation_rate_units_converter(data.center_of_bin, new_unit=self.new_unit).values
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

    def mean_along_coordinate(self, data: xr.Dataset, model_variable: str = None, preprocess: bool = True,
                              trop_lat: float = None, coord: str = 'time', glob: bool = False, s_time: str = None,
                              f_time: str = None, positive: bool = True, s_year: str = None, f_year: str = None,
                              new_unit: str = None, s_month: str = None, f_month: str = None) -> xr.Dataset:
        """
        Function to calculate the mean value of variable in Dataset.

        Args:
            data (xarray.Dataset): The Dataset.
            model_variable (str, optional): The variable of the Dataset. Defaults to 'mtpr'.
            trop_lat (float, optional): The maximum and minimal tropical latitude values in Dataset. Defaults to None.
            coord (str, optional): The coordinate of the Dataset. Defaults to 'time'.
            s_time (str, optional): The starting time of the Dataset. Defaults to None.
            f_time (str, optional): The ending time of the Dataset. Defaults to None.
            s_year (str, optional): The starting year of the Dataset. Defaults to None.
            f_year (str, optional): The ending year of the Dataset. Defaults to None.
            s_month (str, optional): The starting month of the Dataset. Defaults to None.
            f_month (str, optional): The ending month of the Dataset. Defaults to None.
            glob (bool, optional): If True, the median value is calculated for all lat and lon. Defaults to False.
            preprocess (bool, optional): If True, the Dataset is preprocessed. Defaults to True.
            positive (bool, optional): The flag to indicate if the data should be positive. Defaults to True.
            new_unit (str, optional): The new unit. Defaults to None.

        Returns:
            xarray.Dataset: The mean value of the variable.
        """
        self.class_attributes_update(model_variable=model_variable, new_unit=new_unit)

        if preprocess:
            data = self.preprocessing(data, preprocess=preprocess,
                                      model_variable=self.model_variable, trop_lat=self.trop_lat,
                                      s_time=self.s_time, f_time=self.f_time, s_year=self.s_year, f_year=self.f_year,
                                      s_month=None, f_month=None, dask_array=False, new_unit=self.new_unit)
        if positive:
            data = np.maximum(data, 0.)
        coord_lat, coord_lon = self.coordinate_names(data)
        if coord in data.dims:

            self.class_attributes_update(trop_lat=trop_lat, s_time=s_time, f_time=f_time,
                                         s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month)
            if glob:
                return data.mean()
            else:
                if coord == 'time':
                    return data.mean(coord_lat).mean(coord_lon)
                elif coord == coord_lat:
                    return data.mean('time').mean(coord_lon)
                elif coord == coord_lon:
                    return data.mean('time').mean(coord_lat)
        else:
            for i in data.dims:
                coord = i
            return data.median(coord)

    def median_along_coordinate(self, data: xr.Dataset, trop_lat: float = None, preprocess: bool = True,
                                model_variable: str = None, coord: str = 'time', glob: bool = False, s_time: str = None,
                                f_time: str = None, positive: bool = True, s_year: str = None, f_year: str = None,
                                new_unit: str = None, s_month: str = None, f_month: str = None) -> xr.Dataset:
        """
        Function to calculate the median value of a variable in a Dataset.

        Args:
            data (xarray.Dataset): The Dataset.
            model_variable (str, optional): The variable of the Dataset. Defaults to 'mtpr'.
            trop_lat (float, optional): The maximum and minimal tropical latitude values in the Dataset. Defaults to None.
            coord (str, optional): The coordinate of the Dataset. Defaults to 'time'.
            s_time (str, optional): The starting time of the Dataset. Defaults to None.
            f_time (str, optional): The ending time of the Dataset. Defaults to None.
            s_year (str, optional): The starting year of the Dataset. Defaults to None.
            f_year (str, optional): The ending year of the Dataset. Defaults to None.
            s_month (str, optional): The starting month of the Dataset. Defaults to None.
            f_month (str, optional): The ending month of the Dataset. Defaults to None.
            glob (bool, optional): If True, the median value is calculated for all latitudes and longitudes. Defaults to False.
            preprocess (bool, optional): If True, the Dataset is preprocessed. Defaults to True.
            positive (bool, optional): The flag to indicate if the data should be positive. Defaults to True.
            new_unit (str, optional): The new unit. Defaults to None.

        Returns:
            xarray.Dataset: The median value of the variable.
        """
        self.class_attributes_update(model_variable=model_variable, new_unit=new_unit)
        if preprocess:
            data = self.preprocessing(data, preprocess=preprocess,
                                      model_variable=self.model_variable, trop_lat=self.trop_lat,
                                      s_time=self.s_time, f_time=self.f_time, s_year=self.s_year, f_year=self.f_year,
                                      s_month=None, f_month=None, dask_array=False, new_unit=self.new_unit)

        if positive:
            data = np.maximum(data, 0.)
        coord_lat, coord_lon = self.coordinate_names(data)
        if coord in data.dims:
            self.class_attributes_update(trop_lat=trop_lat, s_time=s_time, f_time=f_time,
                                         s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month)

            if glob:
                return data.median(coord_lat).median(coord_lon).mean('time')
            else:
                if coord == 'time':
                    return data.median(coord_lat).median(coord_lon)
                elif coord == coord_lat:
                    return data.median('time').median(coord_lon)
                elif coord == coord_lon:
                    return data.median('time').median(coord_lat)

        else:
            for i in data.dims:
                coord = i
            return data.median(coord)

    def average_into_netcdf(self, data: xr.Dataset, glob: bool = False, preprocess: bool = True,
                            model_variable: str = None, coord: str = 'lat', trop_lat: float = None,
                            get_mean: bool = True, get_median: bool = False, s_time: str = None,
                            f_time: str = None, s_year: str = None, f_year: str = None, s_month: str = None,
                            f_month: str = None, new_unit: str = None, name_of_file: str = None,
                            seasons_bool: bool = True, path_to_netcdf: str = None) -> xr.Dataset:
        """
        Function to plot the mean or median value of the variable in a Dataset.

        Args:
            data (xarray.Dataset): The Dataset.
            glob (bool, optional): If True, the value is calculated for all latitudes and longitudes. Defaults to False.
            preprocess (bool, optional): If True, the Dataset is preprocessed. Defaults to True.
            model_variable (str, optional): The variable of the Dataset. Defaults to 'mtpr'.
            coord (str, optional): The coordinate of the Dataset. Defaults to 'time'.
            trop_lat (float, optional): The maximumal and minimal tropical latitude values in the Dataset. Defaults to None.
            get_mean (bool, optional): The flag to calculate the mean of the variable. Defaults to True.
            get_median (bool, optional): The flag to calculate the median of the variable. Defaults to False.
            s_time (str, optional): The starting time of the Dataset. Defaults to None.
            f_time (str, optional): The ending time of the Dataset. Defaults to None.
            s_year (str, optional): The starting year of the Dataset. Defaults to None.
            f_year (str, optional): The ending year of the Dataset. Defaults to None.
            s_month (str, optional): The starting month of the Dataset. Defaults to None.
            f_month (str, optional): The ending month of the Dataset. Defaults to None.
            new_unit (str, optional): The unit of the model variable. Defaults to None.
            name_of_file (str, optional): The name of the file. Defaults to None.
            seasons_bool (bool, optional): The flag to calculate the seasonal mean. Defaults to True.
            path_to_netcdf (str, optional): The path to the NetCDF file. Defaults to None.

        Returns:
            xarray.Dataset: The calculated mean or median value of the variable.
        """
        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit,
                                     s_time=s_time, f_time=f_time, s_year=s_year, f_year=f_year,
                                     s_month=s_month, f_month=f_month)

        if path_to_netcdf is None and self.path_to_netcdf is not None:
            path_to_netcdf = self.path_to_netcdf+'mean/'

        if preprocess:
            data_with_final_grid = self.preprocessing(data, preprocess=preprocess,
                                                      model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                      s_time=self.s_time, f_time=self.f_time, s_year=self.s_year,
                                                      f_year=self.f_year, s_month=None, f_month=None, dask_array=False,
                                                      new_unit=self.new_unit)

        if get_mean:
            if seasons_bool:
                data_average = self.seasonal_or_monthly_mean(data, preprocess=preprocess,
                                                             seasons_bool=seasons_bool, model_variable=self.model_variable,
                                                             trop_lat=self.trop_lat, new_unit=self.new_unit, coord=coord)

                seasonal_average = data_average[0].to_dataset(name="DJF")
                seasonal_average["MAM"], seasonal_average["JJA"] = data_average[1], data_average[2]
                seasonal_average["SON"], seasonal_average["Yearly"] = data_average[3], data_average[4]
            else:
                data_average = self.mean_along_coordinate(data, preprocess=preprocess, glob=glob,
                                                          model_variable=self.model_variable, trop_lat=trop_lat,
                                                          coord=coord, s_time=self.s_time, f_time=self.f_time,
                                                          s_year=self.s_year, f_year=self.f_year,
                                                          s_month=self.s_month, f_month=self.f_month)
        if get_median:
            data_average = self.median_along_coordinate(data, preprocess=preprocess, glob=glob,
                                                        model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                        coord=coord, s_time=self.s_time, f_time=self.f_time,
                                                        s_year=self.s_year, f_year=self.f_year, s_month=self.s_month,
                                                        f_month=self.f_month)

        s_month, f_month = None, None
        self.class_attributes_update(s_month=s_month, f_month=f_month)
        if seasons_bool:
            seasonal_average.attrs = data_with_final_grid.attrs
            seasonal_average = self.grid_attributes(
                data=data_with_final_grid, mtpr_dataset=seasonal_average)
            for variable in ('DJF', 'MAM', 'JJA', 'SON', 'Yearly'):
                seasonal_average[variable].attrs = data_with_final_grid.attrs
                seasonal_average = self.grid_attributes(
                    data=data_with_final_grid, mtpr_dataset=seasonal_average, variable=variable)
            average_dataset = seasonal_average
        else:
            data_average.attrs = data_with_final_grid.attrs
            data_average = self.grid_attributes(
                data=data_with_final_grid,      mtpr_dataset=data_average)
            average_dataset = data_average

        if average_dataset.time_band == []:
            raise Exception('Time band is empty')

        if isinstance(path_to_netcdf, str) and name_of_file is not None:
            return self.dataset_to_netcdf(
                average_dataset, path_to_netcdf=path_to_netcdf, name_of_file=name_of_file+'_'+str(coord))
        else:
            return average_dataset

    def plot_of_average(self, data: xr.Dataset = None, ymax: int = 12, fontsize: int = None, pad: int = 15, save: bool = True,
                        trop_lat: float = None, get_mean: bool = True, get_median: bool = False, legend: str = '_Hidden',
                        projection: bool = False,
                        figsize: int = None, linestyle: str = None, maxticknum: int = 12, color: str = 'tab:blue',
                        model_variable: str = None, ylogscale: bool = False, xlogscale: bool = False, loc: str = 'upper right',
                        add: figure.Figure = None, fig: figure.Figure = None, plot_title: str = None,
                        path_to_pdf: str = None, new_unit: str = None, name_of_file: str = '', pdf_format: bool = True,
                        path_to_netcdf: str = None) -> None:
        """
        Function to plot the mean or median value of the variable in Dataset.

        Args:
            data (xarray.Dataset): The Dataset.
            ymax (int, optional): The maximum value on the y-axis. Defaults to 12.
            fontsize (int, optional): The font size of the plot. Defaults to None.
            pad (int, optional): The padding value. Defaults to 15.
            save (bool, optional): The flag to save the plot. Defaults to True.
            trop_lat (float, optional): The maximumal and minimal tropical latitude values in the Dataset. Defaults to None.
            get_mean (bool, optional): The flag to calculate the mean of the variable. Defaults to True.
            get_median (bool, optional): The flag to calculate the median of the variable. Defaults to False.
            legend (str, optional): The legend of the plot. Defaults to '_Hidden'.
            figsize (int, optional): The size of the plot. Defaults to None.
            linestyle (str, optional): The line style of the plot. Defaults to None.
            maxticknum (int, optional): The maximum number of ticks on the x-axis. Defaults to 12.
            color (str, optional): The color of the plot. Defaults to 'tab:blue'.
            model_variable (str, optional): The name of the variable. Defaults to None.
            ylogscale (bool, optional): The flag to use a logarithmic scale for the y-axis. Defaults to False.
            xlogscale (bool, optional): The flag to use a logarithmic scale for the x-axis. Defaults to False.
            loc (str, optional): The location of the legend. Defaults to 'upper right'.
            add (matplotlib.figure.Figure, optional): The add previously created figure to plot. Defaults to None.
            fig (matplotlib.figure.Figure, optional): The add previously created figure to plot. Defaults to None.
            plot_title (str, optional): The title of the plot. Defaults to None.
            path_to_pdf (str, optional): The path to the pdf file. Defaults to None.
            new_unit (str, optional): The unit of the model variable. Defaults to None.
            name_of_file (str, optional): The name of the file. Defaults to ''.
            pdf_format (bool, optional): The flag to save the plot in pdf format. Defaults to True.
            path_to_netcdf (str, optional): The path to the NetCDF file. Defaults to None.

        Returns:
            None.
        """
        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf

        if data is None and path_to_netcdf is not None:
            data = self.tools.open_dataset(path_to_netcdf=path_to_netcdf)
        elif path_to_netcdf is None and data is None:
            raise Exception('The path or dataset must be provided.')

        coord_lat, coord_lon = self.coordinate_names(data)

        if coord_lat is not None:
            coord = coord_lat
            self.logger.debug("Latitude coordinate is used.")
        elif coord_lon is not None:
            coord = coord_lon
            self.logger.debug("Longitude coordinate is used.")
        else:
            raise Exception('Unknown coordinate name')

        if data[coord].size <= 1:
            raise ValueError(
                "The length of the coordinate should be more than 1.")

        if self.new_unit is not None and 'xarray' in str(type(data)):
            data = self.precipitation_rate_units_converter(data, new_unit=self.new_unit)
            units = self.new_unit
        else:
            units = data.units
        y_lim_max = self.precipitation_rate_units_converter(ymax, old_unit=data.units, new_unit=self.new_unit)

        ylabel = self.model_variable+', '+str(units)
        if plot_title is None:
            if get_mean:
                plot_title = 'Mean values of ' + self.model_variable
            elif get_median:
                plot_title = 'Median values of ' + self.model_variable

        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + 'mean'+'_along_'+str(coord)+'.pdf'

        return self.plots.plot_of_average(data=data, trop_lat=self.trop_lat, ylabel=ylabel, coord=coord, fontsize=fontsize,
                                          pad=pad, y_lim_max=y_lim_max, legend=legend, figsize=figsize, linestyle=linestyle,
                                          maxticknum=maxticknum, color=color, ylogscale=ylogscale, xlogscale=xlogscale,
                                          projection=projection,
                                          loc=loc, add=add, fig=fig, plot_title=plot_title, path_to_pdf=path_to_pdf,
                                          save=save, pdf_format=pdf_format)

    def get_seasonal_or_monthly_data(self, data: xr.DataArray, preprocess: bool = True, seasons_bool: bool = True,
                                     model_variable: str = None, trop_lat: float = None, new_unit: str = None) -> xr.DataArray:
        """
        Function to retrieve seasonal or monthly data.

        Args:
            data (xarray.DataArray): Data to be processed.
            preprocess (bool, optional): If True, the data will be preprocessed. Default is True.
            seasons_bool (bool, optional): If True, the data will be calculated for the seasons. Default is True.
            model_variable (str, optional): Name of the model variable. Default is 'mtpr'.
            trop_lat (float, optional): Latitude of the tropical region. Default is None.
            new_unit (str, optional): New unit of the data. Default is None.

        Returns:
            xarray.DataArray: Seasonal or monthly data.
        """
        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if seasons_bool:
            seasons = {
                'DJF_1': {'s_month': 12, 'f_month': 12},
                'DJF_2': {'s_month': 1, 'f_month': 2},
                'MAM': {'s_month': 3, 'f_month': 5},
                'JJA': {'s_month': 6, 'f_month': 8},
                'SON': {'s_month': 9, 'f_month': 11}
            }

            global_data = self.preprocessing(data, preprocess=preprocess, trop_lat=self.trop_lat,
                                             model_variable=self.model_variable, new_unit=self.new_unit)

            preprocessed_data = {}
            for key, value in seasons.items():
                preprocessed_data[key] = self.preprocessing(data, preprocess=preprocess, trop_lat=self.trop_lat,
                                                            model_variable=self.model_variable, s_month=value['s_month'],
                                                            f_month=value['f_month'])
                if self.new_unit is not None:
                    preprocessed_data[key] = self.precipitation_rate_units_converter(preprocessed_data[key],
                                                                                     new_unit=self.new_unit)

            DJF_data = xr.concat([preprocessed_data['DJF_1'], preprocessed_data['DJF_2']], dim='time')
            seasonal_data = [DJF_data, preprocessed_data['MAM'], preprocessed_data['JJA'], preprocessed_data['SON'],
                             global_data]

            return seasonal_data
        else:
            all_monthly_data = []
            for i in range(1, 13):
                if preprocess:
                    monthly_data = self.preprocessing(data, preprocess=preprocess, trop_lat=self.trop_lat,
                                                      model_variable=self.model_variable, s_month=i, f_month=i)
                    if self.new_unit is not None:
                        monthly_data = self.precipitation_rate_units_converter(monthly_data, new_unit=self.new_unit)
                all_monthly_data.append(monthly_data)
            return all_monthly_data

    def seasonal_or_monthly_mean(self, data: xr.DataArray, preprocess: bool = True, seasons_bool: bool = True,
                                 model_variable: str = None, trop_lat: float = None, new_unit: str = None,
                                 coord: str = None, positive: bool = True) -> xr.DataArray:
        """ Function to calculate the seasonal or monthly mean of the data.

        Args:
            data (xarray.DataArray):        Data to be calculated.
            preprocess (bool, optional):    If True, the data will be preprocessed.                 The default is True.
            seasons_bool (bool, optional):       If True, the data will be calculated for the seasons.   The default is True.
            model_variable (str, optional): Name of the model variable.                             The default is 'mtpr'.
            trop_lat (float, optional):     Latitude of the tropical region.                        The default is None.
            new_unit (str, optional):       New unit of the data.                                   The default is None.
            coord (str, optional):          Name of the coordinate.                                 The default is None.

        Returns:
            xarray.DataArray:             Seasonal or monthly mean of the data.

        """

        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if seasons_bool:
            [DJF, MAM, JJA, SON, glob] = self.get_seasonal_or_monthly_data(data, preprocess=preprocess,
                                                                           seasons_bool=seasons_bool,
                                                                           model_variable=self.model_variable,
                                                                           trop_lat=self.trop_lat, new_unit=self.new_unit)
            if positive:
                DJF = np.maximum(DJF, 0.)
                MAM = np.maximum(MAM, 0.)
                JJA = np.maximum(JJA, 0.)
                SON = np.maximum(SON, 0.)
                glob = np.maximum(glob, 0.)
            glob_mean = glob.mean('time')
            DJF_mean = DJF.mean('time')
            MAM_mean = MAM.mean('time')
            JJA_mean = JJA.mean('time')
            SON_mean = SON.mean('time')
            if coord == 'lon' or coord == 'lat':
                DJF_mean = DJF_mean.mean(coord)
                MAM_mean = MAM_mean.mean(coord)
                JJA_mean = JJA_mean.mean(coord)
                SON_mean = SON_mean.mean(coord)
                glob_mean = glob_mean.mean(coord)
            seasons = [DJF_mean, MAM_mean, JJA_mean, SON_mean, glob_mean]
            return seasons
        else:
            months = self.get_seasonal_or_monthly_data(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                       model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                       new_unit=self.new_unit)

            for i in range(1, 13):
                mon_mean = months[i].mean('time')
                months[i] = mon_mean
            return months

    def plot_bias(self, data: xr.DataArray, dataset_2: xr.DataArray, preprocess: bool = True, seasons_bool: bool = True,
                  model_variable: str = None, figsize: float = None, save: bool = True, trop_lat: float = None,
                  plot_title: str = None, new_unit: str = None, vmin: float = None, vmax: float = None,
                  path_to_pdf: str = None, name_of_file: str = '', pdf_format: bool = True) -> None:
        """ Function to plot the bias of model_variable between two datasets.

        Args:
            data (xarray): First dataset to be plotted
            dataset_2 (xarray):   Second dataset to be plotted
            preprocess (bool, optional):    If True, data is preprocessed.              Defaults to True.
            seasons_bool (bool, optional):  If True, data is plotted in seasons. If False, data is plotted in months.
                                            Defaults to True.
            model_variable (str, optional): Name of the model variable.                 Defaults to 'mtpr'.
            figsize (float, optional):      Size of the figure.                         Defaults to 1.
            trop_lat (float, optional):     Latitude band of the tropical region.       The default is None.
            new_unit (str, optional):       New unit of the data.                       The default is None.
            contour (bool, optional):       If True, contour is plotted.                The default is True.
            path_to_pdf (str, optional):    Path to the pdf file.                       The default is None.
            name_of_file(str, optional):    Name of the file.                           The default is None.
            pdf_format(bool, optional):     If True, the figure is saved in PDF format. The default is True.

        Returns:
            The pyplot figure in the PDF format
        """
        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        if seasons_bool:
            months = None
            try:
                seasons = [data.DJF, data.MAM, data.JJA, data.SON, data.Yearly]
            except AttributeError:
                seasons = self.seasonal_or_monthly_mean(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                        model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                        new_unit=self.new_unit)
            try:
                seasons = [dataset_2.DJF, dataset_2.MAM, dataset_2.JJA, dataset_2.SON, dataset_2.Yearly]
            except AttributeError:
                seasons_2 = self.seasonal_or_monthly_mean(dataset_2, preprocess=preprocess,
                                                          seasons_bool=seasons_bool, model_variable=self.model_variable,
                                                          trop_lat=self.trop_lat, new_unit=self.new_unit)

            for i in range(0, len(seasons)):
                seasons[i].values = seasons[i].values - seasons_2[i].values
        else:
            seasons = None
            months = self.seasonal_or_monthly_mean(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                   model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                   new_unit=self.new_unit)

            months_2 = self.seasonal_or_monthly_mean(dataset_2, preprocess=preprocess, seasons_bool=seasons_bool,
                                                     model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                     new_unit=self.new_unit)
            for i in range(0, len(months)):
                months[i].values = months[i].values - months_2[i].values
        if self.new_unit is None:
            try:
                unit = data[self.model_variable].units
            except KeyError:
                unit = data.units
        else:
            unit = self.new_unit
        cbarlabel = self.model_variable+", ["+str(unit)+"]"

        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf
        if isinstance(path_to_pdf, str) and name_of_file is not None:
            if seasons_bool:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_seasonal_bias.pdf'
            else:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_monthly_bias.pdf'
        return self.plots.plot_seasons_or_months(data=data, cbarlabel=cbarlabel, seasons=seasons, months=months,
                                                 figsize=figsize, plot_title=plot_title,  vmin=vmin, vmax=vmax, save=save,
                                                 path_to_pdf=path_to_pdf, pdf_format=pdf_format)

    def plot_seasons_or_months(self, data: xr.DataArray, preprocess: bool = True, seasons_bool: bool = True,
                               model_variable: str = None, figsize: float = None, save: bool = True,
                               trop_lat: float = None, plot_title: str = None, new_unit: str = None,
                               vmin: float = None, vmax: float = None, get_mean: bool = True, percent95_level: bool = False,
                               path_to_pdf: str = None, path_to_netcdf: str = None, name_of_file: str = '',
                               pdf_format: bool = True, value: float = 0.95, rel_error: float = 0.1) -> None:
        """ Function to plot seasonal data.

        Args:
            data (xarray): First dataset to be plotted
            preprocess (bool, optional):    If True, data is preprocessed.          Defaults to True.
            seasons_bool (bool, optional):  If True, data is plotted in seasons. If False, data is plotted in months.
                                            Defaults to True.
            model_variable (str, optional): Name of the model variable.             Defaults to 'mtpr'.
            figsize (float, optional):      Size of the figure.                     Defaults to 1.
            trop_lat (float, optional):     Latitude of the tropical region.        Defaults to None.
            plot_title (str, optional):     Title of the plot.                      Defaults to None.
            new_unit (str, optional):       Unit of the data.                       Defaults to None.
            vmin (float, optional):         Minimum value of the colorbar.          Defaults to None.
            vmax (float, optional):         Maximum value of the colorbar.          Defaults to None.
            contour (bool, optional):       If True, contours are plotted.          Defaults to True.
            path_to_pdf (str, optional):    Path to the pdf file.                   Defaults to None.
            path_to_netcdf (str, optional): Path to the netcdf file.                Defaults to None.
            name_of_file (str, optional):   Name of the pdf file.                   Defaults to None.
            pdf_format (bool, optional):    If True, the figure is saved in PDF format. Defaults to True.

        Returns:
            The pyplot figure in the PDF format
        """

        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if seasons_bool:
            months = None
            try:
                seasons = [data.DJF, data.MAM, data.JJA, data.SON, data.Yearly]
            except AttributeError:
                if get_mean:
                    seasons = self.seasonal_or_monthly_mean(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                            model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                            new_unit=self.new_unit)
                elif percent95_level:
                    seasons = self.seasonal_095level_into_netcdf(data, reprocess=preprocess, seasons_bool=seasons_bool,
                                                                 new_unit=self.new_unit, model_variable=self.model_variable,
                                                                 path_to_netcdf=path_to_netcdf,
                                                                 name_of_file=name_of_file, trop_lat=self.trop_lat,
                                                                 value=value, rel_error=rel_error)
        else:
            seasons = None
            months = self.seasonal_or_monthly_mean(data, preprocess=preprocess, seasons_bool=seasons_bool,
                                                   model_variable=self.model_variable, trop_lat=self.trop_lat,
                                                   new_unit=self.new_unit)
        if self.new_unit is None:
            try:
                unit = data[self.model_variable].units
            except KeyError:
                unit = data.units
        else:
            unit = self.new_unit
        cbarlabel = self.model_variable+", ["+str(unit)+"]"

        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf
        if isinstance(path_to_pdf, str) and name_of_file is not None:
            if seasons_bool:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_seasons.pdf'
            else:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_months.pdf'
        return self.plots.plot_seasons_or_months(data=data, cbarlabel=cbarlabel, seasons=seasons, months=months,
                                                 figsize=figsize, plot_title=plot_title,  vmin=vmin, vmax=vmax, save=save,
                                                 path_to_pdf=path_to_pdf, pdf_format=pdf_format)

    def map(self, data, titles: str = None, lonmin: int = -180, lonmax: int = 181, latmin: int = -90, latmax: int = 91,
            cmap: str = None, pacific_ocean: bool = False, atlantic_ocean: bool = False, indian_ocean: bool = False,
            tropical: bool = False, save: bool = True, model_variable: str = None, figsize: int = None,
            number_of_axe_ticks: int = None, number_of_bar_ticks: int = None, fontsize: int = None, trop_lat: float = None,
            plot_title: str = None, new_unit: str = None, vmin: float = None, vmax: float = None, time_selection: str = '01',
            path_to_pdf: str = None, name_of_file: str = '', pdf_format: bool = None):
        """
        Create a map with specified data and various optional parameters.

        Args:
            data (dtype): The data to be used for mapping.
            titles (str): The title for the map.
            lonmin (int): The minimum longitude for the map.
            lonmax (int): The maximum longitude for the map.
            latmin (int): The minimum latitude for the map.
            latmax (int): The maximum latitude for the map.
            pacific_ocean (bool): Whether to include the Pacific Ocean.
            atlantic_ocean (bool): Whether to include the Atlantic Ocean.
            indian_ocean (bool): Whether to include the Indian Ocean.
            tropical (bool): Whether to focus on tropical regions.
            model_variable (str): The model variable to use.
            figsize (int): The size of the figure.
            number_of_axe_ticks (int): The number of ticks to display.
            time_selection (str): The time selection to use.
            fontsize (int): The font size for the plot.
            cmap (str): The color map to use.
            number_of_bar_ticks (int): The number of ticks to display.
            trop_lat (dtype): The latitude for tropical focus.
            plot_title (str): The title for the plot.
            new_unit (dtype): The new unit for the data.
            vmin (dtype): The minimum value for the color scale.
            vmax (dtype): The maximum value for the color scale.
            path_to_pdf (str): The path to save the map as a PDF file.
            name_of_file (str): The name of the file.
            pdf_format (bool): Whether to save the map in PDF format.

        Returns:
            The pyplot figure in the PDF format
        """

        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)
        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf
        data = data if isinstance(data, list) else [data]
        if self.new_unit is None:
            try:
                unit = data[0][self.model_variable].units
            except KeyError:
                unit = data[0].units
        else:
            unit = self.new_unit

        for i in range(0, len(data)):
            if any((pacific_ocean, atlantic_ocean, indian_ocean, tropical)):
                lonmin, lonmax, latmin, latmax = self.tools.zoom_in_data(trop_lat=self.trop_lat,
                                                                         pacific_ocean=pacific_ocean,
                                                                         atlantic_ocean=atlantic_ocean,
                                                                         indian_ocean=indian_ocean, tropical=tropical)

            if lonmin != -180 or lonmax not in (180, 181):
                data[i] = data[i].sel(lon=slice(lonmin, lonmax))
            if latmin != -90 or latmax not in (90, 91):
                data[i] = data[i].sel(lat=slice(latmin-1, latmax))

            data[i] = data[i].where(data[i] > vmin)

            if data[i].time.size == 1:
                pass
            else:
                time_selection = self.tools.improve_time_selection(data[i], time_selection=time_selection)
                data[i] = data[i].sel(time=time_selection)
                if data[i].time.size != 1:
                    self.logger.error('The time selection went wrong. Please check the value of input time.')

            try:
                data[i] = data[i][self.model_variable]
            except KeyError:
                pass

            if self.new_unit is not None:
                data[i] = self.precipitation_rate_units_converter(data[i], model_variable=self.model_variable,
                                                                  new_unit=self.new_unit)

        cbarlabel = self.model_variable+", ["+str(unit)+"]"
        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_map.pdf'

        return self.plots.map(data=data, titles=titles, lonmin=lonmin, lonmax=lonmax, latmin=latmin, latmax=latmax,
                              cmap=cmap, fontsize=fontsize, save=save, model_variable=self.model_variable,
                              figsize=figsize, number_of_axe_ticks=number_of_axe_ticks,
                              number_of_bar_ticks=number_of_bar_ticks, cbarlabel=cbarlabel, plot_title=plot_title,
                              vmin=vmin, vmax=vmax, path_to_pdf=path_to_pdf, pdf_format=pdf_format)

    def get_95percent_level(self, data=None, original_hist=None, value: float = 0.95, preprocess: bool = True,
                            rel_error: float = 0.1, model_variable: str = None, new_unit: str = None, weights=None,
                            trop_lat: float = None):
        """
        Calculate the precipitation rate threshold value at which a specified percentage (1 - value) of the data is below it.

        Args:
            data (xarray.Dataset): The dataset containing the data to analyze.
            original_hist (xarray.Dataset): The original histogram of the data (optional).
            value (float): The desired percentage (between 0 and 1) of data below the threshold.
            preprocess (bool): Whether to preprocess the data (e.g., filtering).
            rel_error (float): The relative error allowed when calculating the threshold.
            model_variable (str): The model variable to use for analysis.
            new_unit (str): The desired unit for precipitation rate conversion (optional).
            weights (xarray.DataArray): Weights associated with the data (optional).
            trop_lat (float): The latitude value for tropical focus (optional).

        Returns:
            float: The calculated threshold value for the specified percentage.
            str: The unit of the threshold value.
            float: The actual percentage of data below the threshold.
        """
        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        if self.new_unit is not None:
            data = self.precipitation_rate_units_converter(
                data, new_unit=self.new_unit)
            units = self.new_unit

        value = 1 - value
        rel_error = value*rel_error
        if original_hist is None:
            original_hist = self.histogram(data, weights=weights, preprocess=preprocess,
                                           trop_lat=self.trop_lat, model_variable=self.model_variable,
                                           num_of_bins=self.num_of_bins, first_edge=self.first_edge,
                                           width_of_bin=self.width_of_bin, bins=self.bins)

        counts_sum = sum(original_hist.counts)
        relative_value = [float((original_hist.counts[i]/counts_sum).values)
                          for i in range(0, len(original_hist.counts))]
        new_sum = 0

        for i in range(len(relative_value)-1, 0, -1):
            new_sum += relative_value[i]
            if new_sum > 0.05:
                break

        bin_i = float(original_hist.center_of_bin[i-1].values)
        del_bin = float(
            original_hist.center_of_bin[i].values) - float(original_hist.center_of_bin[i-1].values)
        last_bin = float(original_hist.center_of_bin[-1].values)

        self.num_of_bins = None
        self.first_edge = None
        self.width_of_bin = None

        for i in range(0, 100):
            self.bins = np.sort([0, bin_i + 0.5*del_bin, last_bin])
            new_hist = self.histogram(data)

            counts_sum = sum(new_hist.counts.values)
            threshold = new_hist.counts[-1].values/counts_sum
            if abs(threshold-value) < rel_error:
                break
            if threshold < value:
                del_bin = del_bin - abs(0.5*del_bin)
            else:
                del_bin = del_bin + abs(0.5*del_bin)

        try:
            units = data[self.model_variable].units
        except KeyError:
            units = data.units

        bin_value = bin_i + del_bin

        return bin_value, units, 1 - threshold

    def seasonal_095level_into_netcdf(self, data, preprocess: bool = True, seasons_bool: bool = True,
                                      model_variable: str = None, path_to_netcdf: str = None, name_of_file: str = None,
                                      trop_lat: float = None, value: float = 0.95, rel_error: float = 0.1,
                                      new_unit: str = None, lon_length: int = None, lat_length: int = None,
                                      space_grid_factor: int = None, tqdm: bool = False):
        """ Function to plot.

        Args:
            data (xarray): The data to be used for plotting.
            preprocess (bool): Whether to preprocess the data.
            seasons_bool (bool): Whether to use seasons for plotting.
            model_variable (str): The model variable to use for plotting.
            path_to_netcdf (str): The path to the netCDF file.
            name_of_file (str): The name of the file.
            trop_lat (float): The latitude value for the tropical region.
            value (float): The specified value for calculation.
            rel_error (float): The relative error allowed for the threshold.
            new_unit (str): The new unit for the data.
            lon_length (int): The length of the longitude.
            lat_length (int): The length of the latitude.
            space_grid_factor (int): The factor for the space grid.
            tqdm (bool): Whether to show the progress bar.

        Returns:
            The calculated seasonal 95th percentile level.
        """
        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        data = self.tools.space_regrider(data, space_grid_factor=space_grid_factor,
                                         lat_length=lat_length, lon_length=lon_length)

        self.class_attributes_update(trop_lat=trop_lat)
        if seasons_bool:
            [DJF, MAM, JJA, SON, glob] = self.get_seasonal_or_monthly_data(data, preprocess=preprocess,
                                                                           seasons_bool=seasons_bool,
                                                                           model_variable=self.model_variable,
                                                                           trop_lat=trop_lat,
                                                                           new_unit=self.new_unit)

            num_of_bins, first_edge, width_of_bin, bins = self.num_of_bins, self.first_edge, self.width_of_bin, self.bins
            self.s_month, self.f_month = None, None
            s_month, f_month = None, None
            progress_bar_template = "[{:<40}] {}%"
            for lat_i in range(0, DJF.lat.size):

                for lon_i in range(0, DJF.lon.size):
                    if tqdm:
                        ratio = ((DJF.lon.size-1)*lat_i + lon_i) / \
                            (DJF.lat.size*DJF.lon.size)
                        progress = int(40 * ratio)
                        print(progress_bar_template.format(
                            "=" * progress, int(ratio * 100)), end="\r")

                    self.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    DJF_095level = DJF.isel(time=0).copy(deep=True)
                    self.logger.debug('DJF:{}'.format(DJF))
                    bin_value, units, threshold = self.get_95percent_level(DJF.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    DJF_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    MAM_095level = MAM.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(MAM.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    MAM_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    JJA_095level = JJA.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(JJA.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    JJA_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    SON_095level = SON.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(SON.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    SON_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    glob_095level = glob.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(glob.isel(lat=lat_i).isel(lon=lon_i),
                                                                           preprocess=False, value=value, rel_error=rel_error)
                    glob_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

            seasonal_095level = DJF_095level.to_dataset(name="DJF")
            seasonal_095level["MAM"] = MAM_095level
            seasonal_095level["JJA"] = JJA_095level
            seasonal_095level["SON"] = SON_095level
            seasonal_095level["Yearly"] = glob_095level

            s_month, f_month = None, None
            self.class_attributes_update(
                s_month=s_month,       f_month=f_month)

            seasonal_095level.attrs = SON.attrs
            seasonal_095level = self.grid_attributes(
                data=SON, mtpr_dataset=seasonal_095level)
            for variable in ('DJF', 'MAM', 'JJA', 'SON', 'Yearly'):
                seasonal_095level[variable].attrs = SON.attrs
                seasonal_095level = self.grid_attributes(
                    data=SON, mtpr_dataset=seasonal_095level, variable=variable)

        if seasonal_095level.time_band == []:
            raise Exception('Time band is empty')
        if isinstance(path_to_netcdf, str) and name_of_file is not None:
            self.dataset_to_netcdf(
                seasonal_095level, path_to_netcdf=path_to_netcdf, name_of_file=name_of_file)
        else:
            return seasonal_095level

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
        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

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
        new_dataset = self.grid_attributes(data=data_final_grid, mtpr_dataset=new_dataset)
        # Calculate relative mtpr and add to the dataset
        mean_val = new_dataset['mtpr'].mean()
        new_dataset['mtpr_relative'] = (new_dataset['mtpr'] - mean_val) / mean_val
        new_dataset['mtpr_relative'].attrs = new_dataset.attrs

        if path_to_netcdf is None and self.path_to_netcdf is not None:
                path_to_netcdf = self.path_to_netcdf+'daily_variability/'
        
        if name_of_file is not None:
            self.dataset_to_netcdf(
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

        self.class_attributes_update(trop_lat=trop_lat, model_variable=model_variable, new_unit=new_unit)

        if path_to_pdf is None:
            path_to_pdf = self.path_to_pdf

        if path_to_netcdf is None:
            raise Exception('The path needs to be provided')
        else:
            data = self.tools.open_dataset(path_to_netcdf=path_to_netcdf)
        if 'Dataset' in str(type(data)):
            y_lim_max = self.precipitation_rate_units_converter(ymax, old_unit=data.units, new_unit=self.new_unit)
            data[self.model_variable] = self.precipitation_rate_units_converter(data[self.model_variable],
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