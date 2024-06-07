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

import dask.array as da

from aqua.util import create_folder
from aqua.logger import log_configure

from .tropical_rainfall_tools import ToolsClass


class TropicalPrecipitationDataManager:
    """
    This class manages tropical precipitation data, including unit conversion, data filtering by latitude and time,
    preprocessing datasets, and saving them as NetCDF files.."""

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
