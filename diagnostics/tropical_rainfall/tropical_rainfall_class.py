"""The module contains Tropical Precipitation Diagnostic:

.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""

import re
from os import listdir
from os.path import isfile, join
from datetime import datetime
import numpy as np
import xarray as xr

from datetime import datetime
# from timezonefinder import TimezoneFinder
import pytz

from itertools import groupby
from statistics import mean


import matplotlib.pyplot as plt
import matplotlib.colors as colors
# import boost_histogram as bh  # pip
from matplotlib.gridspec import GridSpec
import seaborn as sns

import dask.array as da
import dask_histogram as dh  # pip
import dask_histogram.boost as dhb
import dask
import fast_histogram

from aqua.util import create_folder
from aqua.logger import log_configure


import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
from cartopy.util import add_cyclic_point


from aqua import Reader
from aqua.util import create_folder

from .tropical_rainfall_func import ToolsClass
from .tropical_rainfall_plot import PlottingClass 

class Tropical_Rainfall:
    """This class is a minimal version of the Tropical Precipitation Diagnostic."""

    def __init__(self,
                 trop_lat=10,
                 s_time=None,
                 f_time=None,
                 s_year=None,
                 f_year=None,
                 s_month=None,
                 f_month=None,
                 num_of_bins=None,
                 first_edge=None,
                 width_of_bin=None,
                 bins=0,
                 loglevel: str = 'WARNING'):
        """ The constructor of the class.

        Args:
            trop_lat (int or float, optional):      The latitude of the tropical zone.      Defaults to 10.
            s_time (int or str, optional):          The start time of the time interval.    Defaults to None.
            f_time (int or str, optional):          The end time of the time interval.      Defaults to None.
            s_year (int, optional):                 The start year of the time interval.    Defaults to None.
            f_year (int, optional):                 The end year of the time interval.      Defaults to None.
            s_month (int, optional):                The start month of the time interval.   Defaults to None.
            f_month (int, optional):                The end month of the time interval.     Defaults to None.
            num_of_bins (int, optional):            The number of bins.                     Defaults to None.
            first_edge (int or float, optional):    The first edge of the bin.              Defaults to None.
            width_of_bin (int or float, optional):  The width of the bin.                   Defaults to None.
            bins (np.ndarray, optional):            The bins.                               Defaults to 0."""

        self.trop_lat = trop_lat
        self.s_time = s_time
        self.f_time = f_time
        self.s_year = s_year
        self.f_year = f_year
        self.s_month = s_month
        self.f_month = f_month
        self.num_of_bins = num_of_bins
        self.first_edge = first_edge
        self.width_of_bin = width_of_bin
        self.bins = bins
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'Trop. Rainfall')
        self.plots = PlottingClass(loglevel=loglevel)
        self.tools = ToolsClass()
        

    

    def class_attributes_update(self,             trop_lat=None,        s_time=None,          f_time=None,
                                s_year=None,      f_year=None,          s_month=None,         f_month=None,
                                num_of_bins=None, first_edge=None,      width_of_bin=None,    bins=0):
        """ Function to update the class attributes.

        Args:
            trop_lat (int or float, optional):      The latitude of the tropical zone.      Defaults to 10.
            s_time (int or str, optional):          The start time of the time interval.    Defaults to None.
            f_time (int or str, optional):          The end time of the time interval.      Defaults to None.
            s_year (int, optional):                 The start year of the time interval.    Defaults to None.
            f_year (int, optional):                 The end year of the time interval.      Defaults to None.
            s_month (int, optional):                The start month of the time interval.   Defaults to None.
            f_month (int, optional):                The end month of the time interval.     Defaults to None.
            num_of_bins (int, optional):            The number of bins.                     Defaults to None.
            first_edge (int or float, optional):    The first edge of the bin.              Defaults to None.
            width_of_bin (int or float, optional):  The width of the bin.                   Defaults to None.
            bins (np.ndarray, optional):            The bins.                               Defaults to 0. """
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

    def coordinate_names(self, data):
        """ Function to get the names of the coordinates."""

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

    def precipitation_rate_units_converter(self, data, model_variable='tprate', old_unit=None,  new_unit='kg m**-2 s**-1'):
        """
        Function to convert the units of precipitation rate.

        Args:
            data (xarray):                  The Dataset
            model_variable (str, optional): The name of the variable to be converted.   Defaults to 'tprate'.
            new_unit (str, optional):       The new unit of the variable.               Defaults to 'm s**-1'.

        Returns:
            xarray: The Dataset with converted units.
        """
        try:
            data = data[model_variable]
        except (TypeError, KeyError):
            pass
        if 'xarray' in str(type(data)):
            if data.units == new_unit:
                return data

        if isinstance(data, (float, int, np.ndarray)) and old_unit is not None:
            from_mass_unit, from_space_unit, from_time_unit = self.tools.unit_splitter(
                old_unit)
        else:
            from_mass_unit, from_space_unit, from_time_unit = self.tools.unit_splitter(
                data.units)
            old_unit = data.units
        _,   to_space_unit,   to_time_unit = self.tools.unit_splitter(new_unit)

        if old_unit == 'kg m**-2 s**-1':
            data = 0.001 * data
            data = self.tools.convert_length(data,   from_space_unit, to_space_unit)
            data = self.tools.convert_time(data,     from_time_unit,  to_time_unit)
        elif from_mass_unit is None and new_unit == 'kg m**-2 s**-1':
            data = self.tools.convert_length(data,   from_space_unit, 'm')
            data = self.tools.convert_time(data,     from_time_unit,  's')
            data = 1000 * data
        else:
            data = self.tools.convert_length(data,   from_space_unit, to_space_unit)
            data = self.tools.convert_time(data,     from_time_unit,  to_time_unit)
        if 'xarray' in str(type(data)):
            data.attrs['units'] = new_unit
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history_update = str(current_time)+' the units of precipitation are converted from ' + \
                str(data.units) + ' to ' + str(new_unit) + ';\n '
            try:
                history_attr = data.attrs['history'] + history_update
                data.attrs['history'] = history_attr
            except AttributeError or KeyError:
                data.attrs['history'] = history_update
        return data

    def latitude_band(self, data, trop_lat=None):
        """ Function to select the Dataset for specified latitude range

        Args:
            data (xarray):                  The Dataset
            trop_lat (int/float, optional): The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.

        Returns:
            xarray: The Dataset only for selected latitude range.
        """

        self.class_attributes_update(trop_lat=trop_lat)

        coord_lat, _ = self.coordinate_names(data)
        self.class_attributes_update(trop_lat=trop_lat)
        return data.where(abs(data[coord_lat]) <= self.trop_lat, drop=True)

    def time_band(self, data,
                  s_time=None,        f_time=None,
                  s_year=None,        f_year=None,
                  s_month=None,       f_month=None):
        """ Function to select the Dataset for specified time range

        Args:
            data (xarray):                  The Dataset
            s_time (str, optional):         The starting time of the Dataset.       Defaults to None.
            f_time (str, optional):         The ending time of the Dataset.         Defaults to None.
            s_year (str, optional):         The starting year of the Dataset.       Defaults to None.
            f_year (str, optional):         The ending year of the Dataset.         Defaults to None.
            s_month (str, optional):        The starting month of the Dataset.      Defaults to None.
            f_month (str, optional):        The ending month of the Dataset.        Defaults to None.

        Returns:
            xarray: The Dataset only for selected time range.
        """
        self.class_attributes_update(s_time=s_time,         f_time=f_time,
                                     s_year=s_year,         f_year=f_year,
                                     s_month=s_month,       f_month=f_month)

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
            if s_time is not None and f_time is not None:
                _s = re.split(r"[^a-zA-Z0-9\s]", s_time)
                _f = re.split(r"[^a-zA-Z0-9\s]", f_time)
                if len(_s) == 1:
                    s_time = _s[0]
                elif len(_f) == 1:
                    f_time = _f[0]

                elif len(_s) == 2:
                    s_time = _s[0]+'-'+_s[1]
                elif len(_f) == 2:
                    f_time = _f[0]+'-'+_f[1]

                elif len(_s) == 3:
                    s_time = _s[0]+'-' + _s[1] + '-'+_s[2]
                elif len(_f) == 3:
                    f_time = _f[0]+'-' + _f[1] + '-' + _f[2]

                elif len(_s) == 4:
                    s_time = _s[0]+'-' + _s[1] + '-'+_s[2]+'-'+_s[3]
                elif len(_f) == 4:
                    f_time = _f[0]+'-' + _f[1] + '-' + _f[2] + '-' + _f[3]

                elif len(_s) == 5:
                    s_time = _s[0] + '-' + _s[1] + \
                        '-'+_s[2]+'-'+_s[3] + '-'+_s[4]
                elif len(_f) == 5:
                    f_time = _f[0] + '-' + _f[1] + '-' + \
                        _f[2] + '-' + _f[3] + '-' + _f[4]
                else:
                    raise ValueError(
                        "Unknown format of time. Try one more time")
            data = data.sel(time=slice(s_time, f_time))
        elif self.s_time is not None and self.f_time is None:
            if isinstance(s_year, str):
                _temp = re.split(r"[^a-zA-Z0-9\s]", s_time)
                if len(_temp) == 1:
                    time = _temp[0]
                elif len(_temp) == 2:
                    time = _temp[0]+'-'+_temp[1]
                elif len(_temp) == 3:
                    time = _temp[0]+'-'+_temp[1]+'-'+_temp[2]
                elif len(_temp) == 3:
                    time = _temp[0]+'-'+_temp[1]+'-'+_temp[2]+'-'+_temp[3]
                elif len(_temp) == 4:
                    time = _temp[0]+'-'+_temp[1]+'-' + \
                        _temp[2]+'-'+_temp[3]+'-'+_temp[4]
                elif len(_temp) == 5:
                    time = _temp[0]+'-'+_temp[1]+'-'+_temp[2] + \
                        '-'+_temp[3]+'-'+_temp[4]+'-'+_temp[5]
                else:
                    raise ValueError(
                        "Unknown format of time. Try one more time")
                data = data.sel(time=slice(time))
        return data

    def dataset_into_1d(self, data, model_variable='tprate', sort=False):
        """ Function to convert Dataset into 1D array.

        Args:
            data (xarray):                      The Dataset
            model_variable (str, optional):     The variable of the Dataset.    Defaults to 'tprate'.
            sort (bool, optional):              The flag to sort the array.     Defaults to False.

        Returns:
            xarray: The 1D array.
        """

        coord_lat, coord_lon = self.coordinate_names(data)

        try:
            data = data[model_variable]
        except KeyError:
            pass

        try:
            data_1d = data.stack(total=['time', coord_lat, coord_lon])
        except KeyError:
            data_1d = data.stack(total=[coord_lat, coord_lon])
        if sort:
            data_1d = data_1d.sortby(data_1d)
        return data_1d

    def preprocessing(self, data,          trop_lat=None,
                      preprocess=True,     model_variable="tprate",
                      s_time=None,         f_time=None,
                      s_year=None,         f_year=None,     new_unit=None,
                      s_month=None,        f_month=None,    dask_array=False):
        """ Function to preprocess the Dataset according to provided arguments and attributes of the class.

        Args:
            data (xarray):                  The Dataset.
            preprocess (bool, optional):    If True, the functiom preprocess Dataset.   Defaults to True.
            model_variable (str, optional): The variable of the Dataset.                        Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset.           Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset.              Defaults to None.
            s_year (int, optional):         The starting year in Dataset.                       Defaults to None.
            f_year (int, optional):         The final year in Dataset.                          Defaults to None.
            s_month (int, optional):        The starting month in Dataset.                      Defaults to None.
            f_month (int, optional):        The final month in Dataset.                         Defaults to None.
            dask_array (bool, optional):    If True, the function return daskarray.     Defaults to False.

        Returns:
            xarray: Preprocessed Dataset according to the arguments of the function
        """

        self.class_attributes_update(trop_lat=trop_lat,
                                     s_time=s_time,       f_time=f_time,        s_month=s_month,
                                     s_year=s_year,       f_year=f_year,        f_month=f_month)
        if preprocess:
            if 'time' in data.coords:
                data_per_time_band = self.time_band(data,
                                                    s_time=self.s_time,        f_time=self.f_time,
                                                    s_year=self.s_year,        f_year=self.f_year,
                                                    s_month=self.s_month,      f_month=self.f_month)
            else:
                data_per_time_band = data

            try:
                data_variable = data_per_time_band[model_variable]
            except KeyError:
                data_variable = data_per_time_band

            data_per_lat_band = self.latitude_band(
                data_variable, trop_lat=self.trop_lat)

            if new_unit is not None:
                data_per_lat_band = self.precipitation_rate_units_converter(
                    data_per_lat_band, new_unit=new_unit)
            
            if dask_array:
                data_1d = self.dataset_into_1d(data_per_lat_band)
                dask_data = da.from_array(data_1d)
                return dask_data
            else:
                return data_per_lat_band
        else:
            return data

    def histogram_lowres(self,               data,                data_with_global_atributes=None,
                         weights=None,       preprocess=True,     trop_lat=None,           model_variable='tprate',
                         s_time=None,        f_time=None,         s_year=None,
                         f_year=None,        s_month=None,        f_month=None,
                         num_of_bins=None,   first_edge=None,     width_of_bin=None,       bins=0,
                         lazy=False,         create_xarray=True,  path_to_histogram=None,  name_of_file=None,
                         positive=True,      threshold=2,         new_unit=None, test=False):
        """ Function to calculate a histogram of the low-resolution Dataset.

        Args:
            data (xarray.Dataset):          The input Dataset.
            preprocess (bool, optional):    If True, preprocesses the Dataset.              Defaults to True.
            trop_lat (float, optional):     The maximum absolute value of tropical latitude in the Dataset. Defaults to 10.
            model_variable (str, optional): The variable of interest in the Dataset.        Defaults to 'tprate'.
            weights (array-like, optional): The weights of the data.                        Defaults to None.
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
            bins (int, optional):           The number of bins for the histogram (alternative argument to 'num_of_bins'). Defaults to 0.
            lazy (bool, optional):          If True, delays computation until necessary.    Defaults to False.
            create_xarray (bool, optional): If True, creates an xarray dataset from the histogram counts. Defaults to True.
            path_to_histogram (str, optional):   The path to save the xarray dataset.       Defaults to None.

        Returns:
            xarray.Dataset or numpy.ndarray: The histogram of the Dataset.
        """
        self.class_attributes_update(trop_lat=trop_lat,
                                     s_time=s_time,           f_time=f_time,
                                     s_year=s_year,           f_year=f_year,
                                     s_month=s_month,         f_month=f_month,
                                     first_edge=first_edge,   num_of_bins=num_of_bins,
                                     width_of_bin=width_of_bin)

        coord_lat, coord_lon = self.coordinate_names(data)

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

        data_original = data
        if preprocess:
            data = self.preprocessing(data, preprocess=preprocess,
                                      model_variable=model_variable,     trop_lat=self.trop_lat,
                                      s_time=self.s_time,                f_time=self.f_time,
                                      s_year=self.s_year,                f_year=self.f_year,
                                      s_month=self.s_month,              f_month=self.f_month,
                                      dask_array=False)

        size_of_the_data = self.tools.data_size(data)

        if new_unit is not None:
            data = self.precipitation_rate_units_converter(
                data, model_variable=model_variable, new_unit=new_unit)
        data_with_final_grid = data
        if weights is not None:

            weights = self.latitude_band(weights, trop_lat=self.trop_lat)
            data, weights = xr.broadcast(data, weights)
            try:
                weights = weights.stack(total=['time', coord_lat, coord_lon])
            except KeyError:
                weights = weights.stack(total=[coord_lat, coord_lon])
            weights_dask = da.from_array(weights)

        if positive:
            data = np.maximum(data, 0.)

        try:
            data_dask = da.from_array(data.stack(
                total=['time', coord_lat, coord_lon]))
        except KeyError:
            data_dask = da.from_array(data.stack(
                total=[coord_lat, coord_lon]))
        if weights is not None:
            counts, edges = dh.histogram(
                data_dask, bins=bins,   weights=weights_dask,    storage=dh.storage.Weight())
        else:
            counts, edges = dh.histogram(
                data_dask, bins=bins,   storage=dh.storage.Weight())
        if not lazy:
            counts = counts.compute()
            edges = edges.compute()
            self.logger.info('Histogram of the data is created')
            self.logger.debug('Size of data after preprocessing/Sum of Counts: {}/{}'
                              .format(self.tools.data_size(data), int(sum(counts))))
            if int(sum(counts)) != size_of_the_data:
                self.logger.warning(
                    'Amount of counts in the histogram is not equal to the size of the data')
                self.logger.info('Check the data and the bins')
        width_table = [edges[i+1]-edges[i] for i in range(0, len(edges)-1)]
        center_of_bin = [edges[i] + 0.5*width_table[i]
                         for i in range(0, len(edges)-1)]
        counts_per_bin = xr.DataArray(
            counts, coords=[center_of_bin], dims=["center_of_bin"])

        counts_per_bin = counts_per_bin.assign_coords(
            width=("center_of_bin", width_table))
        counts_per_bin.attrs = data.attrs
        counts_per_bin.center_of_bin.attrs['units'] = data.units
        counts_per_bin.center_of_bin.attrs['history'] = 'Units are added to the bins to coordinate'
        counts_per_bin.attrs['size_of_the_data'] = size_of_the_data

        if data_with_global_atributes is None:
            data_with_global_atributes = data_original

        if not lazy and create_xarray:
            tprate_dataset = counts_per_bin.to_dataset(name="counts")
            tprate_dataset.attrs = data_with_global_atributes.attrs
            tprate_dataset = self.add_frequency_and_pdf(
                tprate_dataset=tprate_dataset, test=test)

            mean_from_hist, mean_original, mean_modified = self.mean_from_histogram(hist=tprate_dataset, data=data_with_final_grid,
                                                                                    model_variable=model_variable, trop_lat=self.trop_lat, positive=positive)
            relative_discrepancy = abs(
                mean_modified - mean_from_hist)*100/mean_modified
            self.logger.debug('The difference between the mean of the data and the mean of the histogram: {}%'
                              .format(round(relative_discrepancy, 4)))
            if new_unit is None:
                unit = data.units
            else:
                unit = new_unit
            self.logger.debug('The mean of the data: {}{}'
                              .format(mean_original, unit))
            self.logger.debug('The mean of the histogram: {}{}'
                              .format(mean_from_hist, unit))
            if relative_discrepancy > threshold:
                self.logger.warning('The difference between the mean of the data and the mean of the histogram is greater than the threshold. \n \
                                Increase the number of bins and decrease the width of the bins.')
            for variable in (None, 'counts', 'frequency', 'pdf'):
                tprate_dataset = self.grid_attributes(
                    data=data_with_final_grid, tprate_dataset=tprate_dataset, variable=variable)

            if path_to_histogram is not None and name_of_file is not None:
                bins_info = str(bins[0])+'_'+str(bins[-1])+'_'+str(len(bins))
                bins_info = bins_info.replace('.', '-')
                self.dataset_to_netcdf(
                    tprate_dataset, path_to_netcdf=path_to_histogram, name_of_file=name_of_file+'_histogram_'+bins_info)
            return tprate_dataset
        else:
            tprate_dataset = counts_per_bin.to_dataset(name="counts")
            tprate_dataset.attrs = data_with_global_atributes.attrs
            counts_per_bin = self.grid_attributes(
                data=data_with_final_grid, tprate_dataset=tprate_dataset, variable='counts')
            tprate_dataset = self.grid_attributes(
                data=data_with_final_grid, tprate_dataset=tprate_dataset)
            if path_to_histogram is not None and name_of_file is not None:
                bins_info = str(bins[0])+'_'+str(bins[-1])+'_'+str(len(bins)-1)
                bins_info = bins_info.replace('.', '-')
                self.dataset_to_netcdf(
                    tprate_dataset, path_to_netcdf=path_to_histogram, name_of_file=name_of_file+'_histogram_'+bins_info)
            return counts_per_bin

    def histogram(self,                   data,               data_with_global_atributes=None,
                  weights=None,           preprocess=True,    trop_lat=None,              model_variable='tprate',
                  s_time=None,            f_time=None,        s_year=None,
                  f_year=None,            s_month=None,       f_month=None,
                  num_of_bins=None,       first_edge=None,    width_of_bin=None,       bins=0,
                  path_to_histogram=None, name_of_file=None,  positive=True, new_unit=None, threshold=2, test=False, seasons=None):
        """ Function to calculate a histogram of the high-resolution Dataset.

        Args:
            data (xarray.Dataset):          The input Dataset.
            preprocess (bool, optional):    If True, preprocesses the Dataset.              Defaults to True.
            trop_lat (float, optional):     The maximum absolute value of tropical latitude in the Dataset. Defaults to 10.
            model_variable (str, optional): The variable of interest in the Dataset.        Defaults to 'tprate'.
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
            bins (int, optional):           The number of bins for the histogram (alternative argument to 'num_of_bins'). Defaults to 0.
            create_xarray (bool, optional): If True, creates an xarray dataset from the histogram counts. Defaults to True.
            path_to_histogram (str, optional):   The path to save the xarray dataset.       Defaults to None.

        Returns:
            xarray.Dataset or numpy.ndarray: The histogram of the Dataset.
        """
        self.class_attributes_update(trop_lat=trop_lat,
                                     s_time=s_time,           f_time=f_time,
                                     s_year=s_year,           f_year=f_year,
                                     s_month=s_month,         f_month=f_month,
                                     first_edge=first_edge,   num_of_bins=num_of_bins,
                                     width_of_bin=width_of_bin)
        data_original = data
        if preprocess:
            data = self.preprocessing(data, preprocess=preprocess,
                                      model_variable=model_variable,     trop_lat=self.trop_lat,
                                      s_time=self.s_time,                f_time=self.f_time,
                                      s_year=self.s_year,                f_year=self.f_year,
                                      s_month=self.s_month,              f_month=self.f_month,
                                      dask_array=False)
        size_of_the_data = self.tools.data_size(data)

        if new_unit is not None:
            data = self.precipitation_rate_units_converter(
                data, model_variable=model_variable, new_unit=new_unit)
        data_with_final_grid = data

        if seasons is not None:
            if seasons:
                seasons_or_months = self.get_seasonal_or_monthly_data(data,        preprocess=preprocess,        seasons=seasons,
                                                                      model_variable=model_variable,       trop_lat=trop_lat,          new_unit=new_unit)
            else:
                seasons_or_months = self.get_seasonal_or_monthly_data(data,        preprocess=preprocess,        seasons=seasons,
                                                                      model_variable=model_variable,       trop_lat=trop_lat,          new_unit=new_unit)
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
            if seasons is not None:
                for i in range(0, len(seasons_or_months)):
                    seasons_or_months[i] = np.maximum(seasons_or_months[i], 0.)
        if isinstance(self.bins, int):
            hist_fast = fast_histogram.histogram1d(data,
                                                   range=[
                                                       self.first_edge, self.first_edge + (self.num_of_bins)*self.width_of_bin],
                                                   bins=self.num_of_bins)
            hist_seasons_or_months = []
            if seasons is not None:
                for i in range(0, len(seasons_or_months)):
                    hist_seasons_or_months.append(fast_histogram.histogram1d(seasons_or_months[i],
                                                                             range=[
                        self.first_edge, self.first_edge + (self.num_of_bins)*self.width_of_bin],
                        bins=self.num_of_bins))

        else:
            hist_np = np.histogram(data,  weights=weights, bins=self.bins)
            hist_fast = hist_np[0]
            hist_seasons_or_months = []
            if seasons is not None:
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

        tprate_dataset = counts_per_bin.to_dataset(name="counts")
        tprate_dataset.attrs = data_with_global_atributes.attrs
        tprate_dataset = self.add_frequency_and_pdf(
            tprate_dataset=tprate_dataset, test=test)

        if seasons is not None:
            if seasons:
                seasonal_or_monthly_labels = [
                    'DJF', 'MMA', 'JJA', 'SON', 'glob']
            else:
                seasonal_or_monthly_labels = [
                    'J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'J']
            for i in range(0, len(seasons_or_months)):
                tprate_dataset['counts'+seasonal_or_monthly_labels[i]
                               ] = hist_seasons_or_months[i]
                tprate_dataset = self.add_frequency_and_pdf(
                    tprate_dataset=tprate_dataset, test=test, label=seasonal_or_monthly_labels[i])

        mean_from_hist, mean_original, mean_modified = self.mean_from_histogram(hist=tprate_dataset, data=data_with_final_grid,
                                                                                model_variable=model_variable, trop_lat=self.trop_lat, positive=positive)
        relative_discrepancy = (
            mean_original - mean_from_hist)*100/mean_original
        self.logger.debug('The difference between the mean of the data and the mean of the histogram: {}%'
                          .format(round(relative_discrepancy, 4)))
        if new_unit is None:
            unit = data.units
        else:
            unit = new_unit
        self.logger.debug('The mean of the data: {}{}'
                          .format(mean_original, unit))
        self.logger.debug('The mean of the histogram: {}{}'
                          .format(mean_from_hist, unit))
        if relative_discrepancy > threshold:
            self.logger.warning('The difference between the mean of the data and the mean of the histogram is greater than the threshold. \n \
                                Increase the number of bins and decrease the width of the bins.')
        for variable in (None, 'counts', 'frequency', 'pdf'):
            tprate_dataset = self.grid_attributes(
                data=data_with_final_grid, tprate_dataset=tprate_dataset, variable=variable)
            if variable is None:
                tprate_dataset.attrs['units'] = tprate_dataset.counts.units
                tprate_dataset.attrs['mean_of_original_data'] = float(
                    mean_original)
                tprate_dataset.attrs['mean_of_histogram'] = float(
                    mean_from_hist)
                tprate_dataset.attrs['relative_discrepancy'] = float(
                    relative_discrepancy)

            else:
                tprate_dataset[variable].attrs['mean_of_original_data'] = float(
                    mean_original)
                tprate_dataset[variable].attrs['mean_of_histogram'] = float(
                    mean_from_hist)
                tprate_dataset[variable].attrs['relative_discrepancy'] = float(
                    relative_discrepancy)
        if path_to_histogram is not None and name_of_file is not None:
            bins_info = str(bins[0])+'_'+str(bins[-1])+'_'+str(len(bins)-1)
            bins_info = bins_info.replace('.', '-')
            self.dataset_to_netcdf(
                tprate_dataset, path_to_netcdf=path_to_histogram, name_of_file=name_of_file+'_histogram_'+bins_info)

        return tprate_dataset

    def dataset_to_netcdf(self, dataset=None, path_to_netcdf=None, name_of_file=None):
        """ Function to save the histogram.

        Args:
            dataset (xarray, optional):         The Dataset with the histogram.     Defaults to None.
            path_to_netcdf (str, optional):  The path to save the histogram.     Defaults to None.

        Returns:
            str: The path to save the histogram.
        """
        if isinstance(path_to_netcdf, str):
            create_folder(folder=str(path_to_netcdf), loglevel='WARNING')
            if name_of_file is None:
                name_of_file = '_'
            time_band = dataset.attrs['time_band']
            # self.logger.debug('Time band is {}'.format(time_band))
            try:
                name_of_file = name_of_file + '_' + re.split(":", re.split(", ", time_band)[0])[
                    0] + '_' + re.split(":", re.split(", ", time_band)[1])[0] + '_' + re.split("=", re.split(", ", time_band)[2])[1]
            except IndexError:
                name_of_file = name_of_file + '_' + re.split(":", time_band)[0]
            path_to_netcdf = path_to_netcdf + 'trop_rainfall_' + name_of_file + '.nc'

            dataset.to_netcdf(path=path_to_netcdf, mode='w')
            self.logger.info("NetCDF is saved in the storage.")
        else:
            self.logger.debug(
                "The path to save the histogram needs to be provided.")
        return path_to_netcdf

    def grid_attributes(self, data=None,      tprate_dataset=None,      variable=None):
        """ Function to add the attributes with information about the space and time grid to the Dataset.

        Args:
            data (xarray, optional):            The Dataset with a final time and space grif, for which calculations were performed.    Defaults to None.
            tprate_dataset (xarray, optional):  Created Dataset by the diagnostics, which we would like to populate with attributes.    Defaults to None.
            variable (str, optional):           The name of the Variable objects (not a physical variable) of the created Dataset.      Defaults to None.

        Returns:
            xarray.Dataset: The updated dataset with grid attributes. The grid attributes include time_band, lat_band, and lon_band.

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
                latitude_step = data[coord_lat][1].values - \
                    data[coord_lat][0].values
                lat_band = str(data[coord_lat][0].values)+', ' + \
                    str(data[coord_lat][-1].values) + \
                    ', freq='+str(latitude_step)
            else:
                lat_band = data[coord_lat].values
                latitude_step = 'None'
        except KeyError:
            lat_band = 'None'
            latitude_step = 'None'
        try:
            if data[coord_lon].size > 1:
                longitude_step = data[coord_lon][1].values - \
                    data[coord_lon][0].values
                lon_band = str(data[coord_lon][0].values)+', ' + \
                    str(data[coord_lon][-1].values) + \
                    ', freq='+str(longitude_step)
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
                history_attr = tprate_dataset.attrs['history'] + history_update
                tprate_dataset.attrs['history'] = history_attr
            except KeyError:
                self.logger.debug(
                    "The obtained xarray.Dataset doesn't have global attributes. Consider adding global attributes manually to the dataset.")
                pass
            tprate_dataset.attrs['time_band'] = time_band
            tprate_dataset.attrs['lat_band'] = lat_band
            tprate_dataset.attrs['lon_band'] = lon_band
        else:
            tprate_dataset[variable].attrs['time_band'] = time_band
            tprate_dataset[variable].attrs['lat_band'] = lat_band
            tprate_dataset[variable].attrs['lon_band'] = lon_band

        return tprate_dataset

    def add_frequency_and_pdf(self,  tprate_dataset=None, path_to_histogram=None, name_of_file=None,  test=False, label=None):
        """ Function to convert the histogram to xarray.Dataset.

        Args:
            hist_counts (xarray, optional):     The histogram with counts.      Defaults to None.
            path_to_histogram (str, optional):  The path to save the histogram. Defaults to None.

        Returns:
            xarray: The xarray.Dataset with the histogram.
        """
        hist_frequency = self.convert_counts_to_frequency(
            tprate_dataset.counts,  test=test)
        tprate_dataset['frequency'] = hist_frequency

        hist_pdf = self.convert_counts_to_pdf(
            tprate_dataset.counts,  test=test)
        tprate_dataset['pdf'] = hist_pdf

        hist_pdfP = self.convert_counts_to_pdfP(
            tprate_dataset.counts,  test=test)
        tprate_dataset['pdfP'] = hist_pdfP

        if label is not None:
            hist_frequency = self.convert_counts_to_frequency(
                tprate_dataset['counts'+label],  test=test)
            tprate_dataset['frequency'+label] = hist_frequency

            hist_pdf = self.convert_counts_to_pdf(
                tprate_dataset['counts'+label],  test=test)
            tprate_dataset['pdf'+label] = hist_pdf
        if path_to_histogram is not None and name_of_file is not None:
            if isinstance(self.bins, int):
                bins = [self.first_edge + i *
                        self.width_of_bin for i in range(0, self.num_of_bins+1)]
            else:
                bins = self.bins
            bins_info = str(bins[0])+'_'+str(bins[-1])+'_'+str(len(bins)-1)
            bins_info = bins_info.replace('.', '-')
            self.dataset_to_netcdf(
                dataset=tprate_dataset, path_to_netcdf=path_to_histogram, name_of_file=name_of_file+'_histogram_'+bins_info)
        return tprate_dataset

    def open_dataset(self, path_to_netcdf=None):
        """ Function to load a histogram dataset from a file using pickle.

        Args:
            path_to_netcdf (str):       The path to the dataset file.

        Returns:
            object:                     The loaded histogram dataset.

        Raises:
            FileNotFoundError:          If the specified dataset file is not found.

        """
        try:
            dataset = xr.open_dataset(path_to_netcdf)
            return dataset
        except FileNotFoundError:
            raise FileNotFoundError(
                "The specified dataset file was not found.")

    def merge_two_datasets(self, tprate_dataset_1=None, tprate_dataset_2=None,  test=False):
        """ Function to merge two datasets.

        Args:
            tprate_dataset_1 (xarray, optional):    The first dataset.     Defaults to None.
            tprate_dataset_2 (xarray, optional):    The second dataset.    Defaults to None.

        Returns:
            xarray:     The xarray.Dataset with the merged data.
        """

        if isinstance(tprate_dataset_1, xr.Dataset) and isinstance(tprate_dataset_2, xr.Dataset):
            dataset_3 = tprate_dataset_1.copy(deep=True)
            dataset_3.attrs = {**tprate_dataset_1.attrs,
                               **tprate_dataset_2.attrs}

            for attribute in tprate_dataset_1.attrs:
                try:
                    if tprate_dataset_1.attrs[attribute] != tprate_dataset_2.attrs[attribute] and attribute not in 'time_band':
                        dataset_3.attrs[attribute] = str(
                            tprate_dataset_1.attrs[attribute])+';\n '+str(tprate_dataset_2.attrs[attribute])
                    elif attribute in 'time_band':
                        dataset_3.attrs['time_band_history'] = str(
                            tprate_dataset_1.attrs['time_band']) + ';\n '+str(tprate_dataset_2.attrs['time_band'])
                        if tprate_dataset_1.attrs['time_band'].count(':') <= 2 and tprate_dataset_2.attrs['time_band'].count(':') <= 2:
                            if np.datetime64(tprate_dataset_2.time_band) == np.datetime64(tprate_dataset_1.time_band):
                                dataset_3.attrs['time_band'] = str(
                                    tprate_dataset_1.attrs['time_band'])
                            else:
                                if np.datetime64(tprate_dataset_2.time_band) > np.datetime64(tprate_dataset_1.time_band):
                                    timedelta = np.datetime64(
                                        tprate_dataset_2.time_band) - np.datetime64(tprate_dataset_1.time_band)
                                    tprate_dataset_1_sm = tprate_dataset_1
                                    tprate_dataset_2_bg = tprate_dataset_2
                                elif np.datetime64(tprate_dataset_2.time_band) < np.datetime64(tprate_dataset_1.time_band):
                                    timedelta = np.datetime64(
                                        tprate_dataset_1.time_band) - np.datetime64(tprate_dataset_2.time_band)
                                    tprate_dataset_1_sm = tprate_dataset_2
                                    tprate_dataset_2_bg = tprate_dataset_1

                                days = timedelta / np.timedelta64(1, 'D')
                                if days < 1:
                                    hrs = timedelta / np.timedelta64(1, 'h')
                                    dataset_3.attrs['time_band'] = str(
                                        tprate_dataset_1_sm.attrs['time_band'])+', '+str(tprate_dataset_2_bg.attrs['time_band']) + ', freq='+str(timedelta / np.timedelta64(1, 'h'))+'H'
                                elif days == 1:
                                    dataset_3.attrs['time_band'] = str(
                                        tprate_dataset_1_sm.attrs['time_band'])+', '+str(tprate_dataset_2_bg.attrs['time_band']) + ', freq='+'1H'
                                elif days < 32 and days > 27:
                                    dataset_3.attrs['time_band'] = str(
                                        tprate_dataset_1_sm.attrs['time_band'])+', '+str(tprate_dataset_2_bg.attrs['time_band']) + ', freq='+'1M'
                                elif days < 367 and days > 364:
                                    dataset_3.attrs['time_band'] = str(
                                        tprate_dataset_1_sm.attrs['time_band'])+', '+str(tprate_dataset_2_bg.attrs['time_band']) + ', freq='+'1Y'
                                else:
                                    dataset_3.attrs['time_band'] = str(
                                        tprate_dataset_1_sm.attrs['time_band'])+', '+str(tprate_dataset_2_bg.attrs['time_band']) + ', freq='+str(days)+'D'
                        else:
                            if tprate_dataset_1.time_band.split(',')[2] == tprate_dataset_2.time_band.split(',')[2]:
                                if np.datetime64(tprate_dataset_1.time_band.split(',')[0]) < np.datetime64(tprate_dataset_2.time_band.split(',')[0]):
                                    if np.datetime64(tprate_dataset_1.time_band.split(',')[1]) < np.datetime64(tprate_dataset_2.time_band.split(',')[1]):
                                        dataset_3.attrs['time_band'] = tprate_dataset_1.time_band.split(
                                            ',')[0] + ','+tprate_dataset_2.time_band.split(',')[1]+','+tprate_dataset_2.time_band.split(',')[2]
                                    else:
                                        dataset_3.attrs['time_band'] = tprate_dataset_1.time_band.split(
                                            ',')[0] + ','+tprate_dataset_1.time_band.split(',')[1]+','+tprate_dataset_2.time_band.split(',')[2]
                                else:
                                    if np.datetime64(tprate_dataset_1.time_band.split(',')[1]) < np.datetime64(tprate_dataset_2.time_band.split(',')[1]):
                                        dataset_3.attrs['time_band'] = tprate_dataset_2.time_band.split(
                                            ',')[0] + ','+tprate_dataset_2.time_band.split(',')[1]+','+tprate_dataset_2.time_band.split(',')[2]
                                    else:
                                        dataset_3.attrs['time_band'] = tprate_dataset_2.time_band.split(
                                            ',')[0] + ','+tprate_dataset_1.time_band.split(',')[1]+','+tprate_dataset_2.time_band.split(',')[2]

                except ValueError:
                    if tprate_dataset_1.attrs[attribute].all != tprate_dataset_2.attrs[attribute].all:
                        dataset_3.attrs[attribute] = str(
                            tprate_dataset_1.attrs[attribute])+';\n '+str(tprate_dataset_2.attrs[attribute])

            dataset_3.counts.values = tprate_dataset_1.counts.values + \
                tprate_dataset_2.counts.values
            dataset_3.counts.attrs['size_of_the_data'] = tprate_dataset_1.counts.size_of_the_data + \
                tprate_dataset_2.counts.size_of_the_data
            dataset_3.frequency.values = self.convert_counts_to_frequency(
                dataset_3.counts,  test=test)
            dataset_3.pdf.values = self.convert_counts_to_pdf(
                dataset_3.counts,  test=test)

            for variable in ('counts', 'frequency', 'pdf'):
                for attribute in tprate_dataset_1.counts.attrs:
                    dataset_3[variable].attrs = {
                        **tprate_dataset_1[variable].attrs, **tprate_dataset_2[variable].attrs}
                    try:
                        if tprate_dataset_1[variable].attrs[attribute] != tprate_dataset_2[variable].attrs[attribute]:
                            dataset_3[variable].attrs[attribute] = str(
                                tprate_dataset_1[variable].attrs[attribute])+';\n ' + str(tprate_dataset_2[variable].attrs[attribute])
                    except ValueError:
                        if tprate_dataset_1[variable].attrs[attribute].all != tprate_dataset_2[variable].attrs[attribute].all:
                            dataset_3[variable].attrs[attribute] = str(
                                tprate_dataset_1[variable].attrs[attribute])+';\n ' + str(tprate_dataset_2[variable].attrs[attribute])
                dataset_3[variable].attrs['size_of_the_data'] = tprate_dataset_1[variable].size_of_the_data + \
                    tprate_dataset_2[variable].size_of_the_data
            return dataset_3

    def merge_list_of_histograms(self, path_to_histograms=None, multi=None, seasons=False, all=False, test=False, tqdm=True):
        """ Function to merge list of histograms.

        Args:
            path_to_histograms (str, optional):     The path to the list of histograms.     Defaults to None.
            multi (int, optional):                  The number of histograms to merge.      Defaults to None.
            all (bool, optional):                   If True, all histograms in the repository will be merged. Defaults to False.

        Returns:
            xarray: The xarray.Dataset with the merged data.
        """

        histogram_list = [f for f in listdir(
            path_to_histograms) if isfile(join(path_to_histograms, f))]
        histogram_list.sort()

        if seasons:
            histograms_to_load = [str(path_to_histograms) + str(histogram_list[i])
                                  for i in range(0, len(histogram_list))]

            DJF = []
            MAM = []
            JJA = []
            SON = []

            progress_bar_template = "[{:<40}] {}%"
            for i in range(0, len(histogram_list)):
                if tqdm:
                    ratio = i / len(histogram_list)
                    progress = int(40 * ratio)
                    print(progress_bar_template.format(
                        "=" * progress, int(ratio * 100)), end="\r")

                name_of_file = histogram_list[i]
                re.split(r"[^0-9\s]", name_of_file)
                splitted_name = list(
                    filter(None, re.split(r"[^0-9\s]", name_of_file)))
                syear, fyear = int(splitted_name[-8]), int(splitted_name[-4])
                smonth, fmonth = int(splitted_name[-7]), int(splitted_name[-3])

                if syear == fyear:
                    if fmonth - smonth == 1:
                        if smonth in [12, 1, 2]:
                            DJF.append(histograms_to_load[i])
                        elif smonth in [3, 4, 5]:
                            MAM.append(histograms_to_load[i])
                        elif smonth in [6, 7, 8]:
                            JJA.append(histograms_to_load[i])
                        elif smonth in [9, 10, 11]:
                            SON.append(histograms_to_load[i])
            four_seasons = []
            for hist_seasonal in [DJF, MAM, JJA, SON]:

                if len(hist_seasonal) > 0:
                    for i in range(0, len(hist_seasonal)):
                        if i == 0:
                            dataset = self.open_dataset(
                                path_to_netcdf=hist_seasonal[i])
                        else:
                            dataset = self.merge_two_datasets(tprate_dataset_1=dataset,
                                                              tprate_dataset_2=self.open_dataset(path_to_netcdf=hist_seasonal[i]), test=test)
                    four_seasons.append(dataset)
            self.logger.info("Histograms are merged for each season.")
            return four_seasons
        else:
            if all:
                histograms_to_load = [str(
                    path_to_histograms) + str(histogram_list[i]) for i in range(0, len(histogram_list))]
            elif multi is not None:
                histograms_to_load = [
                    str(path_to_histograms) + str(histogram_list[i]) for i in range(0, multi)]
            if len(histograms_to_load) > 0:
                for i in range(0, len(histograms_to_load)):
                    if i == 0:
                        dataset = self.open_dataset(
                            path_to_netcdf=histograms_to_load[i])
                    else:
                        dataset = self.merge_two_datasets(tprate_dataset_1=dataset,
                                                          tprate_dataset_2=self.open_dataset(path_to_netcdf=histograms_to_load[i]), test=test)
                self.logger.info("Histograms are merged.")
                return dataset
            else:
                raise NameError('The specified repository is empty.')

    def convert_counts_to_frequency(self, data, test=False):
        """ Function to convert the counts to the frequency.

        Args:
            data (xarray): The counts.

        Returns:
            xarray: The frequency.
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

    def convert_counts_to_pdf(self, data, test=False):
        """ Function to convert the counts to the pdf.

        Args:
            data (xarray): The counts.

        Returns:
            xarray: The pdf.
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

    def convert_counts_to_pdfP(self, data, test=False):
        """ Function to convert the counts to the pdf multiplied by center of bin.

        Args:
            data (xarray): The counts.

        Returns:
            xarray: The pdfP.
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

    def mean_from_histogram(self, hist, data=None, old_unit='kg m**-2 s**-1', new_unit=None,
                            model_variable='tprate', trop_lat=None, positive=True):
        """ Function to calculate the mean from the histogram.

        Args:
            hist (xarray): The histogram.
            data (xarray): The data.
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
        self.class_attributes_update(trop_lat=trop_lat)

        if data is not None:
            try:
                data = data[model_variable]
            except KeyError:
                pass
            try:
                mean_of_original_data = data.sel(
                    lat=slice(-self.trop_lat, self.trop_lat)).mean().values
            except KeyError:
                mean_of_original_data = data.mean().values
            if positive:
                _data = np.maximum(data, 0.)
                try:
                    mean_of_modified_data = _data.sel(
                        lat=slice(-self.trop_lat, self.trop_lat)).mean().values
                except KeyError:
                    mean_of_modified_data = _data.mean().values
        else:
            mean_of_original_data, mean_of_modified_data = None, None

        mean_from_freq = (hist.frequency*hist.center_of_bin).sum().values

        if new_unit is not None:
            try:
                mean_from_freq = self.precipitation_rate_units_converter(
                    mean_from_freq, old_unit=hist.counts.units, new_unit=new_unit)
            except AttributeError:
                mean_from_freq = self.precipitation_rate_units_converter(
                    mean_from_freq, old_unit=old_unit, new_unit=new_unit)
            if data is not None:
                mean_of_original_data = self.precipitation_rate_units_converter(
                    mean_of_original_data, old_unit=data.units, new_unit=new_unit)
                mean_of_modified_data = self.precipitation_rate_units_converter(
                    mean_of_modified_data, old_unit=data.units, new_unit=new_unit)

        return mean_from_freq, mean_of_original_data, mean_of_modified_data

    def histogram_plot(self, data,        new_unit=None,        pdfP=False,
                       positive=True, 
                       weights=None,      frequency=False,      pdf=True,
                       smooth=True,       step=False,           color_map=False,
                       ls='-',            ylogscale=True,       xlogscale=False,
                       color='tab:blue',  figsize=1,            legend='_Hidden',
                       plot_title=None,   loc='upper right',    varname='Precipitation',
                       add=None,          fig=None,             path_to_pdf=None,
                       name_of_file=None, pdf_format=True,      xmax=None,  test=False,
                       linewidth=3,     fontsize=14):
        """ Function to generate a histogram figure based on the provided data.

        Args:
            data:                           The data for the histogram.
            weights (optional):             An array of weights for the data.       Default is None.
            frequency (bool, optional):     Whether to plot frequency.              Default is False.
            pdf (bool, optional):           Whether to plot the probability density function (PDF). Default is True.
            smooth (bool, optional):        Whether to plot a smooth line.          Default is True.
            step (bool, optional):          Whether to plot a step line.            Default is False.
            color_map (bool or str, optional): Whether to apply a color map to the histogram bars.
                If True, uses the 'viridis' color map. If a string, uses the specified color map. Default is False.
            ls (str, optional):             The line style for the plot.            Default is '-'.
            ylogscale (bool, optional):     Whether to use a logarithmic scale for the y-axis. Default is True.
            xlogscale (bool, optional):     Whether to use a logarithmic scale for the x-axis. Default is False.
            color (str, optional):          The color of the plot.                  Default is 'tab:blue'.
            figsize (float, optional):      The size of the figure.                 Default is 1.
            legend (str, optional):         The legend label for the plot.          Default is '_Hidden'.
            varname (str, optional):        The name of the variable for the x-axis label. Default is 'Precipitation'.
            plot_title (str, optional):     The title of the plot.                  Default is None.
            loc(str, optional):             The location of the legend.             Default to 'upper right'.
            add (tuple, optional):          Tuple of (fig, ax) to add the plot to an existing figure.
            fig (object, optional):         The figure object to plot on. If provided, ignores the 'add' argument.
            path_to_pdf (str, optional): The path to save the figure. If provided, saves the figure at the specified path.


        Returns:
            A tuple (fig, ax) containing the figure and axes objects.
        """

        if 'Dataset' in str(type(data)):
            data = data['counts']
        if not pdf and not frequency and not pdfP:
            pass
        elif pdf and not frequency and not pdfP:
            data = self.convert_counts_to_pdf(data,  test=test)
        elif not pdf and frequency and not pdfP:
            data = self.convert_counts_to_frequency(data,  test=test)
        elif pdfP:
            data = self.convert_counts_to_pdfP(data,  test=test)

        x = data.center_of_bin.values
        
        if new_unit is None:
            xlabel=varname+", ["+str(data.attrs['units'])+"]"
        else:
            xlabel=varname+", ["+str(new_unit)+"]"

        if pdf and not frequency and not pdfP:
            ylabel = 'PDF'
        elif not pdf and frequency and not pdfP:
            ylabel = 'Frequency'
        elif not frequency and not pdfP and not pdf:
            ylabel = 'Counts'
        elif pdfP:
            ylabel = 'PDF * P'

        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_histogram.pdf'
        
        return self.plots.histogram_plot(x=x, data=data, positive=positive, xlabel=xlabel, ylabel=ylabel, weights=weights, smooth=smooth, 
               step=step, color_map=color_map, ls=ls, ylogscale=ylogscale, xlogscale=xlogscale, color=color, 
               figsize=figsize, legend=legend, plot_title=plot_title, loc=loc, add=add, fig=fig, path_to_pdf=path_to_pdf, 
               pdf_format=pdf_format, xmax=xmax, linewidth=linewidth, fontsize=fontsize)

    def mean_along_coordinate(self, data,       model_variable='tprate',      preprocess=True,
                              trop_lat=None,    coord='time',                 glob=False,
                              s_time=None,      f_time=None,                  positive=True,
                              s_year=None,      f_year=None,                  new_unit=None,
                              s_month=None,     f_month=None):
        """ Function to calculate the mean value of variable in Dataset.

        Args:
            data (xarray):                      The Dataset
            model_variable (str, optional):     The variable of the Dataset.            Defaults to 'tprate'.
            trop_lat (float, optional):         The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            coord (str, optional):              The coordinate of the Dataset.          Defaults to 'time'.
            s_time (str, optional):             The starting time of the Dataset.       Defaults to None.
            f_time (str, optional):             The ending time of the Dataset.         Defaults to None.
            s_year (str, optional):             The starting year of the Dataset.       Defaults to None.
            f_year (str, optional):             The ending year of the Dataset.         Defaults to None.
            s_month (str, optional):            The starting month of the Dataset.      Defaults to None.
            f_month (str, optional):            The ending month of the Dataset.        Defaults to None.
            glob (bool, optional):              If True, the median value is calculated for all lat and lon.  Defaults to False.
            preprocess (bool, optional):        If True, the Dataset is preprocessed.   Defaults to True.

        Returns:
            xarray:         The mean value of variable.
        """
        if preprocess:
            data = self.preprocessing(data,                              preprocess=preprocess,
                                      model_variable=model_variable,     trop_lat=self.trop_lat,
                                      s_time=self.s_time,                f_time=self.f_time,
                                      s_year=self.s_year,                f_year=self.f_year,
                                      s_month=self.s_month,              f_month=self.f_month,
                                      dask_array=False,                  new_unit=new_unit)
        if positive:
            data = np.maximum(data, 0.)
        coord_lat, coord_lon = self.coordinate_names(data)
        if coord in data.dims:

            self.class_attributes_update(trop_lat=trop_lat,
                                         s_time=s_time,          f_time=f_time,
                                         s_year=s_year,          f_year=f_year,
                                         s_month=s_month,        f_month=f_month)
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

    def median_along_coordinate(self, data,               trop_lat=None,     preprocess=True,
                                model_variable='tprate',  coord='time',      glob=False,
                                s_time=None,              f_time=None,       positive=True,
                                s_year=None,              f_year=None,       new_unit=None,
                                s_month=None,             f_month=None):
        """ Function to calculate the median value of variable in Dataset.

        Args:
            data (xarray):                      The Dataset
            model_variable (str, optional):     The variable of the Dataset.            Defaults to 'tprate'.
            trop_lat (float, optional):         The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            coord (str, optional):              The coordinate of the Dataset.          Defaults to 'time'.
            s_time (str, optional):             The starting time of the Dataset.       Defaults to None.
            f_time (str, optional):             The ending time of the Dataset.         Defaults to None.
            s_year (str, optional):             The starting year of the Dataset.       Defaults to None.
            f_year (str, optional):             The ending year of the Dataset.         Defaults to None.
            s_month (str, optional):            The starting month of the Dataset.      Defaults to None.
            f_month (str, optional):            The ending month of the Dataset.        Defaults to None.
            glob (bool, optional):              If True, the median value is calculated for all lat and lon.  Defaults to False.
            preprocess (bool, optional):        If True, the Dataset is preprocessed.   Defaults to True.

        Returns:
            xarray:         The median value of variable.
        """
        if preprocess:
            data = self.preprocessing(data,                              preprocess=preprocess,
                                      model_variable=model_variable,     trop_lat=self.trop_lat,
                                      s_time=self.s_time,                f_time=self.f_time,
                                      s_year=self.s_year,                f_year=self.f_year,
                                      s_month=self.s_month,              f_month=self.f_month,
                                      dask_array=False,                 new_unit=new_unit)

        if positive:
            data = np.maximum(data, 0.)
        coord_lat, coord_lon = self.coordinate_names(data)
        if coord in data.dims:
            self.class_attributes_update(trop_lat=trop_lat,
                                         s_time=s_time,         f_time=f_time,
                                         s_year=s_year,         f_year=f_year,
                                         s_month=s_month,       f_month=f_month)

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

    def average_into_netcdf(self, data,         glob=False,            preprocess=True,
                            model_variable='tprate',   coord='lat',
                            trop_lat=None,             get_mean=True,         get_median=False,
                            s_time=None,               f_time=None,           s_year=None,
                            f_year=None,               s_month=None,          f_month=None,
                            new_unit=None,             name_of_file=None,
                            seasons=True,              path_to_netcdf=None):
        """ Function to plot the mean or median value of variable in Dataset.

        Args:
            data (xarray):                  The Dataset
            model_variable (str, optional): The variable of the Dataset.            Defaults to 'tprate'.
            coord (str, optional):          The coordinate of the Dataset.          Defaults to 'time'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            get_mean (bool, optional):      The flag to calculate the mean of the variable.  Defaults to True.
            get_median (bool, optional):    The flag to calculate the median of the variable.  Defaults to False.
            s_time (str, optional):         The starting time of the Dataset.       Defaults to None.
            f_time (str, optional):         The ending time of the Dataset.         Defaults to None.
            s_year (str, optional):         The starting year of the Dataset.       Defaults to None.
            f_year (str, optional):         The ending year of the Dataset.         Defaults to None.
            s_month (str, optional):        The starting month of the Dataset.      Defaults to None.
            f_month (str, optional):        The ending month of the Dataset.        Defaults to None.
            varname (str, optional):        The name of the variable.               Defaults to 'Precipitation'.
            new_unit (str, optional):       The unit of the model variable.         Defaults to None.
            name_of_file (str, optional):   The name of the file.                   Defaults to None.
            seasons (bool, optional):       The flag to calculate the seasonal mean.  Defaults to True.
        Example:

        Returns:
            None.
        """
        self.class_attributes_update(trop_lat=trop_lat,
                                     s_time=s_time,         f_time=f_time,
                                     s_year=s_year,         f_year=f_year,
                                     s_month=s_month,       f_month=f_month)

        if preprocess:
            data_with_final_grid = self.preprocessing(data,                              preprocess=preprocess,
                                                      model_variable=model_variable,     trop_lat=self.trop_lat,
                                                      s_time=self.s_time,                f_time=self.f_time,
                                                      s_year=self.s_year,                f_year=self.f_year,
                                                      s_month=None,                      f_month=None,
                                                      dask_array=False,                  new_unit=new_unit)

        if get_mean:
            if seasons:
                data_average = self.seasonal_or_monthly_mean(data,                        preprocess=preprocess,
                                                             seasons=seasons,             model_variable=model_variable,
                                                             trop_lat=trop_lat,           new_unit=new_unit,
                                                             coord=coord)

                seasonal_average = data_average[0].to_dataset(name="DJF")
                seasonal_average["MAM"], seasonal_average["JJA"] = data_average[1], data_average[2]
                seasonal_average["SON"], seasonal_average["Yearly"] = data_average[3], data_average[4]
            else:
                data_average = self.mean_along_coordinate(data,                           preprocess=preprocess,
                                                          glob=glob,                      model_variable=model_variable,
                                                          trop_lat=trop_lat,              coord=coord,
                                                          s_time=self.s_time,             f_time=self.f_time,
                                                          s_year=self.s_year,             f_year=self.f_year,
                                                          s_month=self.s_month,           f_month=self.f_month)
        if get_median:
            data_average = self.median_along_coordinate(data,                           preprocess=preprocess,
                                                        glob=glob,                      model_variable=model_variable,
                                                        trop_lat=trop_lat,              coord=coord,
                                                        s_time=self.s_time,             f_time=self.f_time,
                                                        s_year=self.s_year,             f_year=self.f_year,
                                                        s_month=self.s_month,           f_month=self.f_month)

        s_month, f_month = None, None
        self.class_attributes_update(s_month=s_month,       f_month=f_month)
        if seasons:
            seasonal_average.attrs = data_with_final_grid.attrs
            seasonal_average = self.grid_attributes(
                data=data_with_final_grid, tprate_dataset=seasonal_average)
            for variable in ('DJF', 'MAM', 'JJA', 'SON', 'Yearly'):
                seasonal_average[variable].attrs = data_with_final_grid.attrs
                seasonal_average = self.grid_attributes(
                    data=data_with_final_grid, tprate_dataset=seasonal_average, variable=variable)
            average_dataset = seasonal_average
        else:
            data_average.attrs = data_with_final_grid.attrs
            data_average = self.grid_attributes(
                data=data_with_final_grid,      tprate_dataset=data_average)
            average_dataset = data_average

        if average_dataset.time_band == []:
            raise Exception('Time band is empty')
        
        if isinstance(path_to_netcdf, str) and name_of_file is not None:
            return self.dataset_to_netcdf(
                average_dataset, path_to_netcdf=path_to_netcdf, name_of_file=name_of_file+'_'+str(coord))
            #return path_to_netcdf+name_of_file+'_'+str(coord)
        else:
            return average_dataset

    def plot_of_average(self, data=None,
                        ymax=12,                    fontsize=15, pad=15,
                        trop_lat=None,             get_mean=True,         get_median=False,
                        legend='_Hidden',          figsize=1,             ls='-',
                        maxticknum=12,             color='tab:blue',      varname='tprate',
                        ylogscale=False,           xlogscale=False,       loc='upper right',
                        add=None,                  fig=None,              plot_title=None,
                        path_to_pdf=None,          new_unit='mm/day',     name_of_file=None,
                        pdf_format=True,       path_to_netcdf=None):
        """ Function to plot the mean or median value of variable in Dataset.

        Args:
            data (xarray):                  The Dataset
            model_variable (str, optional): The variable of the Dataset.            Defaults to 'tprate'.
            coord (str, optional):          The coordinate of the Dataset.          Defaults to 'time'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            get_mean (bool, optional):      The flag to calculate the mean of the variable.  Defaults to True.
            get_median (bool, optional):    The flag to calculate the median of the variable.  Defaults to False.
            s_time (str, optional):         The starting time of the Dataset.       Defaults to None.
            f_time (str, optional):         The ending time of the Dataset.         Defaults to None.
            s_year (str, optional):         The starting year of the Dataset.       Defaults to None.
            f_year (str, optional):         The ending year of the Dataset.         Defaults to None.
            s_month (str, optional):        The starting month of the Dataset.      Defaults to None.
            f_month (str, optional):        The ending month of the Dataset.        Defaults to None.
            legend (str, optional):         The legend of the plot.                 Defaults to '_Hidden'.
            figsize (int, optional):        The size of the plot.                   Defaults to 1.
            ls (str, optional):             The line style of the plot.             Defaults to '-'.
            maxticknum (int, optional):     The maximum number of ticks on the x-axis.  Defaults to 12.
            color (str, optional):          The color of the plot.                  Defaults to 'tab:blue'.
            varname (str, optional):        The name of the variable.               Defaults to 'Precipitation'.
            loc (str, optional):            The location of the legend.             Defaults to 'upper right'.
            add (matplotlib.figure.Figure, optional): The add previously created figure to plot.  Defaults to None.
            fig (matplotlib.figure.Figure, optional): The add previously created figure to plot.     Defaults to None.
            plot_title (str, optional):     The title of the plot.                  Defaults to None.
            path_to_pdf (str, optional):    The path to the pdf file.               Defaults to None.
            new_unit (str, optional):       The unit of the model variable.         Defaults to None.
            name_of_file (str, optional):   The name of the file.                   Defaults to None.
            seasons (bool, optional):       The flag to calculate the seasonal mean.  Defaults to True.
            pdf_format (bool, optional):    The flag to save the plot in pdf format. Defaults to True.
        Example:

        Returns:
            None.
        """
        self.class_attributes_update(trop_lat=trop_lat)
        
        if data is None and path_to_netcdf is not None:
            data = self.open_dataset(
                path_to_netcdf=path_to_netcdf)  
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

        if new_unit is not None and 'xarray' in str(type(data)):
            data = self.precipitation_rate_units_converter(
                data, new_unit=new_unit)
            units = new_unit
        else:
            units = data.units
        y_lim_max = self.precipitation_rate_units_converter(
                ymax, old_unit='mm/day', new_unit=new_unit)
        
        ylabel = str(varname)+', '+str(units)
        if plot_title is None:
            if get_mean:
                plot_title = 'Mean values of ' + str(varname)
            elif get_median:
                plot_title = 'Median values of '+str(varname)

        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_mean.pdf'

        return self.plots.plot_of_average(data=data, trop_lat=self.trop_lat, ylabel=ylabel, coord=coord, fontsize=fontsize, pad=pad, y_lim_max=y_lim_max,
                    legend=legend, figsize=figsize, ls=ls, maxticknum=maxticknum, color=color, ylogscale=ylogscale, 
                    xlogscale=xlogscale, loc=loc, add=add, fig=fig, plot_title=plot_title, path_to_pdf=path_to_pdf, 
                    pdf_format=pdf_format)


        

    def twin_data_and_observations(self, data,                     dummy_data=None,                trop_lat=None,
                                   s_time=None,                    f_time=None,                    s_year=None,
                                   f_year=None,                    s_month=None,                   f_month=None,
                                   model='era5',                   source='monthly',               plev=0,
                                   space_grid_factor=None,         time_freq=None,                 preprocess=True,
                                   time_length=None,               time_grid_factor=None,          model_variable='tprate',):
        """ Function to regride the data and observations to the same grid.

        Args:
            data (xarray):              Data to be regrided.
            dummy_data (xarray):        Dummy data to be regrided.                  Default to None.
            trop_lat (float):           Latitude band of the tropical region.       Default to None.
            model_variable (str, int):  Name of the variable to be regrided.        Default to 'tprate'.
            s_time (datetime):          Start time of the regrided data.            Default to None.
            f_time (datetime):          End time of the regrided data.              Default to None.
            s_year (int):               Start year of the regrided data.            Default to None.
            f_year (int):               End year of the regrided data.              Default to None.
            s_month (int):              Start month of the regrided data.           Default to None.
            f_month (int):              End month of the regrided data.             Default to None.
            model (str):                Model to be used.                           Default to 'era5'.
            source (str):               Source of the data.                         Default to 'monthly'.
            plev (int):                 Pressure level of the data.                 Default to 0.
            space_grid_factor (float):  Space grid factor.                          Default to None.
            time_freq (str):            Time frequency of the data.                 Default to None.
            preprocess (bool):          If True, the data is preprocessed.          Default to True.

        Returns:
        """

        self.class_attributes_update(trop_lat=trop_lat,
                                     s_time=s_time,                  f_time=f_time,
                                     s_year=s_year,                  f_year=f_year,
                                     s_month=s_month,                f_month=f_month)
        try:
            data = data[model_variable]
        except KeyError:
            pass
        new_unit = data.units
        if dummy_data is None:
            if model == 'era5':
                reader = Reader(model="ERA5", exp="era5", source=source)
                observations = reader.retrieve()
                observations = self.precipitation_rate_units_converter(observations.isel(plev=plev),
                                                                       model_variable=model_variable,
                                                                       new_unit=new_unit)

            elif model == 'mswep':
                reader = Reader(model="MSWEP", exp="past", source=source)
                observations = reader.retrieve()
                observations = self.precipitation_rate_units_converter(observations,
                                                                       model_variable=model_variable,
                                                                       new_unit=new_unit)
            dummy_data = observations

        else:
            dummy_data = self.precipitation_rate_units_converter(dummy_data,
                                                                 model_variable=model_variable,
                                                                 new_unit=new_unit)

        if preprocess:
            data = self.preprocessing(data,                                  preprocess=preprocess,
                                      model_variable=model_variable,         trop_lat=self.trop_lat,
                                      s_time=self.s_time,                    f_time=self.f_time,
                                      s_year=self.s_year,                    f_year=self.f_year,
                                      s_month=self.s_month,                  f_month=self.f_month,       dask_array=False)
            dummy_data = self.preprocessing(dummy_data,                             preprocess=preprocess,
                                            model_variable=model_variable,          trop_lat=self.trop_lat,
                                            s_time=self.s_time,                     f_time=self.f_time,
                                            s_year=self.s_year,                     f_year=self.f_year,
                                            s_month=self.s_month,                   f_month=self.f_month,    dask_array=False)

        data_regrided,  dummy_data_regrided = self.tools.mirror_dummy_grid(data=data,                                dummy_data=dummy_data,
                                                                space_grid_factor=space_grid_factor,      time_freq=time_freq,
                                                                time_length=time_length,                  time_grid_factor=time_grid_factor)
        return data_regrided, dummy_data_regrided

    def get_seasonal_or_monthly_data(self, data, preprocess=True, seasons=True,
                                    model_variable='tprate', trop_lat=None, new_unit=None):
        """ 
        Function to retrieve seasonal or monthly data.

        Args:
            data (xarray.DataArray): Data to be processed.
            preprocess (bool, optional): If True, the data will be preprocessed. Default is True.
            seasons (bool, optional): If True, the data will be calculated for the seasons. Default is True.
            model_variable (str, optional): Name of the model variable. Default is 'tprate'.
            trop_lat (float, optional): Latitude of the tropical region. Default is None.
            new_unit (str, optional): New unit of the data. Default is None.

        Returns:
            xarray.DataArray: Seasonal or monthly data.
        """
        self.class_attributes_update(trop_lat=trop_lat)
        if seasons:
            seasons = {
                'DJF_1': {'s_month': 12, 'f_month': 12},
                'DJF_2': {'s_month': 1, 'f_month': 2},
                'MAM': {'s_month': 3, 'f_month': 5},
                'JJA': {'s_month': 6, 'f_month': 8},
                'SON': {'s_month': 9, 'f_month': 11}
            }

            global_data = self.preprocessing(data, preprocess=preprocess, trop_lat=self.trop_lat, model_variable=model_variable, new_unit=new_unit)

            preprocessed_data = {}
            for key, value in seasons.items():
                preprocessed_data[key] = self.preprocessing(data, preprocess=preprocess, trop_lat=self.trop_lat,
                                                            model_variable=model_variable, s_month=value['s_month'],
                                                            f_month=value['f_month'])
                if new_unit is not None:
                    preprocessed_data[key] = self.precipitation_rate_units_converter(preprocessed_data[key], new_unit=new_unit)

            DJF_data = xr.concat([preprocessed_data['DJF_1'], preprocessed_data['DJF_2']], dim='time')
            all_seasonal_data = [DJF_data, preprocessed_data['MAM'], preprocessed_data['JJA'], preprocessed_data['SON'], global_data]
            
            return all_seasonal_data
        else:
            all_monthly_data = []
            for i in range(1, 13):
                if preprocess:
                    monthly_data = self.preprocessing(data, preprocess=preprocess, trop_lat=self.trop_lat, 
                                            model_variable=model_variable, s_month=i, f_month=i)
                    if new_unit is not None:
                        monthly_data = self.precipitation_rate_units_converter(monthly_data, new_unit=new_unit)
                all_monthly_data.append(monthly_data)
            return all_monthly_data

    def seasonal_or_monthly_mean(self,  data,                      preprocess=True,            seasons=True,
                                 model_variable='tprate',          trop_lat=None,              new_unit=None,
                                 coord=None, positive=True):
        """ Function to calculate the seasonal or monthly mean of the data.

        Args:
            data (xarray.DataArray):        Data to be calculated.
            preprocess (bool, optional):    If True, the data will be preprocessed.                 The default is True.
            seasons (bool, optional):       If True, the data will be calculated for the seasons.   The default is True.
            model_variable (str, optional): Name of the model variable.                             The default is 'tprate'.
            trop_lat (float, optional):     Latitude of the tropical region.                        The default is None.
            new_unit (str, optional):       New unit of the data.                                   The default is None.
            coord (str, optional):          Name of the coordinate.                                 The default is None.

        Returns:
            xarray.DataArray:             Seasonal or monthly mean of the data.

        """

        self.class_attributes_update(trop_lat=trop_lat)
        if seasons:
            [DJF, MAM, JJA, SON, glob] = self.get_seasonal_or_monthly_data(data,        preprocess=preprocess,        seasons=seasons,
                                                                           model_variable=model_variable,       trop_lat=trop_lat,          new_unit=new_unit)
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

            all_season = [DJF_mean, MAM_mean, JJA_mean, SON_mean, glob_mean]
            return all_season

        else:
            all_months = self.get_seasonal_or_monthly_data(data,        preprocess=preprocess,        seasons=seasons,
                                                           model_variable=model_variable,       trop_lat=trop_lat,          new_unit=new_unit)

            for i in range(1, 13):
                mon_mean = all_months[i].mean('time')
                all_months[i] = mon_mean
            return all_months

    def plot_bias(self,         data,         preprocess=True,                  seasons=True,
                  dataset_2=None,             model_variable='tprate',          figsize=1,
                  trop_lat=None,              plot_title=None,                  new_unit=None,
                  vmin=None,                  vmax=None,                        path_to_pdf=None,
                  name_of_file=None,          pdf_format=True):
        """ Function to plot the bias of model_variable between two datasets.

        Args:
            data (xarray): First dataset to be plotted
            dataset_2 (xarray, optional):   Second dataset to be plotted
            preprocess (bool, optional):    If True, data is preprocessed.              Defaults to True.
            seasons (bool, optional):       If True, data is plotted in seasons. If False, data is plotted in months. Defaults to True.
            model_variable (str, optional): Name of the model variable.                 Defaults to 'tprate'.
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

        self.plot_seasons_or_months(data,                         preprocess=preprocess,            seasons=seasons,
                                    dataset_2=dataset_2,          model_variable=model_variable,    figsize=figsize,
                                    trop_lat=trop_lat,            plot_title=plot_title,            new_unit=new_unit,
                                    vmin=vmin,                    vmax=vmax,                        path_to_pdf=path_to_pdf,
                                    name_of_file=name_of_file,    pdf_format=pdf_format)

    def plot_seasons_or_months(self,     data,             preprocess=True,                  seasons=True,
                               dataset_2=None,             model_variable='tprate',          figsize=1,
                               trop_lat=None,              plot_title=None,                  new_unit=None,
                               vmin=None,                  vmax=None,                        get_mean=True,
                               percent95_level=False,
                               path_to_pdf=None,           name_of_file=None,                pdf_format=True,
                               path_to_netcdf=None,
                               value=0.95,                           rel_error=0.1):
        """ Function to plot seasonal data.

        Args:
            data (xarray): First dataset to be plotted
            dataset_2 (xarray, optional):   Second dataset to be plotted
            preprocess (bool, optional):    If True, data is preprocessed.          Defaults to True.
            seasons (bool, optional):       If True, data is plotted in seasons. If False, data is plotted in months. Defaults to True.
            model_variable (str, optional): Name of the model variable.             Defaults to 'tprate'.
            figsize (float, optional):      Size of the figure.                     Defaults to 1.
            trop_lat (float, optional):     Latitude of the tropical region.        Defaults to None.
            plot_title (str, optional):     Title of the plot.                      Defaults to None.
            new_unit (str, optional):       Unit of the data.                       Defaults to None.
            vmin (float, optional):         Minimum value of the colorbar.          Defaults to None.
            vmax (float, optional):         Maximum value of the colorbar.          Defaults to None.
            contour (bool, optional):       If True, contours are plotted.          Defaults to True.
            path_to_pdf (str, optional):    Path to the pdf file.                   Defaults to None.
            name_of_file (str, optional):   Name of the pdf file.                   Defaults to None.
            pdf_format (bool, optional):    If True, the figure is saved in PDF format. Defaults to True.

        Returns:
            The pyplot figure in the PDF format
        """

        self.class_attributes_update(trop_lat=trop_lat)

        if seasons:
            all_months=None
            if isinstance(path_to_netcdf, str):
                data = self.open_dataset(
                    path_to_netcdf=path_to_netcdf)
            try:
                all_season = [data.DJF, data.MAM,
                              data.JJA, data.SON, data.Yearly]
            except AttributeError:
                if get_mean:
                    all_season = self.seasonal_or_monthly_mean(data,               preprocess=preprocess,        seasons=seasons,
                                                               model_variable=model_variable,    trop_lat=self.trop_lat,       new_unit=new_unit)
                elif percent95_level:
                    all_season = self.seasonal_095level_into_netcdf(data, reprocess=preprocess,        seasons=seasons, new_unit=new_unit,
                                                              model_variable=model_variable,          path_to_netcdf=path_to_netcdf,
                                                              name_of_file=name_of_file,                    trop_lat=trop_lat,
                                                              value=value,                           rel_error=rel_error)

            if dataset_2 is not None:
                all_season_2 = self.seasonal_or_monthly_mean(dataset_2,                     preprocess=preprocess,
                                                             seasons=seasons,            model_variable=model_variable,
                                                             trop_lat=self.trop_lat,      new_unit=new_unit)
                for i in range(0, len(all_season)):
                    all_season[i].values = all_season[i].values - \
                        all_season_2[i].values

        else:
            all_season = None
            all_months = self.seasonal_or_monthly_mean(data,                preprocess=preprocess,        seasons=seasons,
                                                       model_variable=model_variable,     trop_lat=trop_lat,            new_unit=new_unit)

            if dataset_2 is not None:
                all_months_2 = self.seasonal_or_monthly_mean(dataset_2,     preprocess=preprocess,         seasons=seasons,
                                                             model_variable=model_variable,     trop_lat=trop_lat,            new_unit=new_unit)
                for i in range(0, len(all_months)):
                    all_months[i].values = all_months[i].values - \
                        all_months_2[i].values
        if new_unit is None:
            try:
                unit = data[model_variable].units
            except KeyError:
                unit = data.units
        else:
            unit = new_unit
        cbarlabel = model_variable+", ["+str(unit)+"]"

        if isinstance(path_to_pdf, str) and name_of_file is not None:
            if seasons:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_seasons.pdf'
            else:
                path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_months.pdf'
        return self.plots.plot_seasons_or_months(data=data, cbarlabel=cbarlabel, all_season=all_season, all_months=all_months,
                          figsize=figsize, plot_title=plot_title,  vmin=vmin, vmax=vmax,
                          path_to_pdf=path_to_pdf, pdf_format=pdf_format)
                
    def savefig(self, path_to_pdf=None, pdf_format=True):
        """
        Save the current figure to a file in either PDF or PNG format.

        Args:
            path_to_pdf (str): The file path where the figure will be saved. If None, the figure will not be saved.
            pdf_format (bool): If True, the figure will be saved in PDF format; otherwise, it will be saved in PNG format.

        Returns:
            None

        Note:
            The function first checks the `path_to_pdf` to determine the format of the saved figure. If `pdf_format` is set to True, the figure will be saved in PDF format with the specified path. If `pdf_format` is False, the function replaces the '.pdf' extension in the `path_to_pdf` with '.png' and saves the figure in PNG format.

        Example:
            savefig(path_to_pdf='example.pdf', pdf_format=True)
            # This will save the current figure in PDF format as 'example.pdf'.

        """
        create_folder(folder=self.tools.extract_directory_path(
                    path_to_pdf), loglevel='WARNING')
        
        if pdf_format:
            plt.savefig(path_to_pdf, format="pdf", bbox_inches="tight", pad_inches=1, transparent=True,
                        facecolor="w", edgecolor='w', orientation='landscape')
        else:
            path_to_pdf = path_to_pdf.replace('.pdf', '.png')
            plt.savefig(path_to_pdf, bbox_inches="tight", pad_inches=1,
                        transparent=True, facecolor="w", edgecolor='w', orientation='landscape')

    def improve_time_selection(self, data=None, time_selection=None): 
        """
        Perform time selection based on the provided criteria.

        Args:
            data (xarray): The input data to be processed.
            time_selection (str): The time selection criteria.

        Returns:
            str: The updated time selection value.

        The function checks if the time selection criteria contains a year and a date in the format 'YYYY-MM-DD'. If the input string doesn't include a year or a date, the function appends the necessary values to the string. The processed time selection value is then returned.

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
                    self.logger.error(f'The dataset does not contain the input time value. Choose a different time value.')
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
                        self.logger.error(f'The dataset does not contain the input time value. Choose a different time value.')
                else:
                    try:
                        time_selection = time_selection + '-' + '01'
                        data.sel(time=time_selection)
                    except KeyError:
                        time_selection = time_selection + '-' + '25'
                    self.logger.debug(f'The input time value for selection does not contain a day. The new time value for selection is: {time_selection}')
        self.logger.info(f'The time value for selection is: {time_selection}')
        return time_selection
    
    def zoom_in_data(self, trop_lat=None,
                     pacific_ocean=False, atlantic_ocean=False, indian_ocean=False, tropical=False):
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
        self.class_attributes_update(trop_lat=trop_lat)

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

        if tropical:
            latmax = self.trop_lat
            latmin = -self.trop_lat
        self.logger.info(f'The data was zoomed in.')
        return lonmin, lonmax, latmin, latmax

    def map(self, data, titles=None, lonmin=-180, lonmax=181, latmin=-90, latmax=91, cmap=None,
            pacific_ocean=False, atlantic_ocean=False, indian_ocean=False, tropical=False,
            model_variable='tprate', figsize=None, number_of_axe_ticks=None, number_of_bar_ticks=None, fontsize=None,
            trop_lat=None, plot_title=None, new_unit="mm/day",
            vmin=None, vmax=None, time_selection='01',
            path_to_pdf=None, name_of_file=None, pdf_format=None):
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

        self.class_attributes_update(trop_lat=trop_lat)

        data = data if isinstance(data, list) else [data]
        if new_unit is None:
            try:
                unit = data[0][model_variable].units
            except KeyError:
                unit = data[0].units
        else:
            unit = new_unit       

        for i in range(0, len(data)):   
            if any((pacific_ocean, atlantic_ocean, indian_ocean, tropical)):
                lonmin, lonmax, latmin, latmax = self.zoom_in_data(trop_lat=self.trop_lat,
                        pacific_ocean=pacific_ocean, atlantic_ocean=atlantic_ocean, indian_ocean=indian_ocean, tropical=tropical)
                
            if lonmin != -180 or lonmax not in (180, 181):
                data[i] = data[i].sel(lon=slice(lonmin, lonmax))
            if latmin != -90 or latmax not in (90, 91):
                data[i] = data[i].sel(lat=slice(latmin-1, latmax))

            data[i] = data[i].where(data[i] > vmin)

            if data[i].time.size==1:
                pass
            else:
                time_selection = self.improve_time_selection(data[i], time_selection=time_selection)
                data[i] = data[i].sel(time=time_selection)
                if data[i].time.size!=1:
                    self.logger.error(f'The time selection went wrong. Please check the value of input time.')

            try:
                data[i] = data[i][model_variable]
            except KeyError:
                pass

            if new_unit is not None:
                data[i] = self.precipitation_rate_units_converter(data[i], model_variable=model_variable, new_unit=new_unit)
        
        cbarlabel=model_variable+", ["+str(unit)+"]"
        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_map.pdf'

        return self.plots.map(data=data, titles=titles, lonmin=lonmin, lonmax=lonmax, latmin=latmin, latmax=latmax, cmap=cmap, fontsize=fontsize,
                   model_variable=model_variable, figsize=figsize, number_of_axe_ticks=number_of_axe_ticks, number_of_bar_ticks=number_of_bar_ticks, cbarlabel=cbarlabel,
                   plot_title=plot_title, vmin=vmin, vmax=vmax, path_to_pdf=path_to_pdf, pdf_format=pdf_format)

    def get_95percent_level(self, data=None, original_hist=None, value=0.95, preprocess=True, rel_error=0.1, model_variable='tprate',
                            new_unit=None, weights=None,  trop_lat=None):
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

        if new_unit is not None:
            data = self.precipitation_rate_units_converter(
                data, new_unit=new_unit)
            units = new_unit

        value = 1 - value
        rel_error = value*rel_error
        if original_hist is None:

            self.class_attributes_update(trop_lat=trop_lat)

            original_hist = self.histogram(data,         weights=weights,       preprocess=preprocess,
                                           trop_lat=self.trop_lat,              model_variable=model_variable,
                                           num_of_bins=self.num_of_bins,   first_edge=self.first_edge,      width_of_bin=self.width_of_bin,       bins=self.bins)

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
            units = data[model_variable].units
        except KeyError:
            units = data.units

        bin_value = bin_i + del_bin

        

        return bin_value, units, 1 - threshold

    def seasonal_095level_into_netcdf(self,     data,           preprocess=True,        seasons=True,
                                      model_variable='tprate',              path_to_netcdf=None,
                                      name_of_file=None,                    trop_lat=None,
                                      value=0.95,                           rel_error=0.1, new_unit=None,
                                      lon_length=None,                        lat_length=None,
                                      space_grid_factor=None,                 tqdm=True):
        """ Function to plot.
        Args:"""

        data = self.tools.space_regrider(data, space_grid_factor=space_grid_factor,
                              lat_length=lat_length, lon_length=lon_length)

        self.class_attributes_update(trop_lat=trop_lat)
        if seasons:
            [DJF, MAM, JJA, SON, glob] = self.get_seasonal_or_monthly_data(data,        preprocess=preprocess,        seasons=seasons,
                                                                           model_variable=model_variable,       trop_lat=trop_lat,          new_unit=new_unit)

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
                    bin_value, units, threshold = self.get_95percent_level(DJF.isel(lat=lat_i).isel(lon=lon_i), preprocess=False,
                                                                           value=value, rel_error=rel_error)
                    DJF_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    MAM_095level = MAM.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(MAM.isel(lat=lat_i).isel(lon=lon_i), preprocess=False,
                                                                           value=value, rel_error=rel_error)
                    MAM_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    JJA_095level = JJA.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(JJA.isel(lat=lat_i).isel(lon=lon_i), preprocess=False,
                                                                           value=value, rel_error=rel_error)
                    JJA_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    SON_095level = SON.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(SON.isel(lat=lat_i).isel(lon=lon_i), preprocess=False,
                                                                           value=value, rel_error=rel_error)
                    SON_095level.isel(lat=lat_i).isel(
                        lon=lon_i).values = bin_value

                    self.class_attributes_update(s_month=s_month, f_month=f_month, num_of_bins=num_of_bins,
                                                 first_edge=first_edge, width_of_bin=width_of_bin, bins=bins)
                    glob_095level = glob.isel(time=0).copy(deep=True)
                    bin_value, units, threshold = self.get_95percent_level(glob.isel(lat=lat_i).isel(lon=lon_i), preprocess=False,
                                                                           value=value, rel_error=rel_error)
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
                data=SON, tprate_dataset=seasonal_095level)
            for variable in ('DJF', 'MAM', 'JJA', 'SON', 'Yearly'):
                seasonal_095level[variable].attrs = SON.attrs
                seasonal_095level = self.grid_attributes(
                    data=SON, tprate_dataset=seasonal_095level, variable=variable)

        if seasonal_095level.time_band == []:
            raise Exception('Time band is empty')
        if isinstance(path_to_netcdf, str) and name_of_file is not None:
            self.dataset_to_netcdf(
                seasonal_095level, path_to_netcdf=path_to_netcdf, name_of_file=name_of_file)
        else:
            return seasonal_095level

    def _utc_to_local(self, utc_time, longitude):
        """
        Convert a UTC time to local time based on the longitude provided.

        The function calculates the time zone offset based on the longitude, where each 15 degrees of longitude corresponds to 1 hour of time difference. It then applies the time zone offset to convert the UTC time to local time.

        Args:
            utc_time (int): The UTC time to convert to local time.
            longitude (float): The longitude value to calculate the time zone offset.

        Returns:
            int: The local time after converting the UTC time based on the provided longitude.

        """
        # Calculate the time zone offset based on longitude
        # Each 15 degrees of longitude corresponds to 1 hour of time difference
        time_zone_offset_hours = int(longitude / 15)

        # Apply the time zone offset to convert UTC time to local time
        local_time = (utc_time + time_zone_offset_hours) % 24

        return local_time

    def add_UTC_DataAaray(self, data, model_variable='tprate', space_grid_factor=None, time_length=None,
                        trop_lat=None, new_unit='mm/day', path_to_netcdf=None, name_of_file=None, tqdm=True):
        """
        Add a new dataset with UTC time based on the provided data.

        The function processes the data by selecting specific dimensions, calculating means, and applying space regridding. It then computes the local time for each longitude value and adds it to the dataset as UTC time. It also converts the data to a new unit if specified and saves the dataset to a NetCDF file.

        Args:
            data: The input data to be processed.
            model_variable (str): The variable from the model to be used in the process.
            space_grid_factor (int): The factor for space regridding.
            time_length (int): The length of the time dimension to be selected.
            trop_lat (float): The tropical latitude value to be used.
            new_unit (str): The new unit to which the data should be converted.
            path_to_netcdf (str): The path to the NetCDF file to be saved.
            name_of_file (str): The name of the file to be saved.
            tqdm (bool): A flag indicating whether to display the progress bar.

        Returns:
            xr.Dataset: The new dataset with added UTC time.

        """
        self.class_attributes_update(trop_lat=trop_lat)
        try:
            data = data[model_variable]
        except KeyError:
            pass

        utc_data = []
        progress_bar_template = "[{:<40}] {}%"
        if time_length is not None:
            data = data.isel(time=slice(0, time_length))
            self.logger.debug('Time selected')

        _data = data.sel(lat=slice(-self.trop_lat, self.trop_lat))
        data = _data.mean('lat')
        self.logger.debug('Latitude selected and mean calculated')
        self.logger.debug("Mean value: {}".format(data.mean()))
        if space_grid_factor is not None:
            data = self.tools.space_regrider(
                data, lon_length=space_grid_factor*data.lon.size)
            self.logger.debug('Space regrided')
        for time_ind in range(0, data.time.size):
            utc_data.append([])
            for lon_ind in range(0, data.lon.size):
                total_ind = time_ind*data.lon.size + lon_ind
                ratio = total_ind / (data.lon.size*data.time.size)
                progress = int(40 * ratio)
                print(progress_bar_template.format(
                    "=" * progress, int(ratio * 100)), end="\r")

                local_time = data.time[time_ind]
                longitude = data.lon[lon_ind].values - 180

                local_datetime = float(
                    local_time['time.hour'].values+local_time['time.minute'].values/60)

                utc_element = self._utc_to_local(
                    longitude=longitude, utc_time=local_datetime)
                utc_data[time_ind].append(utc_element)

        _dataset = data.to_dataset(name="tprate")
        _dataset.attrs = data.attrs
        _dataset.update({'utc_time': (['time', 'lon'], utc_data)})

        self.grid_attributes(data=_dataset, tprate_dataset=_dataset)

        data = _dataset.where(~np.isnan(_dataset.tprate), 0)

        utc_time = data['utc_time'].stack(total=['time', 'lon']).values
        tprate = data['tprate'].stack(total=['time', 'lon']).values

        if new_unit is not None and 'xarray' in str(type(tprate)):
            tprate = self.precipitation_rate_units_converter(
                tprate, new_unit=new_unit)
            units = new_unit
        elif new_unit is not None and 'ndarray' in str(type(tprate)):
            result_list = []
            for element in tprate:
                result_list.append(self.precipitation_rate_units_converter(
                    float(element), old_unit=data.units, new_unit=new_unit))
            tprate = np.array(result_list, dtype=np.float64)
        else:
            units = tprate.units

        new_data = []
        for i in range(0, len(utc_time)):
            new_data.append([utc_time[i], tprate[i]])

        # Sorted list with corresponding values
        sorted_list = sorted(new_data, key=lambda x: x[0])

        # Group elements by the first value in each element
        grouped_data = {key: [value for _, value in group]
                        for key, group in groupby(sorted_list, key=lambda x: x[0])}

        # Calculate the mean for each group and create the result list
        result = [[key, mean(values)] for key, values in grouped_data.items()]

        new_data = [result[i][1] for i in range(0, len(result))]
        new_coord = [result[i][0] for i in range(0, len(result))]

        da = xr.DataArray(new_data,
                          dims=('utc_time'),
                          coords={'utc_time': new_coord})

        new_dataset = da.to_dataset(name="tprate")
        new_dataset.attrs = _dataset.attrs

        mean_val = da.mean()

        da = [(new_data[i] - mean_val)/mean_val for i in range(0, len(new_data))]

        new_dataset.update({'tprate_relative': (['utc_time'], da)})

        if isinstance(path_to_netcdf, str) and name_of_file is not None:
            self.dataset_to_netcdf(
                new_dataset, path_to_netcdf=path_to_netcdf, name_of_file=name_of_file)
        else:
            return new_dataset
    
    def update_dict_of_loaded_analyses(self, loaded_dict=None):
        """
        Updates a dictionary with loaded data and assigns colors to each entry.

        Args:
            loaded_dict (dict): Dictionary with paths to datasets.

        Returns:
            dict: Updated dictionary with loaded data and colors assigned.
        """
        if not isinstance(loaded_dict, dict):
            self.logger.error("The provided object must be a 'dict' type.") 
            return None

        for key, value in loaded_dict.items():
            if 'path' not in value:
                print(f"Error: 'path' key is missing in the entry with key {key}")

        # Select a seaborn palette
        palette = sns.color_palette("husl", len(loaded_dict))

        # Loop through the dictionary and assign colors
        for i, (key, value) in enumerate(loaded_dict.items()):
            loaded_dict[key]["data"] = self.open_dataset(path_to_netcdf=value["path"])
            loaded_dict[key]["color"] = palette[i]   

        return loaded_dict


    def daily_variability_plot(self, ymax=12, trop_lat=None, relative=True, get_median=False,
                            legend='_Hidden', figsize=1, ls='-', maxticknum=12, color='tab:blue',
                            varname='tprate', ylogscale=False, xlogscale=False, loc='upper right',
                            add=None, fig=None, plot_title=None, path_to_pdf=None, new_unit='mm/day',
                            name_of_file=None, pdf_format=True, path_to_netcdf=None):
        """
        Plot the daily variability of the dataset.

        This function generates a plot showing the daily variability of the provided dataset. It allows customization of various plot parameters such as color, scale, and legends.

        Args:
            ymax (int): The maximum y-value for the plot.
            trop_lat (float): The tropical latitude value to be used.
            relative (bool): A flag indicating whether the plot should be relative.
            get_median (bool): A flag indicating whether to calculate the median.
            legend (str): The legend for the plot.
            figsize (int): The size of the figure.
            ls (str): The linestyle for the plot.
            maxticknum (int): The maximum number of ticks for the plot.
            color (str): The color of the plot.
            varname (str): The variable name to be used.
            ylogscale (bool): A flag indicating whether to use a log scale for the y-axis.
            xlogscale (bool): A flag indicating whether to use a log scale for the x-axis.
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

        self.class_attributes_update(trop_lat=trop_lat)
        if path_to_netcdf is None:
            raise Exception('The path needs to be provided')
        else:
            data = self.open_dataset(
                path_to_netcdf=path_to_netcdf)

        utc_time = data['utc_time']
        if relative:
            tprate = data['tprate_relative']
        else:
            tprate = data['tprate']
        try:
            units = data.units
        except AttributeError:
            try:
                units = data.tprate.units
            except AttributeError:
                units = 'mm/day'  # 'kg m**-2 s**-1'

        if 'Dataset' in str(type(data)):
            y_lim_max = self.precipitation_rate_units_converter(
                ymax, old_unit='mm/day', new_unit=new_unit)
            if fig is not None:
                fig, ax = fig
            elif add is None and fig is None:
                fig, ax = plt.subplots(
                    figsize=(11*figsize, 10*figsize), layout='constrained')
            elif add is not None:
                fig, ax = add
        ax.plot(utc_time, tprate,
                color=color,  label=legend,  ls=ls)

        if relative:
            ax.set_title(
                'Relative Value of Daily Precipitation Variability', fontsize=15)
            ax.set_xlabel('tprate variability, '+units,  fontsize=12)
        else:
            ax.set_title('Daily Precipitation Variability', fontsize=15)
            ax.set_xlabel('relative tprate',  fontsize=12)

        ax.set_frame_on(True)
        ax.grid(True)

        ax.set_xlabel('Local time', fontsize=12)

        if legend != '_Hidden':
            plt.legend(loc=loc,
                       fontsize=12,    ncol=2)

        if isinstance(path_to_pdf, str) and name_of_file is not None:
            path_to_pdf = path_to_pdf + 'trop_rainfall_' + name_of_file + '_dailyvar.pdf'
            self.savefig(path_to_pdf, pdf_format)

        return [fig,  ax]
