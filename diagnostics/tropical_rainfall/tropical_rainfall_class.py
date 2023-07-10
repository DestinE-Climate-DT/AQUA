from   datetime import datetime
import numpy as np
import xarray as xr
import re

from os import listdir
from os.path import isfile, join

import matplotlib.pyplot as plt
from   matplotlib import colors
import matplotlib.colors as colors
import matplotlib.cbook as cbook
from matplotlib import cm
from   matplotlib.colors import LogNorm
import boost_histogram as bh #pip

import dask.array as da
import dask_histogram as dh # pip
import dask_histogram.boost as dhb
import dask
import fast_histogram 

from aqua.util import create_folder

import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
from cartopy.util import add_cyclic_point

from aqua import Reader
from aqua.util import create_folder

from .tropical_rainfall_func import time_interpreter, convert_24hour_to_12hour_clock, convert_monthnumber_to_str
from .tropical_rainfall_func import mirror_dummy_grid, data_size
from .tropical_rainfall_func import convert_length, convert_time, unit_splitter, extract_directory_path

"""The module contains Tropical Precipitation Diagnostic:

.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""

class Tropical_Rainfall:
    """This class is a minimal version of the Tropical Precipitation Diagnostic.
    """


    def __init__(self,
            trop_lat = 10,
            s_time      = None,
            f_time      = None,
            s_year      = None,
            f_year      = None, 
            s_month     = None,
            f_month     = None,
            num_of_bins = None,
            first_edge  = None,
            width_of_bin= None,
            bins        = 0):
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

        self.trop_lat       = trop_lat
        self.s_time         = s_time
        self.f_time         = f_time
        self.s_year         = s_year
        self.f_year         = f_year
        self.s_month        = s_month
        self.f_month        = f_month     
        self.num_of_bins    = num_of_bins
        self.first_edge     = first_edge
        self.width_of_bin   = width_of_bin
        self.bins           = bins
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def class_attributes_update(self,       trop_lat = None,    s_time = None,       f_time = None,
                          s_year = None,    f_year = None,      s_month = None,      f_month = None,
                        num_of_bins = None, first_edge = None,  width_of_bin = None, bins = 0):
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
        if trop_lat is not None     and     isinstance(trop_lat, (int, float)):     
            self.trop_lat = trop_lat
        elif trop_lat is not None   and not isinstance(trop_lat, (int, float)):
            raise Exception("trop_lat must to be integer or float")
        
        if s_time is not None       and     isinstance(s_time, (int, str)):        
            self.s_time = s_time
        elif s_time is not None     and not isinstance(s_time, (int, str)):
            raise Exception("s_time must to be integer or string")
        
        if f_time is not None       and     isinstance(f_time, (int, str)):       
            self.f_time = f_time
        elif f_time is not None     and not isinstance(f_time, (int, str)):
            raise Exception("f_time must to be integer or string")
        
        if s_year is not None       and     isinstance(s_year, int):    
            self.s_year = s_year
        elif s_year is not None     and not isinstance(s_year, int):
            raise Exception("s_year must to be integer")
        
        if f_year is not None       and     isinstance(f_year, int):
            self.f_year = f_year
        elif f_year is not None     and not isinstance(f_year, int):
            raise Exception("f_year must to be integer")
        
        if s_month is not None      and     isinstance(s_month, int):   
            self.s_month = s_month
        elif s_month is not None    and not isinstance(s_month, int):
            raise Exception("s_month must to be integer")
        
        if f_month is not None      and     isinstance(f_month, int):     
            self.f_month = f_month
        elif f_month is not None    and not isinstance(f_month, int):
            raise Exception("f_month must to be integer")
        
        if bins!=0                  and     isinstance(bins, np.ndarray):
            self.bins = bins
        elif bins!=0                and not isinstance(bins, (np.ndarray, list)):
            raise Exception("bins must to be array")
        
        if num_of_bins is not None          and     isinstance(num_of_bins, int):
            self.num_of_bins = num_of_bins
        elif num_of_bins is not None        and not isinstance(num_of_bins, int):
            raise Exception("num_of_bins must to be integer")
        
        if first_edge is not None           and     isinstance(first_edge, (int, float)):
            self.first_edge = first_edge
        elif first_edge is not None         and not isinstance(first_edge, (int, float)):
            raise Exception("first_edge must to be integer or float")
        
        if width_of_bin is not None         and     isinstance(width_of_bin, (int, float)):
            self.width_of_bin = width_of_bin
        elif width_of_bin is not None       and not isinstance(width_of_bin, (int, float)):
            raise Exception("width_of_bin must to be integer or float")

        
        
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
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

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def precipitation_rate_units_converter(self, data, model_variable = 'tprate', old_unit = None,  new_unit = 'kg m**-2 s**-1'):
        """ Function to convert the units of precipitation rate.

        Args:
            data (xarray):                  The Dataset
            model_variable (str, optional): The name of the variable to be converted.   Defaults to 'tprate'.
            new_unit (str, optional):       The new unit of the variable.               Defaults to 'm s**-1'.

        Returns:
            xarray: The Dataset with converted units.
        """    
        if 'Dataset' in str(type(data)):
            data        = data[model_variable]
        else:
            data        = data

        if 'DataArray' in str(type(data)):   #isinstance(data, xarray.core.dataarray.DataArray):
            if data.units == new_unit:
                return data
      
        if isinstance(data, (float, int, np.ndarray)) and old_unit is not None:
            from_mass_unit, from_space_unit, from_time_unit     = unit_splitter(old_unit)
        else:
            from_mass_unit, from_space_unit, from_time_unit     = unit_splitter(data.units)
            old_unit  = data.units
        to_mass_unit,   to_space_unit,   to_time_unit           = unit_splitter(new_unit)

        
        if old_unit == 'kg m**-2 s**-1':
            data            = 0.001 * data
            data            = convert_length(data,   from_space_unit, to_space_unit)
            data            = convert_time(data,     from_time_unit,  to_time_unit)
        elif from_mass_unit is None and new_unit == 'kg m**-2 s**-1':
            data            = convert_length(data,   from_space_unit, 'm')
            data            = convert_time(data,     from_time_unit,  's')
            data            = 1000 * data
        else:
            data            = convert_length(data,   from_space_unit, to_space_unit)
            data            = convert_time(data,     from_time_unit,  to_time_unit)
        if 'xarray' in str(type(data)):
            data.attrs['units'] = new_unit
            current_time        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history_update      = str(current_time)+' the units of precipitation are converted from ' + str(data.units) + ' to ' + str(new_unit) + ';\n '
            try:
                history_attr                    = data.attrs['history'] + history_update
                data.attrs['history']           = history_attr
            except AttributeError or KeyError:
                data.attrs['history']           = history_update
        return data 
        
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def latitude_band(self, data, trop_lat = None):
        """ Function to select the Dataset for specified latitude range

        Args:
            data (xarray):                  The Dataset
            trop_lat (int/float, optional): The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.

        Returns:
            xarray: The Dataset only for selected latitude range. 
        """    
        
        
        self.class_attributes_update(trop_lat = trop_lat)
        
        coord_lat, coord_lon = self.coordinate_names(data)
        self.class_attributes_update(trop_lat = trop_lat)
        return data.where(abs(data[coord_lat]) <= self.trop_lat, drop = True)

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def time_band(self, data,
                  s_time = None,        f_time = None,
                  s_year = None,        f_year = None,
                  s_month = None,       f_month = None):
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

        if isinstance(self.s_time, int)     and isinstance(self.f_time, int):
            if self.s_time != None          and self.f_time != None:
                data = data.isel(time=slice(self.s_time, self.f_time))

        elif self.s_year != None            and self.f_year == None:
            data = data.where(data['time.year'] == self.s_year, drop = True)

        elif self.s_year != None            and self.f_year != None:
            data = data.where(data['time.year'] >= self.s_year, drop = True)
            data = data.where(data['time.year'] <= self.f_year, drop = True)

        if self.s_month != None             and self.f_month != None:
            data = data.where(data['time.month'] >= self.s_month, drop = True)
            data = data.where(data['time.month'] <= self.f_month, drop = True) 
        
        if isinstance(self.s_time, str)     and isinstance(self.f_time, str):
            if  s_time != None and f_time != None:
                _s = re.split(r"[^a-zA-Z0-9\s]", s_time)
                _f = re.split(r"[^a-zA-Z0-9\s]", f_time)
                if len(_s)==1:
                    s_time=_s[0]
                elif len(_f)==1:
                    f_time=_f[0]

                elif len(_s)==2:
                    s_time=_s[0]+'-'+_s[1]
                elif len(_f)==2:
                    f_time=_f[0]+'-'+_f[1]

                elif len(_s)==3:
                    s_time=_s[0]+'-'+  _s[1] +'-'+_s[2]
                elif len(_f)==3:
                    f_time=_f[0]+'-'+  _f[1] +'-'+ _f[2] 

                elif len(_s)==4:
                    s_time=_s[0]+'-'+  _s[1] +'-'+_s[2]+'-'+_s[3]
                elif len(_f)==4:
                    f_time=_f[0]+'-'+  _f[1] +'-'+ _f[2] +'-'+ _f[3]

                elif len(_s)==5:
                    s_time=_s[0] +'-'+  _s[1] +'-'+_s[2]+'-'+_s[3] +'-'+_s[4]
                elif len(_f)==5:
                    f_time=_f[0] +'-'+  _f[1] +'-'+ _f[2] +'-'+ _f[3] +'-'+ _f[4]  
                else:
                    raise Exception("Sorry, unknown format of time. Try one more time")  
            data=data.sel(time=slice(s_time, f_time))    
        elif self.s_time != None            and  self.f_time == None:
            if isinstance(s_year, str): 
                _temp = re.split(r"[^a-zA-Z0-9\s]", s_time)
                if len(_temp)==1:
                    time=_temp[0]
                elif len(_temp)==2:
                    time=_temp[0]+'-'+_temp[1]
                elif len(_temp)==3:
                    time=_temp[0]+'-'+_temp[1]+'-'+_temp[2]
                elif len(_temp)==3:
                    time=_temp[0]+'-'+_temp[1]+'-'+_temp[2]+'-'+_temp[3] 
                elif len(_temp)==4:
                    time=_temp[0]+'-'+_temp[1]+'-'+_temp[2]+'-'+_temp[3]+'-'+_temp[4]  
                elif len(_temp)==5:
                    time=_temp[0]+'-'+_temp[1]+'-'+_temp[2]+'-'+_temp[3]+'-'+_temp[4]+'-'+_temp[5]  
                else:
                    raise Exception("Sorry, unknown format of time. Try one more time")    
                data=data.sel(time=slice(time))
        return data

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def dataset_into_1d(self, data, model_variable = 'tprate', sort = False): 
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
            data        = data[model_variable]
        except KeyError:
            data        = data

        try:
            data_1d     = data.stack(total=['time', coord_lat, coord_lon])
        except KeyError:
            data_1d     = data.stack(total=[coord_lat, coord_lon])
        if sort == True:
            data_1d     = data_1d.sortby(data_1d)
        return data_1d

  
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def preprocessing(self, data,
                        preprocess = True,      model_variable = "tprate",      trop_lat = None,
                        s_time  = None,         f_time  = None,
                        s_year  = None,         f_year  = None,
                        s_month = None,         f_month = None,
                        sort = False,           dask_array = False):
        """ Function to preprocess the Dataset according to provided arguments and attributes of the class.

        Args:
            data (xarray):                  The Dataset.
            preprocess (bool, optional):    If sort is True, the functiom preprocess Dataset.   Defaults to True.
            model_variable (str, optional): The variable of the Dataset.                        Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset.           Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset.              Defaults to None.
            s_year (int, optional):         The starting year in Dataset.                       Defaults to None.
            f_year (int, optional):         The final year in Dataset.                          Defaults to None.
            s_month (int, optional):        The starting month in Dataset.                      Defaults to None.
            f_month (int, optional):        The final month in Dataset.                         Defaults to None.
            sort (bool, optional):          If sort is True, the DataArray is sorted.           Defaults to False.
            dask_array (bool, optional):    If sort is True, the function return daskarray.     Defaults to False.

        Returns:
            xarray: Preprocessed Dataset according to the arguments of the function
        """        
        
        self.class_attributes_update(trop_lat = trop_lat,
                                     s_time = s_time,       f_time = f_time,
                                     s_year = s_year,       f_year = f_year,        s_month = s_month,      f_month = f_month)
        if preprocess == True:
            if 'time' in data.coords:
                data_per_time_band  = self.time_band(data,
                                                    s_time  = self.s_time,       f_time  = self.f_time,
                                                    s_year  = self.s_year,       f_year  = self.f_year,
                                                    s_month = self.s_month,      f_month = self.f_month)
            else:
                data_per_time_band  = data
            
            try:
                data_variable       = data_per_time_band[model_variable]
            except KeyError:
                data_variable       = data_per_time_band

            data_per_lat_band       = self.latitude_band(data_variable, trop_lat = self.trop_lat)
            
            if dask_array == True:
                data_1d             = self.dataset_into_1d(data_per_lat_band)
                dask_data           = da.from_array(data_1d)
                return dask_data
            else:
                return data_per_lat_band
        else:
            return data
    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def histogram_lowres(self,          data,                   data_with_global_atributes = None,
                  weights = None,       preprocess = True,      trop_lat = 10,              model_variable = 'tprate',
                  s_time = None,        f_time = None,          s_year = None,              f_year = None,      s_month = None,     f_month = None,
                  num_of_bins = None,   first_edge = None,      width_of_bin  = None,       bins = 0,
                  lazy = False,         create_xarray = True,   path_to_histogram = None,   name_of_file=None):
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
        self.class_attributes_update(trop_lat = trop_lat,
                                     s_time  = s_time,           f_time  = f_time,
                                     s_year  = s_year,           f_year  = f_year,
                                     s_month = s_month,          f_month = f_month, 
                                     first_edge = first_edge,    num_of_bins = num_of_bins,      width_of_bin = width_of_bin)

        coord_lat, coord_lon = self.coordinate_names(data)

        if isinstance(self.bins, int):
            bins            = [self.first_edge  + i*self.width_of_bin for i in range(0, self.num_of_bins+1)]
            width_table     = [self.width_of_bin for j in range(0, self.num_of_bins)]
            center_of_bin   = [bins[i] + 0.5*width_table[i] for i in range(0, len(bins)-1)]
        else:
            bins            = self.bins
            width_table     = [self.bins[i+1]-self.bins[i] for i in range(0, len(self.bins)-1)]
            center_of_bin   = [self.bins[i] + 0.5*width_table[i] for i in range(0, len(self.bins)-1)]


        data_original=data
        if preprocess == True:
            data = self.preprocessing(data, preprocess = preprocess,
                                      model_variable   = model_variable,     trop_lat= self.trop_lat,
                                      s_time  = self.s_time,                 f_time  = self.f_time,
                                      s_year  = self.s_year,                 f_year  = self.f_year,           
                                      s_month = self.s_month,                f_month = self.f_month,
                                      sort = False,                          dask_array = False)
            
        data_with_final_grid=data

        if weights is not None:

            weights         = self.latitude_band(weights, trop_lat = self.trop_lat)
            data, weights   = xr.broadcast(data, weights)
            weights         = weights.stack(total=['time', coord_lat, coord_lon])
            weights_dask    = da.from_array(weights)
    

        data_dask           = da.from_array( data.stack(total=['time', coord_lat, coord_lon]))
        if weights is not None:
            counts, edges   = dh.histogram(data_dask, bins = bins,   weights = weights_dask,    storage = dh.storage.Weight())
        else:
            counts, edges   = dh.histogram(data_dask, bins = bins,   storage = dh.storage.Weight())
        if not lazy:
            counts          = counts.compute()
            edges           = edges.compute()

        width_table         = [edges[i+1]-edges[i] for i in range(0, len(edges)-1)]
        center_of_bin       = [edges[i] + 0.5*width_table[i] for i in range(0, len(edges)-1)]
        counts_per_bin      =  xr.DataArray(counts, coords=[center_of_bin], dims=["center_of_bin"])
        
        counts_per_bin      = counts_per_bin.assign_coords(width=("center_of_bin", width_table))
        counts_per_bin.attrs= data.attrs
        counts_per_bin.center_of_bin.attrs['units'] = data.units
        counts_per_bin.center_of_bin.attrs['history'] = 'Units are added to the bins to coordinate'
        
        if data_with_global_atributes is None:
            data_with_global_atributes = data_original

        if not lazy and create_xarray:
            tprate_dataset      = counts_per_bin.to_dataset(name="counts")
            tprate_dataset.attrs= data_with_global_atributes.attrs
            tprate_dataset      = self.add_frequency_and_pdf(tprate_dataset = tprate_dataset) #, path_to_histogram = path_to_histogram)
            
            
            for variable in (None, 'counts', 'frequency', 'pdf'):
                tprate_dataset  = self.grid_attributes(data = data_with_final_grid, tprate_dataset = tprate_dataset, variable = variable)

            if path_to_histogram is not None and name_of_file is not None:
                self.dataset_to_netcdf(tprate_dataset, path_to_netcdf = path_to_histogram, name_of_file = name_of_file)
            return tprate_dataset
        else:
            tprate_dataset      = counts_per_bin.to_dataset(name="counts")
            tprate_dataset.attrs= data_with_global_atributes.attrs
            counts_per_bin      = self.grid_attributes(data = data_with_final_grid, tprate_dataset = tprate_dataset, variable='counts')
            tprate_dataset      = self.grid_attributes(data = data_with_final_grid, tprate_dataset = tprate_dataset)
            if path_to_histogram is not None and name_of_file is not None:
                self.dataset_to_netcdf(tprate_dataset, path_to_netcdf = path_to_histogram, name_of_file = name_of_file)
            return counts_per_bin
        
    """ """ """ """ """ """ """ """ """ """
    def histogram(self,                 data,                   data_with_global_atributes = None,
                  weights = None,       preprocess = True,      trop_lat = 10,              model_variable = 'tprate',
                  s_time = None,        f_time = None,          s_year = None,              
                  f_year = None,        s_month = None,         f_month = None,
                  num_of_bins = None,   first_edge = None,      width_of_bin  = None,       bins = 0,
                  lazy = False,         create_xarray = True,   path_to_histogram = None,   name_of_file=None):
        """ Function to calculate a histogram of the high-resolution Dataset.

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
        self.class_attributes_update(trop_lat = trop_lat,
                                     s_time  = s_time,           f_time  = f_time,
                                     s_year  = s_year,           f_year  = f_year,
                                     s_month = s_month,          f_month = f_month, 
                                     first_edge = first_edge,    num_of_bins = num_of_bins,      width_of_bin = width_of_bin)
        data_original=data
        if preprocess == True:
            data = self.preprocessing(data, preprocess = preprocess,
                                      model_variable   = model_variable,     trop_lat= self.trop_lat,
                                      s_time  = self.s_time,                 f_time  = self.f_time,
                                      s_year  = self.s_year,                 f_year  = self.f_year,           
                                      s_month = self.s_month,                f_month = self.f_month,
                                      sort = False,                          dask_array = False)
        data_with_final_grid=data
        if isinstance(self.bins, int):
            bins            = [self.first_edge  + i*self.width_of_bin for i in range(0, self.num_of_bins+1)]
            width_table     = [self.width_of_bin for j in range(0, self.num_of_bins)]
            center_of_bin   = [bins[i] + 0.5*width_table[i] for i in range(0, len(bins)-1)]
        else:
            bins            = self.bins
            width_table     = [self.bins[i+1]-self.bins[i] for i in range(0, len(self.bins)-1)]
            center_of_bin   = [self.bins[i] + 0.5*width_table[i] for i in range(0, len(self.bins)-1)] 
        
        hist_fast   = fast_histogram.histogram1d(data, 
                                                   range=[self.first_edge, self.first_edge + (self.num_of_bins)*self.width_of_bin], 
                                                   bins = self.num_of_bins)
        
        counts_per_bin =  xr.DataArray(hist_fast, coords=[center_of_bin], dims=["center_of_bin"])
        counts_per_bin = counts_per_bin.assign_coords(width=("center_of_bin", width_table))
        counts_per_bin.attrs = data.attrs

        counts_per_bin.center_of_bin.attrs['units'] = data.units
        counts_per_bin.center_of_bin.attrs['history'] = 'Units are added to the bins to coordinate'
        
        if data_with_global_atributes is None:
            data_with_global_atributes = data_original

        tprate_dataset      = counts_per_bin.to_dataset(name="counts")
        tprate_dataset.attrs= data_with_global_atributes.attrs
        tprate_dataset      = self.add_frequency_and_pdf(tprate_dataset = tprate_dataset) 
        
        
        for variable in (None, 'counts', 'frequency', 'pdf'):
            tprate_dataset  = self.grid_attributes(data = data_with_final_grid, tprate_dataset = tprate_dataset, variable = variable)

        if path_to_histogram is not None and name_of_file is not None:
            self.dataset_to_netcdf(tprate_dataset, path_to_netcdf = path_to_histogram, name_of_file = name_of_file)
    
        return  tprate_dataset 
    
    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def dataset_to_netcdf(self, dataset = None, path_to_netcdf = None, name_of_file = None):
        """ Function to save the histogram.

        Args:
            dataset (xarray, optional):         The Dataset with the histogram.     Defaults to None.
            path_to_netcdf (str, optional):  The path to save the histogram.     Defaults to None.

        Returns:
            str: The path to save the histogram.
        """
        if path_to_netcdf is not None:
            create_folder(folder    = str(path_to_netcdf), loglevel = 'WARNING')
            if name_of_file is None:
                name_of_file    = '_'
            time_band           = dataset.counts.attrs['time_band']
            try:
                name_of_file    = name_of_file +'_'+ re.split(":", re.split(", ", time_band)[0])[0] +'_'+ re.split(":", re.split(", ", time_band)[1])[0]
            except IndexError:
                name_of_file    = name_of_file +'_'+ re.split("'", re.split(":", time_band)[0])[1]

            path_to_netcdf      = path_to_netcdf + 'trop_rainfall_' + name_of_file + '_histogram.nc'
        
        if path_to_netcdf is not None:
            dataset.to_netcdf(path = path_to_netcdf)
        return path_to_netcdf
    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def grid_attributes(self, data = None, tprate_dataset = None, variable = None):
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

        if data.time.size>1:
            time_band       = str(data.time[0].values)+', '+str(data.time[-1].values)+', freq='+str(time_interpreter(data))
        else:
            time_band       = str(data.time.values)
        if data[coord_lat].size>1:
            latitude_step   = data[coord_lat][1].values - data[coord_lat][0].values
            lat_band        = str(data[coord_lat][0].values)+', '+str(data[coord_lat][-1].values)+', freq='+str(latitude_step)
        else:
            lat_band        = data[coord_lat].values
            latitude_step   = data[coord_lat].values
        if data[coord_lon].size>1:
            longitude_step  = data[coord_lon][1].values - data[coord_lon][0].values
            lon_band        = str(data[coord_lon][0].values)+', '+str(data[coord_lon][-1].values)+', freq='+str(longitude_step)
        else:
            longitude_step  = data[coord_lon].values
            lon_band        = data[coord_lon].values 

        if variable is None:
            current_time    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            history_update  = str(current_time)+' histogram is calculated for time_band: ['+str(time_band)+']; lat_band: ['+str(lat_band)+']; lon_band: ['+str(lon_band)+'];\n '
            try:
                history_attr                    = tprate_dataset.attrs['history'] + history_update
                tprate_dataset.attrs['history'] = history_attr
            except KeyError:
                pass
                #print("The obtained xarray.Dataset doesn't have global attributes. Consider adding global attributes manually to the dataset.")
        else:
            tprate_dataset[variable].attrs['time_band'] = time_band
            tprate_dataset[variable].attrs['lat_band']  = lat_band
            tprate_dataset[variable].attrs['lon_band']  = lon_band

        return tprate_dataset
    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def add_frequency_and_pdf(self,  tprate_dataset = None, path_to_histogram = None, name_of_file = None):
        """ Function to convert the histogram to xarray.Dataset.

        Args:
            hist_counts (xarray, optional):     The histogram with counts.      Defaults to None.
            path_to_histogram (str, optional):  The path to save the histogram. Defaults to None.

        Returns:
            xarray: The xarray.Dataset with the histogram.
        """        
        hist_frequency                  = self.convert_counts_to_frequency(tprate_dataset.counts)
        tprate_dataset['frequency']     = hist_frequency

        hist_pdf                        = self.convert_counts_to_pdf(tprate_dataset.counts)
        tprate_dataset['pdf']           = hist_pdf

        if path_to_histogram is not None and name_of_file  is not None:
            self.dataset_to_netcdf(dataset = tprate_dataset, path_to_netcdf = path_to_histogram, name_of_file = name_of_file )
        return tprate_dataset

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def open_dataset(self, path_to_netcdf = None, name_of_file = None):
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
            raise FileNotFoundError("The specified dataset file was not found.")
    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def merge_two_datasets(self, tprate_dataset_1 = None, tprate_dataset_2 = None):
        """ Function to merge two datasets.

        Args:
            tprate_dataset_1 (xarray, optional):    The first dataset.     Defaults to None.
            tprate_dataset_2 (xarray, optional):    The second dataset.    Defaults to None.

        Returns:
            xarray:     The xarray.Dataset with the merged data.
        """
        
        if isinstance(tprate_dataset_1, xr.Dataset) and isinstance(tprate_dataset_2, xr.Dataset):
            dataset_3       = tprate_dataset_1.copy(deep = True)
            dataset_3.attrs = {**tprate_dataset_1.attrs, **tprate_dataset_2.attrs}
            
            for attribute in tprate_dataset_1.attrs:
                if tprate_dataset_1.attrs[attribute]    != tprate_dataset_2.attrs[attribute]:
                    dataset_3.attrs[attribute]          = str(tprate_dataset_1.attrs[attribute])+';\n '+str(tprate_dataset_2.attrs[attribute])


            dataset_3.counts.values     = tprate_dataset_1.counts.values + tprate_dataset_2.counts.values
            dataset_3.frequency.values  = self.convert_counts_to_frequency(dataset_3.counts)
            dataset_3.pdf.values        = self.convert_counts_to_pdf(dataset_3.counts)

            for variable in ('counts', 'frequency', 'pdf'):
                for attribute in tprate_dataset_1.counts.attrs:
                    dataset_3[variable].attrs                       = {**tprate_dataset_1[variable].attrs, **tprate_dataset_2[variable].attrs}
                    if tprate_dataset_1[variable].attrs[attribute]  != tprate_dataset_2[variable].attrs[attribute]:
                        dataset_3[variable].attrs[attribute]        = str(tprate_dataset_1[variable].attrs[attribute])+';\n ' + str(tprate_dataset_2[variable].attrs[attribute])

            return dataset_3
        
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def merge_list_of_histograms(self, path_to_histograms = None, multi = None, all = False):
        """ Function to merge list of histograms.

        Args:
            path_to_histograms (str, optional):     The path to the list of histograms.     Defaults to None.
            multi (int, optional):                  The number of histograms to merge.      Defaults to None.
            all (bool, optional):                   If True, all histograms in the repository will be merged. Defaults to False.

        Returns:
            xarray: The xarray.Dataset with the merged data.
        """

        histogram_list          = [f for f in listdir(path_to_histograms) if isfile(join(path_to_histograms, f))]
        histogram_list.sort()

        if all:
            histograms_to_load  = [str(path_to_histograms) + str(histogram_list[i]) for i in range(0, len(histogram_list))]
        elif multi is not None:
            histograms_to_load  = [str(path_to_histograms) + str(histogram_list[i]) for i in range(0, multi)]

        if len(histograms_to_load) > 0:
            for i in range(0, len(histograms_to_load)):
                if i == 0:
                    dataset     = self.open_dataset(path_to_netcdf = histograms_to_load[i])
                else:
                    dataset     = self.merge_two_datasets(tprate_dataset_1 = dataset,
                                                      tprate_dataset_2 = self.open_dataset(path_to_netcdf = histograms_to_load[i]))
            return dataset
        else:
            raise Exception('The specified repository is empty.')

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def convert_counts_to_frequency(self, data):
        """ Function to convert the counts to the frequency.

        Args:
            data (xarray): The counts.

        Returns:
            xarray: The frequency.
        """
        sum_of_counts           = sum(data[:])
        frequency               = data[0:]/sum_of_counts
        frequency_per_bin       = xr.DataArray(frequency, coords=[data.center_of_bin],    dims=["center_of_bin"])
        frequency_per_bin       = frequency_per_bin.assign_coords(width=("center_of_bin", data.width.values))
        frequency_per_bin.attrs = data.attrs
        sum_of_frequency        = sum(frequency_per_bin[:])
        
        if abs(sum_of_frequency -1) < 10**(-4):
            return frequency_per_bin
        else:
            raise Exception("Test failed.")

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def convert_counts_to_pdf(self, data):
        """ Function to convert the counts to the pdf.

        Args:
            data (xarray): The counts.

        Returns:
            xarray: The pdf.
        """
        sum_of_counts       = sum(data[:])
        pdf                 = data[0:]/(sum_of_counts*data.width[0:])
        pdf_per_bin         = xr.DataArray(pdf, coords=[data.center_of_bin],    dims=["center_of_bin"])
        pdf_per_bin         = pdf_per_bin.assign_coords(width=("center_of_bin", data.width.values))
        pdf_per_bin.attrs   = data.attrs
        sum_of_pdf          = sum(pdf_per_bin[:]*data.width[0:])
        
        if abs(sum_of_pdf-1.) < 10**(-4):
            return pdf_per_bin
        else:
            raise Exception("Test failed.")


    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def histogram_plot(self, data,          new_unit = None,
                       weights = None,      frequency = False,      pdf = True, \
                       smooth  = True,      step = False,           color_map = False, \
                       ls = '-',            ylogscale = True,       xlogscale = False, \
                       color = 'tab:blue',  figsize = 1,            legend = '_Hidden', \
                       plot_title = None,   loc = 'upper right',    varname = 'Precipitation',  \
                       add = None,          fig = None,             path_to_pdf = None, 
                       name_of_file = None, pdf_format=True):
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
        if fig is not None:
                fig, ax  = fig
                if color == 'tab:blue': color   = 'tab:orange'
        elif add is None and fig is None:
            fig, ax = plt.subplots( figsize=(8*figsize, 5*figsize) )
        elif add is not None:
            fig, ax  = add 
            if color == 'tab:blue': color   = 'tab:orange'

        if not pdf and not frequency:
            if 'Dataset' in str(type(data)):
                data = data['counts']
        elif pdf and not frequency:     
            if 'Dataset' in str(type(data)):
                data = data['counts']
            data = self.convert_counts_to_pdf(data)
        elif not pdf and frequency:    
            if 'Dataset' in str(type(data)):
                data = data['counts']
            data = self.convert_counts_to_frequency(data)
        
        x       =   data.center_of_bin.values
        if new_unit is not None:
            converter       = self.precipitation_rate_units_converter(1, old_unit = data.center_of_bin.units, new_unit=new_unit)
            x = converter * x
            data = data/converter
        
        if smooth:
            plt.plot(x, data,
                linewidth=3.0, ls = ls, color = color, label = legend)
            plt.grid(True)
        elif step:
            plt.step(x, data,
                linewidth=3.0, ls = ls, color = color, label = legend)
            plt.grid(True)
        elif color_map:
            if weights is None:
                N, bins, patches    = plt.hist(x = x, bins = x, weights = data,    label = legend)
            else:
                N, bins, patches    = plt.hist(x = x, bins = x, weights = weights, label = legend)
            
            fracs   = ((N**(1 / 5)) / N.max())
            norm    = colors.Normalize(fracs.min(), fracs.max())

            for thisfrac, thispatch in zip(fracs, patches):
                if color_map is True:
                    color = plt.cm.get_cmap('viridis')(norm(thisfrac))
                elif isinstance(color_map, str):
                    color = plt.cm.get_cmap(color_map)(norm(thisfrac))
                thispatch.set_facecolor(color)
        if new_unit is None:
            plt.xlabel(varname+", ["+str(data.attrs['units'])+"]", fontsize=14)
        else:
            plt.xlabel(varname+", ["+str(new_unit)+"]", fontsize=14)
        if ylogscale:
            plt.yscale('log')
        if xlogscale:
            plt.xscale('log')

        if pdf and not frequency:
            plt.ylabel('PDF',       fontsize=14)
        elif not pdf and frequency:
            plt.ylabel('Frequency', fontsize=14)
        else:
            plt.ylabel('Counts',    fontsize=14)
        
        plt.title(plot_title,       fontsize=16)


        if legend!='_Hidden':
            plt.legend(loc=loc,     fontsize=12)

        if pdf_format:
            if path_to_pdf is not None and name_of_file is not None:
                path_to_pdf      = path_to_pdf + 'trop_rainfall_' + name_of_file + '_histogram.pdf'

            if path_to_pdf is not None and isinstance(path_to_pdf, str):
                create_folder(folder    = extract_directory_path(path_to_pdf), loglevel = 'WARNING')
                plt.savefig(path_to_pdf,  format="pdf",  bbox_inches="tight")
        else:
            if path_to_pdf is not None and name_of_file is not None:
                path_to_pdf      = path_to_pdf + 'trop_rainfall_' + name_of_file + '_histogram.png'

            if path_to_pdf is not None and isinstance(path_to_pdf, str):
                create_folder(folder    = extract_directory_path(path_to_pdf), loglevel = 'WARNING')
                plt.savefig(path_to_pdf,  bbox_inches="tight")
        return {fig, ax}
    

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def mean_along_coordinate(self, data,           model_variable = 'tprate',      preprocess=True,
                              trop_lat = None,      coord   = 'time',               glob=False,
                              s_time   = None,      f_time  = None,
                              s_year   = None,      f_year  = None,
                              s_month  = None,      f_month = None):
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

        Returns:
            xarray:         The mean value of variable.
        """
        if preprocess == True:
            data = self.preprocessing(data,                                  preprocess = preprocess,
                                      model_variable   = model_variable,     trop_lat= self.trop_lat,
                                      s_time  = self.s_time,                 f_time  = self.f_time,
                                      s_year  = self.s_year,                 f_year  = self.f_year,   
                                      s_month = self.s_month,                f_month = self.f_month,
                                      sort = False,                          dask_array = False)
        coord_lat, coord_lon = self.coordinate_names(data)
        if coord in data.dims:
            
            self.class_attributes_update(trop_lat = trop_lat,
                                         s_time   = s_time,          f_time  = f_time,
                                         s_year   = s_year,          f_year  = f_year,
                                         s_month  = s_month,         f_month = f_month)
            if glob:
                return data.mean()
            else:
                if coord    == 'time':
                    return data.mean(coord_lat).mean(coord_lon)
                elif coord  == coord_lat:
                    return data.mean('time').mean(coord_lon)
                elif coord  == coord_lon:
                    return data.mean('time').mean(coord_lat)
        else:
            for i in data.dims:
                coord = i
            return data.median(coord)



    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def median_along_coordinate(self, data,                 trop_lat = None,        preprocess = True,
                                model_variable = 'tprate',  coord    = 'time',      glob       = False,
                                s_time  = None,             f_time   = None,
                                s_year  = None,             f_year   = None,
                                s_month = None,             f_month  = None):
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

        Returns:
            xarray: The median value of variable.
        """      
        if preprocess == True:
            data = self.preprocessing(data,                                  preprocess = preprocess,
                                      model_variable   = model_variable,     trop_lat= self.trop_lat,
                                      s_time  = self.s_time,                 f_time  = self.f_time,
                                      s_year  = self.s_year,                 f_year  = self.f_year,    
                                      s_month = self.s_month,                f_month = self.f_month,
                                      sort = False,                          dask_array = False)

        coord_lat, coord_lon = self.coordinate_names(data)
        if coord in data.dims:
            self.class_attributes_update(trop_lat = trop_lat,
                                         s_time   = s_time,         f_time  = f_time,
                                         s_year   = s_year,         f_year  = f_year,
                                         s_month  = s_month,        f_month = f_month)
        
            if glob:
                return data.median(coord_lat).median(coord_lon).mean('time')
            else:
                if coord    == 'time':
                    return data.median(coord_lat).median(coord_lon)
                elif coord  == coord_lat:
                    return data.median('time').median(coord_lon)
                elif coord  == coord_lon:
                    return data.median('time').median(coord_lat)
            
        else:
            for i in data.dims:
                coord = i
            return data.median(coord)
        
    
    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def mean_and_median_plot(self, data,                        glob=False,                     preprocess=True,
                             model_variable = 'tprate',         variability = False,            coord      = 'lat',  
                             trop_lat       = None,             get_mean    = True,             get_median = False,
                             s_time         = None,             f_time      = None,             s_year     = None,    
                             f_year         = None,             s_month     = None,             f_month    = None,
                             legend         = '_Hidden',        figsize     = 1,                ls         = '-',
                             maxticknum     = 12,               color       = 'tab:blue',       varname    = 'Precipitation',
                             ylogscale      = False,            xlogscale   = False,            loc        = 'upper right',
                             add            = None,             fig         = None,             plot_title = None,   
                             path_to_pdf = None,                new_unit    = None,             name_of_file=None,
                             seasons = True,                    pdf_format=True):
        """ Function to plot the mean or median value of variable in Dataset.

        Args:
            data (xarray):                  The Dataset
            model_variable (str, optional): The variable of the Dataset.            Defaults to 'tprate'.
            coord (str, optional):          The coordinate of the Dataset.          Defaults to 'time'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            variability (bool, optional):   The flag to calculate the variability of the variable.  Defaults to False.
            get_mean (bool, optional):      The flag to calculate the mean of the variable.  Defaults to True.
            get_median (bool, optional):    The flag to calculate the median of the variable.  Defaults to False.
            s_time (str, optional):         The starting time of the Dataset.       Defaults to None.
            f_time (str, optional):         The ending time of the Dataset.         Defaults to None.
            s_year (str, optional):         The starting year of the Dataset.       Defaults to None."""
        
        self.class_attributes_update(trop_lat = trop_lat, 
                                     s_time   = s_time,         f_time  = f_time,
                                     s_year   = s_year,         f_year  = f_year,
                                     s_month  = s_month,        f_month = f_month)
            

        if preprocess == True:
            data_with_final_grid = self.preprocessing(data,                                    preprocess = preprocess,
                                                        model_variable   = model_variable,     trop_lat= self.trop_lat,
                                                        s_time  = self.s_time,                 f_time  = self.f_time,
                                                        s_year  = self.s_year,                 f_year  = self.f_year,     
                                                        s_month = self.s_month,                f_month = self.f_month,
                                                        sort = False,                          dask_array = False)

        if get_mean:
            if seasons: 
                data_average = self.seasonal_or_monthly_mean(data,                               preprocess=preprocess,                  seasons = seasons,     
                                                        model_variable = model_variable,         trop_lat       = trop_lat,              new_unit = new_unit, 
                                                        coord = coord)
            else:
                data_average = self.mean_along_coordinate(data,                                     preprocess=preprocess,
                                                        glob       = glob,                      model_variable = model_variable,
                                                        trop_lat   = trop_lat,                  coord    = coord,
                                                        s_time  = self.s_time,                  f_time  = self.f_time,
                                                        s_year  = self.s_year,                  f_year  = self.f_year,     
                                                        s_month = self.s_month,                 f_month = self.f_month)
        if get_median:
            data_average = self.median_along_coordinate(data,                                   preprocess=preprocess,
                                                        glob       = glob,                      model_variable = model_variable,
                                                        trop_lat   = trop_lat,                  coord    = coord,
                                                        s_time  = self.s_time,                 f_time  = self.f_time,
                                                        s_year  = self.s_year,                 f_year  = self.f_year,     
                                                        s_month = self.s_month,                f_month = self.f_month)
        
        
        if variability      and get_mean:
            data_variability_from_average               = data_average.copy(deep=True)
            data_variability_from_average.values        = (data_average.values -  data_average.mean(coord).values)/data_average.values
        coord_lat, coord_lon= self.coordinate_names(data)

        if data[coord].size<=1:
            raise Exception("The length of the coordinate should be more than 1.")
        
        # make a plot with different y-axis using second axis object
        if coord            == 'time':
            if 'm' in time_interpreter(data_with_final_grid):
                labels      = [str(data_with_final_grid['time.hour'][i].values) + ':'+str(data_with_final_grid['time.minute'][i].values)
                                                                                    for i in range(0, data_with_final_grid.time.size)]
                labels_int  = [float(data_with_final_grid['time.hour'][i].values)   for i in range(0, data_with_final_grid.time.size)]
            elif 'H' in time_interpreter(data_with_final_grid):
                labels      = [convert_24hour_to_12hour_clock(data_with_final_grid, i)
                                                                                    for i in range(0, data_with_final_grid.time.size)]
                labels_int  = [float(data_with_final_grid['time.hour'][i].values)   for i in range(0, data_with_final_grid.time.size)]
            elif time_interpreter(data_with_final_grid) == 'D':
                labels      = [str(data_with_final_grid['time.day'][i].values + convert_monthnumber_to_str(data_with_final_grid, i))
                                                                                    for i in range(0, data_with_final_grid.time.size)]
                labels_int  = [float(data_with_final_grid['time.day'][i].values)    for i in range(0, data_with_final_grid.time.size)]
            elif time_interpreter(data_with_final_grid) == 'M':
                labels      = [convert_monthnumber_to_str(data_with_final_grid, i)
                                                                                    for i in range(0, data_with_final_grid.time.size)]
                labels_int  = [float(data_with_final_grid['time.month'][i].values)  for i in range(0, data_with_final_grid.time.size)]
            else:
                labels      = [None for i in range(0, data_with_final_grid.time.size)]
                labels_int  = [None for i in range(0, data_with_final_grid.time.size)]

        elif coord          == coord_lat:
            labels_int      = data_with_final_grid[coord_lat]
        elif coord          == coord_lon:
            labels_int      = data_with_final_grid[coord_lon]
        
        if new_unit is not None and seasons:
            data_average    = self.precipitation_rate_units_converter(data_average[0], new_unit=new_unit)
            units = new_unit
        elif new_unit is not None:
            data_average    = self.precipitation_rate_units_converter(data_average, new_unit=new_unit)
            units = new_unit
        else:
            try:
                units = data[model_variable].attrs['units']
            except KeyError:
                units = data_average.units 
        if seasons:
            y_lim_max=self.precipitation_rate_units_converter(15, old_unit='mm/day', new_unit=new_unit)
            if fig is not None:
                
                ax1, ax2, ax3, ax4, ax5, ax_twin_5 = fig[1], fig[2], fig[3], fig[4], fig[5], fig[6]
                fig = fig[0]
                axs = [ax1, ax2, ax3, ax4, ax5]
                
            elif add is None and fig is None:
                fig = plt.figure(figsize=(11*figsize, 10*figsize), layout='constrained')
                gs = fig.add_gridspec(3,2)
                if coord=='lat':
                    ax1 = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree())
                    ax2 = fig.add_subplot(gs[0, 1], projection=ccrs.PlateCarree())
                    ax3 = fig.add_subplot(gs[1, 0], projection=ccrs.PlateCarree())
                    ax4 = fig.add_subplot(gs[1, 1], projection=ccrs.PlateCarree())
                    ax5 = fig.add_subplot(gs[2, :], projection=ccrs.PlateCarree())
                    ax_twin_5 = ax5.twinx()
                else:
                    ax1 = fig.add_subplot(gs[0, 0])
                    ax2 = fig.add_subplot(gs[0, 1])
                    ax3 = fig.add_subplot(gs[1, 0])
                    ax4 = fig.add_subplot(gs[1, 1])
                    ax5 = fig.add_subplot(gs[2, :])
                    ax_twin_5 = ax4.twinx()
                axs = [ax1, ax2, ax3, ax4, ax5, ax_twin_5]
            elif add is not None:
                fig = add 
                ax1, ax2, ax3, ax4, ax5, ax_twin_5 = add
                axs = [ ax1, ax2, ax3, ax4, ax5]
            data_average = self.seasonal_or_monthly_mean(data,                               preprocess = preprocess,                  seasons = seasons,     
                                                        model_variable = model_variable,     trop_lat = self.trop_lat,              new_unit = new_unit, 
                                                        coord = coord)
            
            titles      = ["DJF", "MAM", "JJA", "SON", "GLOBAL"]

            
            for i in range(0, len(data_average)):
                one_season = data_average[i]

                axs[i].set_title(titles[i], fontsize = 16)
                # Latitude labels
                if coord == 'lat':
                    axs[i].set_xlabel('Longitude',                              fontsize=12)
                elif coord == 'lon':
                    axs[i].set_xlabel('Latitude',                             fontsize=12)
               
                if ylogscale:
                    axs[i].set_yscale('log')
                if xlogscale:
                    axs[i].set_xscale('log')
                
                if coord=='lat':
                    # twin object for two different y-axis on the sample plot
                    #      transform = ccrs.PlateCarree(), extend='both')
                    ax_span = axs[i].twinx()
                    ax_span.axhspan(-self.trop_lat, self.trop_lat,  alpha=0.05, color='tab:red')
                    ax_span.set_ylim([-90,90])
                    #ax_span.set_xlim([-180,180])
                    ax_span.set_xticks([])
                    ax_span.set_yticks([])

                    axs[i].coastlines(alpha=0.5)
                    axs[i].set_xticks(np.arange(-180,181,60), crs=ccrs.PlateCarree())
                    lon_formatter = cticker.LongitudeFormatter()
                    axs[i].xaxis.set_major_formatter(lon_formatter)

                    # Latitude labels
                    axs[i].set_yticks(np.arange(-90,91,30), crs=ccrs.PlateCarree())
                    lat_formatter = cticker.LatitudeFormatter()
                    axs[i].yaxis.set_major_formatter(lat_formatter)
                    
                    #
                    if i < 4:
                        ax_twin = axs[i].twinx()
                        ax_twin.set_frame_on(True)
                        ax_twin.plot(one_season.lon - 180,    one_season, 
                                    color = color,  label = legend,  ls = ls)
                        ax_twin.set_ylim([0, y_lim_max])
                        ax_twin.set_ylabel(str(varname)+', '+str(units),
                                                                                    fontsize=12)
                    else: 
                        ax_twin_5.set_frame_on(True)
                        ax_twin_5.plot(one_season.lon - 180,    one_season, 
                                    color = color,  label = legend,  ls = ls)
                        ax_twin_5.set_ylim([0, y_lim_max])
                        ax_twin_5.set_ylabel(str(varname)+', '+str(units),
                                                                                    fontsize=12)
                        
                else:   
                    axs[i].plot(one_season.lat,    one_season,                   color = color,  label = legend,  ls = ls)  
                    axs[i].set_ylim([0, y_lim_max])
                    axs[i].set_xlabel('Latitude',                               fontsize=12)
                    try:
                        axs[i].set_ylabel(str(varname)+', '+str(units),
                                                                                fontsize=12)
                    except KeyError:
                        axs[i].set_ylabel(str(varname),                         fontsize=12)


                axs[i].grid(True)
            if coord=='lat':
                if legend!='_Hidden':
                    ax_twin_5.legend(loc=loc,    fontsize=12,    ncol=2)
                if plot_title is not None:
                    plt.suptitle(plot_title,                       fontsize = 17)
            else: 
                if legend!='_Hidden':
                    ax5.legend(loc=loc,    fontsize=12,    ncol=2)
                if plot_title is not None:
                    plt.suptitle(plot_title,                       fontsize = 17)
            
        else:   
            if fig is not None:
                fig, ax  = fig
            elif add is None and fig is None:
                fig, ax = plt.subplots( figsize=(8*figsize, 5*figsize) )
            elif add is not None:
                fig, ax  = add
            if data_average.size== 1:
                if variability:
                    plt.axhline(y=float(data_variability_from_average),         color = color,  label = legend,  ls = ls)
                else:
                    plt.axhline(y=float(data_average.values),                   color = color,  label = legend,  ls = ls)
            else:
                if variability:
                    plt.plot(labels_int,        data_variability_from_average,  color = color,  label = legend,  ls = ls)
                else:
                    if coord == 'time':
                        plt.scatter(labels_int, data_average,                   color = color,  label = legend,  ls = ls)
                    else:
                        plt.plot(labels_int,    data_average,                   color = color,  label = legend,  ls = ls) 

            if coord == 'time':
                plt.gca().set_xticks(labels_int,  labels)

            plt.gca().xaxis.set_major_locator(plt.MaxNLocator(maxticknum))
            plt.gca().tick_params(axis = 'both',   which = 'major',    pad = 10)
            plt.xlim([min(labels_int),    max(labels_int)])
            
            plt.grid(True)

            if coord   == 'time':
                plt.xlabel('Timestep index',                        fontsize=12)
                if data['time.year'][0].values  == data['time.year'][-1].values:
                    plt.xlabel(str(data['time.year'][0].values),    fontsize=12)
                else:
                    plt.xlabel(str(data['time.year'][0].values)+' - '+str(data['time.year'][-1].values),
                                                                    fontsize=12)
            elif coord == coord_lat:
                plt.xlabel('Latitude',                              fontsize=12)
            elif coord == coord_lon:
                plt.xlabel('Longitude',                             fontsize=12)
            try:
                plt.ylabel(str(varname)+', '+str(units),
                                                                    fontsize=12)
            except KeyError:
                plt.ylabel(str(varname),                            fontsize=12)

            if plot_title is None:
                if variability:
                    plt.title('Bias of '         +str(varname),     fontsize=17,    pad=15)
                elif not variability        and get_mean:
                    plt.title('Mean values of '  +str(varname),     fontsize=17,    pad=15)
                elif not variability        and get_median:
                    plt.title('Median values of '+str(varname),     fontsize=17,    pad=15)
            else:
                plt.title(plot_title,                               fontsize=17,    pad=15)
            
            if legend!='_Hidden':
                plt.legend(loc=loc,                                 fontsize=12,    ncol=2)
            if ylogscale:
                plt.yscale('log')
            if xlogscale:
                plt.xscale('log')
        if pdf_format:      
            # set the spacing between subplots
            if path_to_pdf is not None and name_of_file is not None:
                path_to_pdf      = path_to_pdf + 'trop_rainfall_' + name_of_file + '_mean.pdf'

            if path_to_pdf is not None and isinstance(path_to_pdf, str):

                create_folder(folder    = extract_directory_path(path_to_pdf), loglevel = 'WARNING')

                plt.savefig(path_to_pdf,
                            format="pdf",
                            bbox_inches  = "tight",
                            pad_inches   = 1,
                            transparent  = True,
                            facecolor    = "w",
                            edgecolor    = 'w',
                            orientation  = 'landscape')
        else:
            if path_to_pdf is not None and name_of_file is not None:
                path_to_pdf      = path_to_pdf + 'trop_rainfall_' + name_of_file + '_mean.png'

            if path_to_pdf is not None and isinstance(path_to_pdf, str):

                create_folder(folder    = extract_directory_path(path_to_pdf), loglevel = 'WARNING')

                plt.savefig(path_to_pdf,
                            bbox_inches  = "tight",
                            pad_inches   = 1,
                            transparent  = True,
                            facecolor    = "w",
                            edgecolor    = 'w',
                            orientation  = 'landscape')
        if seasons:
            return [fig,  ax1, ax2, ax3, ax4, ax5, ax_twin_5]
        else:
            return [fig,  ax]

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def twin_data_and_observations(self, data,                          dummy_data = None,                    trop_lat = 10,                      
                                    model_variable = 'tprate',
                                    s_time  = None,                     f_time   = None,                    s_year  = None,               
                                    f_year   = None,                    s_month = None,                     f_month  = None,
                                    model = 'era5',                     source = 'monthly',                 plev = 0,
                                    space_grid_factor = None,           time_freq = None,                   preprocess = True,
                                    time_length = None,                 time_grid_factor = None):
        
        """ Function to regride the data and observations to the same grid. """

        self.class_attributes_update(trop_lat = trop_lat,
                                    s_time   = s_time,                  f_time  = f_time,
                                    s_year   = s_year,                  f_year  = f_year,
                                    s_month  = s_month,                 f_month = f_month)
        try:
            data                    = data[model_variable]
        except KeyError:
            data                    = data
        new_unit = data.units
        if dummy_data is None:
            if model                    == 'era5':
                reader                  = Reader(model="ERA5", exp="era5", source=source)
                observations            = reader.retrieve()
                observations            = self.precipitation_rate_units_converter(observations.isel(plev = plev), 
                                                                                model_variable = model_variable, 
                                                                                new_unit = new_unit)

            elif model                  == 'mswep':
                reader                  = Reader(model="MSWEP", exp="past", source=source)
                observations            = reader.retrieve()
                observations            = self.precipitation_rate_units_converter(observations,  
                                                                                model_variable = model_variable, 
                                                                                new_unit = new_unit)
            dummy_data                  = observations
    
        else:
            dummy_data                  = self.precipitation_rate_units_converter(dummy_data,  
                                                                                model_variable = model_variable, 
                                                                                new_unit = new_unit)

        if preprocess == True:
            data            = self.preprocessing(               data,                                       preprocess = preprocess,
                                                                model_variable = model_variable,            trop_lat   = self.trop_lat,
                                                                s_time  = self.s_time,                      f_time     = self.f_time,
                                                                s_year  = self.s_year,                      f_year     = self.f_year,
                                                                s_month = self.s_month,                     f_month    = self.f_month,
                                                                sort    = False,                            dask_array = False)
            dummy_data      = self.preprocessing(               dummy_data,                                 preprocess = preprocess,
                                                                model_variable = model_variable,            trop_lat   = self.trop_lat,
                                                                s_time  = self.s_time,                      f_time     = self.f_time,
                                                                s_year  = self.s_year,                      f_year     = self.f_year,
                                                                s_month = self.s_month,                     f_month    = self.f_month,
                                                                sort    = False,                            dask_array = False)

        
        data_regrided,  dummy_data_regrided = mirror_dummy_grid(data = data,                                dummy_data = dummy_data,
                                                                space_grid_factor = space_grid_factor,      time_freq  = time_freq,
                                                                time_length = time_length,                  time_grid_factor = time_grid_factor)
        return data_regrided, dummy_data_regrided
    

    


    def seasonal_or_monthly_mean(self,  data,                               preprocess=True,                    seasons = True,     
                                        model_variable = 'tprate',          trop_lat       = None,              new_unit = None, 
                                        coord = None):
        
        self.class_attributes_update(trop_lat = trop_lat)
        if seasons:
            
            if preprocess           == True:
                glob                = self.preprocessing(           data,                               preprocess = preprocess,
                                                                    trop_lat   = self.trop_lat,         model_variable = model_variable)
                glob_mean           = glob.mean('time')

                DJF_1               = self.preprocessing(           data,                               preprocess = preprocess,
                                                                    trop_lat   = self.trop_lat,         model_variable = model_variable,
                                                                    s_month = 12,                       f_month    = 12)
                DJF_2               = self.preprocessing(           data,                               preprocess = preprocess,
                                                                    trop_lat   = self.trop_lat,         model_variable = model_variable,
                                                                    s_month = 1,                        f_month    = 2)
                DJF                 = xr.concat([DJF_1, DJF_2], dim = 'time')
                DJF_mean            = DJF.mean('time')
                
                MAM                 = self.preprocessing(           data,                               preprocess = preprocess,
                                                                    trop_lat   = self.trop_lat,         model_variable = model_variable,
                                                                    s_month = 3,                        f_month    = 5)
                MAM_mean            = MAM.mean('time')
                
                JJA                 = self.preprocessing(           data,                               preprocess = preprocess,
                                                                    trop_lat   = self.trop_lat,         model_variable = model_variable,
                                                                    s_month = 6,                        f_month    = 8)
                JJA_mean            = JJA.mean('time')

                SON                 = self.preprocessing(           data,                               preprocess = preprocess,
                                                                    trop_lat   = self.trop_lat,         model_variable = model_variable,
                                                                    s_month = 9,                        f_month    = 11)
                SON_mean            = SON.mean('time')

                
                if coord == 'lon' or coord == 'lat':
                    DJF_mean = DJF_mean.mean(coord)
                    MAM_mean = MAM_mean.mean(coord)
                    JJA_mean = JJA_mean.mean(coord) 
                    SON_mean = SON_mean.mean(coord)
                    glob_mean=glob_mean.mean(coord)  

            
            all_season  = [DJF_mean, MAM_mean, JJA_mean, SON_mean, glob_mean]

            for i in range(0, len(all_season)):
                
                if new_unit is not None:
                    all_season[i]           = self.precipitation_rate_units_converter(all_season[i], new_unit=new_unit)
            return all_season
    
        else:
            all_months=[]
            for i in range(1, 13):
                if preprocess           == True:
                    mon                 = self.preprocessing(         data,                               preprocess = preprocess,
                                                                    trop_lat   = self.trop_lat,         model_variable = model_variable,
                                                                    s_month = i,                        f_month    = i)
                    mon_mean            = mon.mean('time')
                    if new_unit is not None:
                        mon_mean        = self.precipitation_rate_units_converter(mon_mean, new_unit=new_unit)
                all_months.append(mon_mean)
            return all_months


    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """   

    def plot_bias(self,         data,                               preprocess=True,                    seasons = True,     
                                dataset_2=None,                     model_variable = 'tprate',          figsize  = 1,      
                                trop_lat       = None,              plot_title = None,                  new_unit = None,      
                                vmin = None,                        vmax = None,                        contour  = True,                           
                                path_to_pdf = None,                 weights = None,                     level_95=True,
                                name_of_file = None,                pdf_format=True):
        
        self.plot_seasons_or_months(  data,                         preprocess=preprocess,              seasons = seasons,     
                                dataset_2 = dataset_2,              model_variable = model_variable,    figsize  = figsize,              
                                trop_lat = trop_lat,                plot_title = plot_title,            new_unit = new_unit,      
                                vmin = vmin,                        vmax = vmax,                        contour  = contour,                           
                                path_to_pdf = path_to_pdf,          weights = weights,                  level_95=level_95, 
                                name_of_file=name_of_file,          pdf_format=pdf_format)

    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """    
    def plot_seasons_or_months(self,     data,                      preprocess=True,                    seasons = True,     
                                dataset_2=None,                     model_variable = 'tprate',          figsize     = 1,      
                                trop_lat       = None,              plot_title = None,                  new_unit = None,      
                                vmin = None,                        vmax = None,                        contour  = True,                           
                                path_to_pdf = None,                 weights = None,                     level_95=True, 
                                value = 0.95,                       rel_error = 0.1,                    name_of_file = None,
                                pdf_format=True):
        
        """ Function to plot the mean or median value of variable in Dataset.

        Args:"""
        self.class_attributes_update(trop_lat = trop_lat)
        
        
        if seasons:
            
            fig = plt.figure(figsize=(11*figsize, 10*figsize), layout='constrained')
            gs = fig.add_gridspec(3,2)
            ax1 = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree())
            ax2 = fig.add_subplot(gs[0, 1], projection=ccrs.PlateCarree())
            ax3 = fig.add_subplot(gs[1, 0], projection=ccrs.PlateCarree())
            ax4 = fig.add_subplot(gs[1, 1], projection=ccrs.PlateCarree())
            ax5 = fig.add_subplot(gs[2, :], projection=ccrs.PlateCarree())
            axs = [ax1, ax2, ax3, ax4, ax5]
            all_season  = self.seasonal_or_monthly_mean(data,                preprocess = preprocess,        seasons  = seasons,     
                                        model_variable = model_variable,    trop_lat = self.trop_lat,            new_unit = new_unit )
            
            if vmin is None and vmax is None:
                if level_95 is True and dataset_2 is None:
                    bin_value,  units, threshold = self.get_95percent_level(data = data, original_hist = None, value = value, rel_error = rel_error, 
                                                                            model_variable=model_variable, new_unit = new_unit, weights = weights,  
                                                                            trop_lat = self.trop_lat) 
                    vmin = bin_value
                    vmax = 20*bin_value
                else:
                    vmax        = float(all_season[0].max().values)/10
                    vmin        = 0
            clevs           = np.arange(vmin, vmax, abs(vmax - vmin)/10) 

            if dataset_2 is not None:
                all_season_2 = self.seasonal_or_monthly_mean(dataset_2,                preprocess = preprocess,        seasons  = seasons,     
                                        model_variable = model_variable,    trop_lat = self.trop_lat,            new_unit = new_unit )
                for i in range(0, len(all_season)):
                    all_season[i].values = all_season[i].values - all_season_2[i].values

                data_new = data - dataset_2
            titles      = ["DJF", "MAM", "JJA", "SON", "GLOBAL"]

            
            for i in range(0, len(all_season)):
                one_season = all_season[i]
                
                #if vmin                 != None:
                one_season          = one_season.where(one_season > vmin)
                one_season, lons    = add_cyclic_point(one_season, coord=data['lon'])
                
            
                im1 = axs[i].contourf(lons, data['lat'], one_season, clevs,
                          transform = ccrs.PlateCarree(),  
                          cmap='coolwarm', extend='both')

                axs[i].set_title(titles[i], fontsize = 17)

                axs[i].coastlines()

                # Longitude labels
                axs[i].set_xticks(np.arange(-180,181,60), crs=ccrs.PlateCarree())
                lon_formatter = cticker.LongitudeFormatter()
                axs[i].xaxis.set_major_formatter(lon_formatter)

                # Latitude labels
                axs[i].set_yticks(np.arange(-90,91,30), crs=ccrs.PlateCarree())
                lat_formatter = cticker.LatitudeFormatter()
                axs[i].yaxis.set_major_formatter(lat_formatter)
                axs[i].grid(True)
        
        else:
            fig, axes   = plt.subplots(ncols=3, nrows=4, subplot_kw={'projection': ccrs.PlateCarree()},
                                     figsize=(11*figsize, 8.5*figsize), layout='constrained')
            all_months  = self.seasonal_or_monthly_mean(data,                preprocess = preprocess,        seasons  = seasons,     
                                        model_variable = model_variable,     trop_lat = trop_lat,            new_unit = new_unit )

            if vmin is None and vmax is None:
                if level_95 is True and dataset_2 is None:
                    bin_value,  units, threshold = self.get_95percent_level(data = data, original_hist = None, 
                                                                            value = value, rel_error = rel_error, model_variable=model_variable, 
                                                                            new_unit = new_unit, weights = weights,  trop_lat = self.trop_lat) 
                    vmin = bin_value
                    vmax = 20*bin_value
                else:
                    vmax        = float(all_months[6].max().values)
                    vmin        = 0
            clevs           = np.arange(vmin, vmax, (vmax - vmin)/10) 

            if dataset_2 is not None:
                all_months_2 = self.seasonal_or_monthly_mean(dataset_2,                preprocess = preprocess,        seasons  = seasons,     
                                        model_variable = model_variable,    trop_lat = trop_lat,            new_unit = new_unit )
                for i in range(0, len(all_months)):
                    all_months[i].values = all_months[i].values - all_months_2[i].values

            for i in range(0, len(all_months)):
                #if vmin                 != None:
                all_months[i]           = all_months[i].where(all_months[i] > vmin)
                all_months[i], lons     = add_cyclic_point(all_months[i], coord=data['lon'])

            titles =['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 
                     'October', 'November', 'December']
            axs=axes.flatten()
            
            for i in range(0, len(all_months)):
                im1 =axs[i].contourf(lons, data['lat'], all_months[i], clevs,
                          transform = ccrs.PlateCarree(), #vmin = vmin, vmax = vmax, 
                          cmap='coolwarm', extend='both')

                axs[i].set_title(titles[i], fontsize = 17)

                axs[i].coastlines()

                # Longitude labels
                axs[i].set_xticks(np.arange(-180,181,60), crs=ccrs.PlateCarree())
                lon_formatter = cticker.LongitudeFormatter()
                axs[i].xaxis.set_major_formatter(lon_formatter)

                # Latitude labels
                axs[i].set_yticks(np.arange(-90,91,30), crs=ccrs.PlateCarree())
                lat_formatter = cticker.LatitudeFormatter()
                axs[i].yaxis.set_major_formatter(lat_formatter)
                axs[i].grid(True)
        if new_unit is None:
            try:
                unit        = data[model_variable].units
            except KeyError:
                unit        = data.units
        else:
            unit = new_unit
        # Draw the colorbar
        cbar = fig.colorbar(im1, ax=ax5, location='bottom' ) 
        cbar.set_label(model_variable+", ["+str(unit)+"]", fontsize = 14)

        if plot_title is not None:
            plt.suptitle(plot_title,                       fontsize = 17)

        if pdf_format:
            if path_to_pdf is not None and name_of_file is not None:
                if seasons:
                    path_to_pdf      = path_to_pdf + 'trop_rainfall_' + name_of_file + '_seasons.pdf'
                else:
                    path_to_pdf      = path_to_pdf + 'trop_rainfall_' + name_of_file + '_months.pdf'

            if path_to_pdf is not None and isinstance(path_to_pdf, str):

                create_folder(folder    = extract_directory_path(path_to_pdf), loglevel = 'WARNING')

                plt.savefig(path_to_pdf,
                            format="pdf",
                            bbox_inches  = "tight",
                            pad_inches   = 1,
                            transparent  = True,
                            facecolor    = "w",
                            edgecolor    = 'w',
                            orientation  = 'landscape')
        else:
            if path_to_pdf is not None and name_of_file is not None:
                if seasons:
                    path_to_pdf      = path_to_pdf + 'trop_rainfall_' + name_of_file + '_seasons.png'
                else:
                    path_to_pdf      = path_to_pdf + 'trop_rainfall_' + name_of_file + '_months.png'

            if path_to_pdf is not None and isinstance(path_to_pdf, str):

                create_folder(folder    = extract_directory_path(path_to_pdf), loglevel = 'WARNING')

                plt.savefig(path_to_pdf,
                            bbox_inches  = "tight",
                            pad_inches   = 1,
                            transparent  = True,
                            facecolor    = "w",
                            edgecolor    = 'w',
                            orientation  = 'landscape')
  
