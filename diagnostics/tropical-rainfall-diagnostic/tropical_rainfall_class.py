import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from matplotlib import colors
from matplotlib.ticker import PercentFormatter
import time
import fast_histogram 

import boost_histogram as bh #pip
import dask.array as da
import dask_histogram as dh # pip

import dask_histogram.boost as dhb
import dask

import math 
import re
import cartopy
import cartopy.feature as cfeature
import cartopy.crs as ccrs

import matplotlib.animation as animation
import numpy as np

from aqua import Reader

from aqua.benchmark.time_functions import time_interpreter, month_convert_num_to_str, hour_convert_num_to_str
from aqua.benchmark import graphic_creator
#from aqua.benchmark.functions_for_xarrays import  xarray_attribute_update as  xarray_attribute_update
"""The module contains Tropical Precipitation Diagnostic:

.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""

class TR_PR_Diagnostic:
    """Tropical precipitation diagnostic
    """ 
    def class_attributes_update(self,   trop_lat = None,  s_time = None, f_time = None,  
                          s_year = None, f_year = None, s_month = None, f_month = None, 
                          num_of_bins = None, first_edge = None, width_of_bin = None, bins = None):
        
        if trop_lat:    self.trop_lat = trop_lat
        if s_time:          self.s_time = s_time
        if f_time:          self.f_time = f_time
        if s_year:          self.s_year = s_year
        if f_year:          self.f_year = f_year
        if s_month:         self.s_month = s_month
        if f_month:         self.f_month = f_month
        if num_of_bins:     self.num_of_bins = num_of_bins
        if first_edge:      self.first_edge = first_edge
        if width_of_bin:    self.width_of_bin = width_of_bin 
        if bins:            self.bins = bins
        #attributes = inspect.getmembers(diag, lambda a:not(inspect.isroutine(a)))
        # check if its possible to generilize 


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
        
        """The Tropical Precipitaion constructor.
        Arguments:
            trop_lat        (int/float, optional):    The maximumal and minimal tropical latitude values in Dataset.  Defaults to 10.
            s_time          (str/int, optional):      The starting time value/index in Dataset. Defaults to None.
            f_time          (str/int, optional):      The final time value/index in Dataset. Defaults to None.
            s_year          (int, optional):          The starting/first year of the desired Dataset. 
            f_year          (int, optional):          The final/last year of the desired Dataset.
            s_month         (int, optional):          The starting/first month of the desired Dataset.
            f_month         (int, optional):          The final/last month of the desired Dataset.
            num_of_bins     (int, optional):          The number of bins in the histogram.
            first_edge      (int, optional):          The first edge of the first bin of the histogram.
            width_of_bin    (int/float, optional):    Histogram bin width.
            bins            (array, optional):        The array of bins in the histogram. Defaults to 0.
        """
        self.trop_lat   = trop_lat  
        self.s_time     = s_time
        self.f_time     = f_time  
        self.s_year     = s_year
        self.f_year     = f_year   
        self.s_month    = s_month
        self.f_month    = f_month         
        self.num_of_bins    = num_of_bins
        self.first_edge     = first_edge
        self.width_of_bin   = width_of_bin
        self.bins           = bins

    

    
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def coordinate_names(self, data):
        """Returning the names of coordinates

        Args:
            data (xarray): The Dataset.

        Returns:
            str: name of latitude coordinate
            str: name of longitude coordinate
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


    def precipitation_units_converter(self, data, variable_1 ='tprate', new_unit='m s**-1'):
        if data[variable_1].units==new_unit:
            return data
        else:
            if data[variable_1].units=='m':
                if new_unit=='m s**-1':
                    data_copy=data.copy(deep=True)
                    data_copy[variable_1].values = data_copy[variable_1].values/(60*60*24)
            elif data[variable_1].units=='m s**-1':
                if new_unit=='m':
                    data_copy=data.copy(deep=True)
                    data_copy[variable_1].values = data_copy[variable_1].values*(60*60*24)
                
            data_copy[variable_1].attrs['units']=new_unit
            return data_copy
    
        
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def ds_per_lat_range(self, data, trop_lat=None):
        """Selecting the Dataset for specified latitude range

        Args:
            data (xarray):                  The Dataset
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.

        Returns:
            xarray: The Dataset for selected latitude range.
        """        
        coord_lat, coord_lon = self.coordinate_names(data)
        self.class_attributes_update( trop_lat=trop_lat )

        data_trop = data.where(abs(data[coord_lat]) <= self.trop_lat, drop=True)  
        return data_trop
    


    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def ds_per_time_range(self, data,  s_time = None, f_time = None, s_year = None, f_year = None,
        s_month = None, f_month = None):
        """Selecting the Dataset for specified time range

        Args:
            data (xarray):              The Dataset
            s_time (str/int, optional): The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional): The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):     The starting year in Dataset. Defaults to None.
            f_year (int, optional):     The final year in Dataset. Defaults to None.
            s_month (int, optional):    The starting month in Dataset. Defaults to None.
            f_month (int, optional):    The final month in Dataset. Defaults to None.


        Raises:
            Exception: "s_year must to be integer"
            Exception: "s_year and f_year must to be integer"
            Exception: "s_month and f_month must to be integer"
            Exception: "Sorry, unknown format of time. Try one more time"
            Exception: "Sorry, unknown format of time. Try one more time"

        Returns:
            xarray: The Dataset only for selected time range. 
        """              
        self.class_attributes_update( s_time=s_time,  f_time=f_time, s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month)

        if isinstance(self.s_time, int) and isinstance(self.f_time, int): 
            if self.s_time != None and self.f_time != None:
                data = data.isel(time=slice(self.s_time, self.f_time))
        elif self.s_year != None and self.f_year == None:
            if isinstance(s_year, int):
                data= data.where(data['time.year'] == self.s_year, drop=True)
            else:
                raise Exception("s_year must to be integer")
        elif self.s_year != None and self.f_year != None:
            if isinstance(s_year, int) and isinstance(f_year, int): 
                data = data.where(data['time.year'] >= self.s_year, drop=True)
                data = data.where(data['time.year'] <= self.f_year, drop=True)
            else:
                raise Exception("s_year and f_year must to be integer") 
        if self.s_month != None and self.f_month != None:
            if isinstance(s_month, int) and isinstance(f_month, int): 
                data = data.where(data['time.month'] >= self.s_month, drop=True)
                data = data.where(data['time.month'] <= self.f_month, drop=True)  
            else:
                raise Exception("s_month and f_month must to be integer") 
        
        if isinstance(self.s_time, str) and isinstance(self.f_time, str):
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
        elif self.s_time != None and  self.f_time == None:
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
    def ds_into_array(self, data, variable_1 = 'tprate', sort = False):
        """Generatig DataArray from Dataset of specific variable

        Args:
            data (xarray):              The Dataset
            variable_1 (str, optional): The variable of the Dataset. Defaults to 'tprate'.
            sort (bool, optional):      If sort is True, the DataArray is sorted. Defaults to False.

        Returns:
            xarray: DataArray
        """        
        coord_lat, coord_lon = self.coordinate_names(data)

        if 'Dataset' in str(type(data)):
            data  = data[variable_1]

        data_1d  = data.stack(total=['time', coord_lat, coord_lon])
        if sort == True:
            data_1d  = data_1d.sortby(data_1d)
        return data_1d

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
    def mean_per_timestep(self, data, variable_1 = 'tprate', trop_lat = None, s_time = None, f_time = None, 
        s_year = None, f_year = None, s_month = None, f_month = None):
        """Calculating the mean value of varibale in Dataset 

        Args:
            data (xarray):                  The Dataset
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.

        Returns:
            float/xarray: The mean value of the argument variable for each time step. 
        """ 
        if 'lat' in data.dims:
            #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, then the argument becomes a new class attributes.
            self.class_attributes_update( s_time = s_time, f_time = f_time, trop_lat = trop_lat, 
                                   s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month)
        
        
            coord_lat, coord_lon = self.coordinate_names(data)

            ds = self.ds_per_lat_range(data, trop_lat=self.trop_lat)
            ds = self.ds_per_time_range(ds, s_time = self.s_time, f_time = self.f_time,
                                        s_year=self.s_year, f_year=self.f_year, s_month=self.s_month, f_month=self.f_month)
            if 'Dataset' in str(type(data)):
                ds = ds[variable_1]

            return ds.mean(coord_lat).mean(coord_lon)
        else:
            for i in data.dims:
                coord = i
            return data.median(coord)



    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def median_per_timestep(self, data, variable_1 = 'tprate', trop_lat = None,  s_time = None, f_time = None, 
                            s_year = None, f_year = None, s_month = None, f_month = None):
        """Calculating the median value of varibale in Dataset 

        Args:
            data (xarray):                  The Dataset
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.

        Returns:
            float/xarray: The median value of the argument variable for each time step. 
        """        

        if 'lat' in data.dims:
            #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, then the argument becomes a new class attributes.
            self.class_attributes_update(s_time = s_time, f_time = f_time, trop_lat = trop_lat, 
                                   s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month)
        
            coord_lat, coord_lon = self.coordinate_names(data)

            ds = self.ds_per_lat_range(data, trop_lat=self.trop_lat)
            ds = self.ds_per_time_range(ds, s_time = self.s_time, f_time = self.f_time, 
                                        s_year=self.s_year, f_year=self.f_year, s_month=self.s_month, f_month=self.f_month)
            if 'Dataset' in str(type(data)):
                ds = ds[variable_1]

            return ds.median(coord_lat).median(coord_lon)
        
        else:
            for i in data.dims:
                coord = i
            return data.median(coord)
    
    def mean_and_median_plot(self, data,  variable_1 = 'tprate', trop_lat = None, s_time = None, f_time = None, 
                            s_year = None, f_year = None, s_month = None, f_month = None, savelabel = '', maxticknum = 5,
                            log=True, highlight_seasons=True):
        """Plotting the mean and median values of variable

        Args:
            data (xarray):                  The Dataset
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.
            savelabel (str, optional):      The unique label of the figure in the filesystem. Defaults to ''.
            maxticknum (int, optional):     The maximal number of ticks on x-axe. Defaults to 8.
        """              
        fig, ax =  plt.subplots( ) #figsize=(8,5) )
        data_mean = self.mean_per_timestep(data, variable_1 = variable_1, trop_lat = trop_lat, s_time = s_time, f_time = f_time, 
                            s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month)
        data_median = self.median_per_timestep(data, variable_1 = variable_1, trop_lat = trop_lat, s_time = s_time, f_time = f_time, 
                            s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month)
        ax2=ax.twiny()
        # make a plot with different y-axis using second axis object
        if 'm' in time_interpreter(data):
            time_labels = [str(data['time.hour'][i].values)+':'+str(data['time.minute'][i].values) for i in range(0, len(data)) ] 
        elif 'H' in time_interpreter(data):
            time_labels = [hour_convert_num_to_str(data, i)  for i in range(0, len(data)) ]
            #time_labels = [str(data['time.hour'][i].values)+':00' for i in range(0, len(data)) ]
        elif time_interpreter(data) == 'D':
            time_labels = [str(data['time.day'][i].values+month_convert_num_to_str(data, i)) for i in range(0, len(data)) ]
        elif time_interpreter(data) == 'M':   
            time_labels = [month_convert_num_to_str(data, i) for i in range(0, len(data)) ] 
            #time_labels = [str(data['time.year'][i].values)+':'+str(data['time.month'][i].values)+':'+str(data['time.day'][i].values) for i in range(0, len(data)) ]
        else:
            time_labels = [None for i in range(0, len(data))]
        
        time_labels_int = [data['time.month'][i].values for i in range(0, data.time.size)]

        if data_mean.size == 1 and data_median.size == 1:
            ax.axhline(time_labels_int, data_mean, label= 'mean', color = 'tab:blue')
            ax.axhline(time_labels_int, data_median, label= 'median', lw=3,  alpha=0.7, ls='--', color = 'tab:orange')

            ax2.plot(time_labels, data_median.values, ls = ' ')
        else:
            ax.plot(time_labels_int, data_mean, label= 'mean', color = 'tab:blue')
            ax.plot(time_labels_int, data_median, label='median', lw=3,  alpha=0.7, ls='--', color = 'tab:orange')
            ax2.plot(time_labels_int, data_median.values, ls = ' ')
            ax2.set_xticks(time_labels_int, time_labels)

        if highlight_seasons:
            ax.axvspan(6, 9, alpha=0.2, color='red')
            ax.axvspan(11, 12, alpha=0.2, color='blue')
            ax.axvspan(1, 2, alpha=0.2, color='blue')

        ax.set_xlim([time_labels_int[0], time_labels_int[-1]])
        ax2.xaxis.set_major_locator(plt.MaxNLocator(maxticknum))
        ax.xaxis.set_major_locator(plt.MaxNLocator(maxticknum))
        ax.tick_params(axis='both', which='major', pad=10)
        ax2.tick_params(axis='both', which='major', pad=10)

        ax.grid(True)
        ax.set_xlabel('Timestep index', fontsize=12)
        if data['time.year'][0].values==data['time.year'][-1].values:
            ax2.set_xlabel(str(data['time.year'][0].values), fontsize=12)
        else:
            ax2.set_xlabel(str(data['time.year'][0].values)+' - '+str(data['time.year'][-1].values), fontsize=12)
        ax.set_ylabel('Precipitation per timestep, '+str(data.attrs['units']), fontsize=12)
        ax.set_title('Mean/median values of precipitation', fontsize =17, pad=15)
        ax.legend(fontsize=12)
        
        if log:
            ax.set_yscale('log')

        if savelabel == None:
            savelabel = str(data.attrs['title'])
            savelabel = re.split(r'[ ]', savelabel)[0]

        # set the spacing between subplots
        fig.tight_layout()
        fig.savefig("./figures/"+str(savelabel)+"_mean_and_median.png",
                    bbox_inches ="tight",
                    pad_inches = 1,
                    transparent = True,
                    facecolor ="w",
                    edgecolor ='w',
                    orientation ='landscape')
            
  
    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def preprocessing(self, data, preprocess = True,  variable_1="tprate", trop_lat=None, 
                       s_time = None, f_time = None,  
                       s_year = None, f_year = None, s_month = None, f_month = None, sort = False, dask_array = False):
        """Preprocessing the Dataset

        Args:
            data (xarray):                  The Dataset.
            preprocess (bool, optional):    If sort is True, the functiom preprocess Dataset. Defaults to True.
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.
            sort (bool, optional):          If sort is True, the DataArray is sorted. Defaults to False.
            dask_array (bool, optional):    If sort is True, the function return daskarray. Defaults to False.

        Returns:
            xarray: Preprocessed Dataset according to the arguments of the function
        """        
        
        self.class_attributes_update(trop_lat=trop_lat,  s_time=s_time, f_time=f_time,  
                               s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month)
        if preprocess == True:
            ds_per_time = self.ds_per_time_range(data, s_time=self.s_time, f_time=self.f_time, 
                                        s_year=self.s_year, f_year=self.f_year, s_month=self.s_month, f_month=self.f_month) 
            ds_var = ds_per_time[variable_1]
            #ds_var = xarray_attribute_update(ds_var, data)
            ds_per_lat = self.ds_per_lat_range(ds_var, trop_lat=self.trop_lat)
            #ds_array = self.ds_into_array(ds_per_lat, variable_1=variable_1, sort=sort)
            if dask_array == True:
                ds = da.from_array(ds_per_lat)
                return ds
            else:
                return ds_per_lat
        else:
            print("Nothong to preprocess")


    """ """ """ """ """ """ """ """ """ """
    def hist1d_fast(self, data, preprocess = True,   trop_lat = 10, variable_1 = 'tprate',  
                    s_time = None, f_time = None,
                    s_year = None, f_year = None, s_month = None, f_month = None, 
                    num_of_bins = None, first_edge = None,  width_of_bin  = None,   bins = 0):
        """Calculating the histogram with the use of fast_histogram.histogram1d (***fast_histogram*** package)

        Args:
            data (xarray):                  The Dataset.
            preprocess (bool, optional):    If sort is True, the functiom preprocess Dataset. Defaults to True.
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.
            sort (bool, optional):          If sort is True, the DataArray is sorted. Defaults to False.
            dask_array (bool, optional):    If sort is True, the function return daskarray. Defaults to False.
            
            num_of_bins (int, optional):    The number of bins in the histogram. Defaults to None.
            first_edge (float, optional):   The first edge of the histogram. Defaults to None.
            width_of_bin (float, optional): The width of the bins in the histogram. Defaults to None.
            bins (array, optional):         The array of bins in the histogram. Defaults to 0.

        Returns:
            xarray: The frequency histogram of the specified variable in the Dataset
        """        
        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.class_attributes_update(s_time = s_time, f_time = f_time, trop_lat = trop_lat, 
                               s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, 
                               num_of_bins = num_of_bins, width_of_bin = width_of_bin, bins = bins)


        if preprocess == True:
            data = self.preprocessing(data, preprocess=preprocess,  variable_1=variable_1, trop_lat=trop_lat, 
                                      s_time = self.s_time, f_time = self.f_time,
                                      s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month,  sort = False, dask_array = False)
        #print(self.bins)
        if isinstance(self.bins, int):
            left_edges_table   = [self.first_edge + self.width_of_bin*j for j in range(0, self.num_of_bins)]  
            width_table = [self.width_of_bin for j in range(0, self.num_of_bins)]  
            hist_fast   = fast_histogram.histogram1d(data, 
                                                   range=[self.first_edge, self.first_edge + (self.num_of_bins)*self.width_of_bin], bins = (self.num_of_bins))  
        else: 
            bin_table = self.bins
            left_edges_table = [bin_table[i] for i in range(0, len(self.bins)-1)]
            width_table = [bin_table[i+1]-bin_table[i] for i in range(0, len(self.bins)-1)]
            hist_fast = fast_histogram.histogram1d(data, 
                                                   range=[min(self.bins),max(self.bins)], bins = len(self.bins)-1)   
        
        
        counts_per_bin =  xr.DataArray(hist_fast, coords=[left_edges_table], dims=["left_edge"])
        counts_per_bin = counts_per_bin.assign_coords(width=("left_edge", width_table))
        counts_per_bin.attrs = data.attrs
        return  counts_per_bin 
    
    
    
    """ """ """ """ """ """ """ """ """ """
    def hist1d_np(self, data, weights=None, preprocess = True,   trop_lat = 10, variable_1 = 'tprate',  
                  s_time = None, f_time = None,   
                  s_year = None, f_year = None, s_month = None, f_month = None, 
                  num_of_bins = None, first_edge = None,  width_of_bin  = None,  bins = 0):
        """Calculating the histogram with the use of numpy.histogram (***numpy*** package)
        Args:
            data (xarray):                  The Dataset.
            preprocess (bool, optional):    If sort is True, the functiom preprocess Dataset. Defaults to True.
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.
            sort (bool, optional):          If sort is True, the DataArray is sorted. Defaults to False.
            dask_array (bool, optional):    If sort is True, the function return daskarray. Defaults to False.
            
            num_of_bins (int, optional):    The number of bins in the histogram. Defaults to None.
            first_edge (float, optional):   The first edge of the histogram. Defaults to None.
            width_of_bin (float, optional): The width of the bins in the histogram. Defaults to None.
            bins (array, optional):         The array of bins in the histogram. Defaults to 0.

        Returns:
            xarray: The frequency histogram of the specified variable in the Dataset
        """ 
        self.class_attributes_update(s_time = s_time, f_time = f_time,  trop_lat = trop_lat, 
                               s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, 
                               num_of_bins = num_of_bins, width_of_bin = width_of_bin, bins = bins)
        
        if preprocess == True:
            data = self.preprocessing(data, preprocess=preprocess, variable_1=variable_1, trop_lat=self.trop_lat, 
                                      s_time = self.s_time, f_time = self.f_time, 
                                      s_year=self.s_year, f_year=self.f_year, s_month=self.s_month, f_month=self.f_month,  
                                      sort = False, dask_array = False)
        if 'DataArray' in str(type(weights)):
            weights = self.ds_per_lat_range(weights, trop_lat=self.trop_lat)
            hist_np=0
            for i in range(0, len(data.time)):
                data_element = data.isel(time=i)
                if isinstance(self.bins, int):
                    hist_np = np.histogram(data_element, weights=weights, range=[self.first_edge, self.first_edge + (self.num_of_bins )*self.width_of_bin], \
                                           bins = (self.num_of_bins))
                else:
                    hist_np = np.histogram(data_element,  weights=weights, bins = self.bins) 
                counts_per_bin =+  xr.DataArray(hist_np[0], coords=[hist_np[1][0:-1]], dims=["left_edge"])
        else: 
            if isinstance(self.bins, int):
                hist_np = np.histogram(data, weights=weights, range=[self.first_edge, self.first_edge + (self.num_of_bins )*self.width_of_bin], bins = (self.num_of_bins))
            else:
                hist_np = np.histogram(data,  weights=weights, bins = self.bins) 
            
            counts_per_bin =  xr.DataArray(hist_np[0], coords=[hist_np[1][0:-1]], dims=["left_edge"])
       
        width_table = [hist_np[1][i+1]-hist_np[1][i] for i in range(0, len(hist_np[1])-1)]
        counts_per_bin = counts_per_bin.assign_coords(width=("left_edge", width_table))
        counts_per_bin.attrs = data.attrs
        return  counts_per_bin
        
    """ """ """ """ """ """ """ """ """ """
    def hist1d_pyplot(self, data, weights=None,preprocess = True,  trop_lat = 10,  variable_1 = 'tprate',  
                      s_time = None, f_time = None,  
                      s_year = None, f_year = None, s_month = None, f_month = None, 
                      num_of_bins = None, first_edge = None,  width_of_bin  = None,  bins = 0):
        """Calculating the histogram with the use of plt.hist (***matplotlib.pyplot*** package)
        Args:
            data (xarray):                  The Dataset.
            preprocess (bool, optional):    If sort is True, the functiom preprocess Dataset. Defaults to True.
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.
            sort (bool, optional):          If sort is True, the DataArray is sorted. Defaults to False.
            dask_array (bool, optional):    If sort is True, the function return daskarray. Defaults to False.
            
            num_of_bins (int, optional):    The number of bins in the histogram. Defaults to None.
            first_edge (float, optional):   The first edge of the histogram. Defaults to None.
            width_of_bin (float, optional): The width of the bins in the histogram. Defaults to None.
            bins (array, optional):         The array of bins in the histogram. Defaults to 0.

        Returns:
            xarray: The frequency histogram of the specified variable in the Dataset
        """ 
        self.class_attributes_update(s_time = s_time, f_time = f_time,  trop_lat = trop_lat, 
                               s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, 
                               num_of_bins = num_of_bins, width_of_bin = width_of_bin, bins=bins)  

        if preprocess == True:
            data = self.preprocessing(data, preprocess=preprocess, variable_1=variable_1, trop_lat=trop_lat, 
                                      s_time = self.s_time, f_time = self.f_time, 
                                      s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month,  sort = False, dask_array = False)

        if isinstance(self.bins, int):
            bins = [self.first_edge  + i*self.width_of_bin for i in range(0, self.num_of_bins+1)]
            left_edges_table   = [self.first_edge + self.width_of_bin*j for j in range(0, self.num_of_bins)]  
            width_table = [self.width_of_bin for j in range(0, self.num_of_bins)]  
        else:
            bins = self.bins
            left_edges_table =[self.bins[i] for i in range(0, len(self.bins)-1)]
            width_table = [self.bins[i+1]-self.bins[i] for i in range(0, len(self.bins)-1)]
            

        coord_lat, coord_lon = self.coordinate_names(data)

        if 'DataArray' in str(type(weights)):
            weights = self.ds_per_lat_range(weights, trop_lat=self.trop_lat)
            weights=weights.stack(total=[coord_lat, coord_lon])
            for i in range(0, len(data.time)):
                data_element = data.isel(time=i)
                data_element=data_element.stack(total=[coord_lat, coord_lon])
                #print(len(weights), len(data_element))
                hist_pyplt = plt.hist(x = data_element, bins = bins, weights=weights)
                counts_per_bin =+  xr.DataArray(hist_pyplt[0], coords=[hist_pyplt[1][0:-1]], dims=["left_edge"])
        else:    
            #data_element = data.isel(time=0)
            data_element=data.stack(total=['time', coord_lat, coord_lon])
            hist_pyplt = plt.hist(x = data_element, bins = bins)
            counts_per_bin =  xr.DataArray(hist_pyplt[0], coords=[hist_pyplt[1][0:-1]], dims=["left_edge"])
        plt.close()

        counts_per_bin = counts_per_bin.assign_coords(width=("left_edge", width_table))
        counts_per_bin.attrs = data.attrs
        return  counts_per_bin

        
         
    """ """ """ """ """ """ """ """ """ """
    def dask_factory(self, data, preprocess = True,   trop_lat = 10,  variable_1 = 'tprate',  
                     s_time = None, f_time = None, 
                     s_year = None, f_year = None, s_month = None, f_month = None, 
                     num_of_bins = None, first_edge = None,  width_of_bin  = None,   bins = 0, 
                     delay = False):
        """Calculating the histogram 
        Args:
            data (xarray):                  The Dataset.
            preprocess (bool, optional):    If sort is True, the functiom preprocess Dataset. Defaults to True.
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.
            sort (bool, optional):          If sort is True, the DataArray is sorted. Defaults to False.
            dask_array (bool, optional):    If sort is True, the function return daskarray. Defaults to False.
            
            num_of_bins (int, optional):    The number of bins in the histogram. Defaults to None.
            first_edge (float, optional):   The first edge of the histogram. Defaults to None.
            width_of_bin (float, optional): The width of the bins in the histogram. Defaults to None.
            bins (array, optional):         The array of bins in the histogram. Defaults to 0.
            delay (bool, optional):         
        Returns:
            xarray: The frequency histogram of the specified variable in the Dataset
        """ 

        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.class_attributes_update(s_time = s_time, f_time = f_time, trop_lat = trop_lat, 
                               s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, 
                               num_of_bins = num_of_bins, width_of_bin = width_of_bin)

        last_edge = self.first_edge  + self.num_of_bins*self.width_of_bin
        

        if preprocess == True:
            data = self.preprocessing(data, preprocess=preprocess, variable_1=variable_1, trop_lat=trop_lat, 
                                      s_time = self.s_time, f_time = self.f_time, 
                                      s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month,  sort = False, dask_array = False)


        h = dh.factory(data, 
                axes=(bh.axis.Regular(self.num_of_bins, self.first_edge, last_edge),))     
        counts, edges = h.to_dask_array()
        if delay == False:
            counts = counts.compute()
            edges = edges.compute()
        counts_per_bin =  xr.DataArray(counts, coords=[edges[0:-1]], dims=["bin"])
        counts_per_bin.attrs = data.attrs  
        return  counts_per_bin


    """ """ """ """ """ """ """ """ """ """
    def dask_factory_weights(self, data, preprocess = True,  trop_lat = 10,  variable_1 = 'tprate',  
                             s_time = None, f_time = None,
                             s_year = None, f_year = None, s_month = None, f_month = None, 
                             num_of_bins = None, first_edge = None,  width_of_bin  = None,  bins = 0,   
                             delay = False):
        """Calculating the histogram 
        Args:
            data (xarray):                  The Dataset.
            preprocess (bool, optional):    If sort is True, the functiom preprocess Dataset. Defaults to True.
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.
            sort (bool, optional):          If sort is True, the DataArray is sorted. Defaults to False.
            dask_array (bool, optional):    If sort is True, the function return daskarray. Defaults to False.
            
            num_of_bins (int, optional):    The number of bins in the histogram. Defaults to None.
            first_edge (float, optional):   The first edge of the histogram. Defaults to None.
            width_of_bin (float, optional): The width of the bins in the histogram. Defaults to None.
            bins (array, optional):         The array of bins in the histogram. Defaults to 0.
            delay (bool, optional):         
        Returns:
            xarray: The frequency histogram of the specified variable in the Dataset
        """ 

        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.class_attributes_update(s_time = s_time, f_time = f_time, trop_lat = trop_lat, 
                               s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, 
                               num_of_bins = num_of_bins, width_of_bin = width_of_bin)    

        last_edge = self.first_edge  + self.num_of_bins*self.width_of_bin

        if preprocess == True:
            data = self.preprocessing(data, preprocess=preprocess, variable_1=variable_1, trop_lat=trop_lat, 
                                      s_time = self.s_time, f_time = self.f_time,
                                      s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month,  sort = False, dask_array = False)

        ref = bh.Histogram(bh.axis.Regular(self.num_of_bins, self.first_edge, last_edge), storage=bh.storage.Weight())
        h = dh.factory(data, weights=data, histref=ref)
        counts, edges = h.to_dask_array()
        if delay == False:
            counts = counts.compute()
            edges = edges.compute()
        
        counts_per_bin =  xr.DataArray(counts, coords=[edges[0:-1]], dims=["bin"])
        counts_per_bin.attrs = data.attrs        
        return  counts_per_bin


    """ """ """ """ """ """ """ """ """ """
    def dask_boost(self, data, preprocess = True, trop_lat = 10,  variable_1 = 'tprate',  
                   s_time = None, f_time = None, 
                   s_year = None, f_year = None, s_month = None, f_month = None, 
                   num_of_bins = None, first_edge = None,  width_of_bin  = None,  bins = 0):
        """Calculating the histogram 
        Args:
            data (xarray):                  The Dataset.
            preprocess (bool, optional):    If sort is True, the functiom preprocess Dataset. Defaults to True.
            variable_1 (str, optional):     The variable of the Dataset. Defaults to 'tprate'.
            trop_lat (float, optional):     The maximumal and minimal tropical latitude values in Dataset.  Defaults to None.
            s_time (str/int, optional):     The starting time value/index in Dataset. Defaults to None.
            f_time (str/int, optional):     The final time value/index in Dataset. Defaults to None.
            s_year (int, optional):         The starting year in Dataset. Defaults to None.
            f_year (int, optional):         The final year in Dataset. Defaults to None.
            s_month (int, optional):        The starting month in Dataset. Defaults to None.
            f_month (int, optional):        The final month in Dataset. Defaults to None.
            sort (bool, optional):          If sort is True, the DataArray is sorted. Defaults to False.
            dask_array (bool, optional):    If sort is True, the function return daskarray. Defaults to False.
            
            num_of_bins (int, optional):    The number of bins in the histogram. Defaults to None.
            first_edge (float, optional):   The first edge of the histogram. Defaults to None.
            width_of_bin (float, optional): The width of the bins in the histogram. Defaults to None.
            bins (array, optional):         The array of bins in the histogram. Defaults to 0.        
        Returns:
            xarray: The frequency histogram of the specified variable in the Dataset
        """ 
        #If the user has specified a function argument **trop_lat, s_year, f_year, s_month, f_month***, 
        # then the argument becomes a new class attributes.
        self.class_attributes_update(s_time = s_time, f_time = f_time,  trop_lat = trop_lat, 
                               s_year = s_year, f_year = f_year, s_month = s_month, f_month = f_month, first_edge = first_edge, 
                               num_of_bins = num_of_bins, width_of_bin = width_of_bin) 

        last_edge = self.first_edge  + self.num_of_bins*self.width_of_bin

        if preprocess == True:
            data = self.preprocessing(data, preprocess=preprocess, variable_1=variable_1, trop_lat=trop_lat, 
                                      s_time = self.s_time, f_time = self.f_time, 
                                      s_year=s_year, f_year=f_year, s_month=s_month, f_month=f_month,  sort = False, dask_array = False)


        h = dhb.Histogram(dh.axis.Regular(self.num_of_bins, self.first_edge, last_edge),  storage=dh.storage.Double(), )
        h.fill(data)
        
        counts, edges = h.to_dask_array()
        print(counts, edges)
        counts =  dask.compute(counts)
        edges = dask.compute(edges[0]) 
        counts_per_bin =  xr.DataArray(counts[0], coords=[edges[0][0:-1]], dims=["bin"])
        #counts_per_bin.attrs = data.attrs
        #else: 
        #    counts =  dask.compute(counts.to_delayed())
        #    edges = dask.compute(edges[0].to_delayed())
           

        return  counts_per_bin


    def convert_counts_to_frequency(self, data):
        sum_of_counts = sum(data[:]) 
        frequency = data[0:]/sum_of_counts
        frequency_per_bin =  xr.DataArray(frequency, coords=[data.left_edge], dims=["left_edge"])
        frequency_per_bin = frequency_per_bin.assign_coords(width=("left_edge", data.width.values))
        frequency_per_bin.attrs = data.attrs
        
        sum_of_frequency=sum(frequency_per_bin[:]) 
        if abs(sum_of_frequency -1) < 10**(-4):
            return frequency_per_bin
        else:
            raise Exception("Test failed")

    def convert_counts_to_pdf(self, data):
        delx = data.width[0:]
        sum_of_counts = sum(data[:]) 
        pdf =  data[0:]/(sum_of_counts*data.width[0:])
        pdf_per_bin =  xr.DataArray(pdf, coords=[data.left_edge], dims=["left_edge"])
        pdf_per_bin = pdf_per_bin.assign_coords(width=("left_edge", data.width.values))
        pdf_per_bin.attrs = data.attrs

        sum_of_pdf=sum(pdf_per_bin[:]*data.width[0:]) 
        if abs(sum_of_pdf-1.) < 10**(-4):
            return pdf_per_bin
        else:
            raise Exception("Test failed")

    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def hist_plot(self, data, weights=None, frequency = False, pdf = True, smooth = True, step = False, viridis = False,  \
                  ls = '-', xlogscale = False, \
                  color = 'tab:blue', varname = 'Precipitation', save = True, plot_title = None,  label = None):
        """Ploting the histogram 

        Args:
            data (xarray):              The histogarm 
            pdf (bool, optional):       If pdf is True, the function returns the pdf histogram. Defaults to True.
            smooth (bool, optional):    If smooth is True, the function returns the smooth 2D-line instead of bars. Defaults to True.
            ls (str, optional):         The style of the line. Defaults to '-'.
            xlogscale (bool, optional): If xlogscale is True, the scale of x-axe is logaritmical. Defaults to False.
            color (str, optional):      The color of the line. Defaults to 'tab:blue'.
            varname (str, optional):    The name of the variable and x-axe. Defaults to 'Precipitation'.
            plot_title (str, optional): The title of the plot. Defaults to None.
            label (str, optional):      The unique label of the figure in the file system. Defaults to None.
        """        
   
        #pdf             (bool)      :   If ***_pdf=True***, then function returns the pdf histogram.
        #                                    If ***_pdf=False***, then function returns the frequency histogram.
        #Return:
        #    plot                        :   Frequency or pdf histogram. 
            

        fig = plt.figure( figsize=(8,5) )

        if pdf==True and frequency==False:
            data = self.convert_counts_to_pdf(data)
        elif pdf==False and frequency==True:
            data = self.convert_counts_to_frequency(data)

        if smooth:
            plt.plot(data.left_edge, data, 
                linewidth=3.0, ls = ls, color = color, label = label )
            plt.grid(True)
        elif step:
            plt.step(data.left_edge, data, 
                linewidth=3.0, ls = ls, color = color, label = label )
            plt.grid(True)
        else:
            N, bins, patches = plt.hist(x= data.left_edge, bins = data.left_edge, weights=data,  label = label)
            #if weights==None:
            #    N, bins, patches = plt.hist(x= data.left_edge, bins = data.left_edge,   label = label)
            #else:
            #    N, bins, patches = plt.hist(x= data.left_edge, bins = data.left_edge, weights = weights,  label = label)

            fracs = ((N**(1 / 5)) / N.max())
            norm = colors.Normalize(fracs.min(), fracs.max())

            for thisfrac, thispatch in zip(fracs, patches):
                color = plt.cm.viridis(norm(thisfrac))
                thispatch.set_facecolor(color)
        
        if pdf==True and frequency==False:
            plt.ylabel('PDF', fontsize=14)
        elif pdf==False and frequency==True:
            plt.ylabel('Frequency', fontsize=14)
        else:
            plt.ylabel('Counts', fontsize=14)
        

        plt.xlabel(varname+", "+str(data.attrs['units']), fontsize=14)
        plt.yscale('log')        
    
        if xlogscale == True:
            plt.xscale('log') 
        
        if plot_title == None:
            plot_title = str(data.attrs['title'])
            plot_title = re.split(r'[ ]', plot_title)[0]
        plt.title(plot_title, fontsize=16)

        if label == None:
            label = str(data.attrs['title'])
            label = re.split(r'[ ]', label)[0]

        # set the spacing between subplots
        fig.tight_layout()
        if save:
            if smooth:
                if pdf: 
                    plt.savefig('../notebooks/figures/'+str(label)+'_pdf_histogram_smooth.png')
                elif frequency:
                    plt.savefig('../notebooks/figures/'+str(label)+'_frequency_histogram_smooth.png')
                else:
                    plt.savefig('../notebooks/figures/'+str(label)+'_counts_histogram_smooth.png')
            elif step:
                if pdf: 
                    plt.savefig('../notebooks/figures/'+str(label)+'_pdf_histogram_step.png')
                elif frequency:
                    plt.savefig('../notebooks/figures/'+str(label)+'_frequency_histogram_step.png')
                else:
                    plt.savefig('../notebooks/figures/'+str(label)+'_counts_histogram_step.png')
            else:
                if pdf:
                    plt.savefig('../notebooks/figures/'+str(label)+'_pdf_histogram_viridis.png')
                elif frequency:
                    plt.savefig('../notebooks/figures/'+str(label)+'_frequency_histogram_viridis.png')
                else: 
                    plt.savefig('../notebooks/figures/'+str(label)+'_counts_histogram_viridis.png') 
    # You also can check this normalization
    #cmap = plt.get_cmap('viridis')
    #norm = plt.Normalize(min(_x), max(_x))
    #colors = cmap(norm(_x))


    """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """
    def hist_figure(self,  data, weights=None, frequency = False, pdf = True, smooth = True, step = False, viridis = False,  \
                    ls = '-', xlogscale = False, color = 'tab:blue', 
                    varname = 'Precipitation', plot_title = None,  add = None, save = True, label = None):
        """Ploting the histogram 

        Args:
            data (xarray):              The histogarm 
            pdf (bool, optional):       If pdf is True, the function returns the pdf histogram. Defaults to True.
            smooth (bool, optional):    If smooth is True, the function returns the smooth 2D-line instead of bars. Defaults to True.
            ls (str, optional):         The style of the line. Defaults to '-'.
            xlogscale (bool, optional): If xlogscale is True, the scale of x-axe is logaritmical. Defaults to False.
            color (str, optional):      The color of the line. Defaults to 'tab:blue'.
            varname (str, optional):    The name of the variable and x-axe. Defaults to 'Precipitation'.
            plot_title (str, optional): The title of the plot. Defaults to None.
            save (bool, optional):      The function saves the figure in the file system if the save is True.
            label (str, optional):      The unique label of the figure in the file system. Defaults to None.
        """        
   
        #pdf             (bool)      :   If ***_pdf=True***, then function returns the pdf histogram.
        #                                    If ***_pdf=False***, then function returns the frequency histogram.
        #Return:
        #    plot                        :   Frequency or pdf histogram. 
            

        if add==None:
            fig, ax = plt.subplots() #figsize=(8,5) 
        else: 
            fig, ax = add

        if pdf==True and frequency==False:
            data = self.convert_counts_to_pdf(data)
        elif pdf==False and frequency==True:
            data = self.convert_counts_to_frequency(data)
        
        line_label = re.split(r'/', label)[-1]
        if smooth:
            plt.plot(data.left_edge, data, 
                linewidth=3.0, ls = ls, color = color, label = line_label  )
            plt.grid(True)
        elif step:
            plt.step(data.left_edge, data, 
                linewidth=3.0, ls = ls, color = color, label = line_label )
            plt.grid(True)
        else:
            N, bins, patches = plt.hist(x= data.left_edge, bins = data.left_edge, weights = weights,  label = line_label )

            fracs = ((N**(1 / 5)) / N.max())
            norm = colors.Normalize(fracs.min(), fracs.max())

            for thisfrac, thispatch in zip(fracs, patches):
                color = plt.cm.viridis(norm(thisfrac))
                thispatch.set_facecolor(color)

        plt.xlabel(varname+", "+str(data.attrs['units']), fontsize=14)
        plt.yscale('log')
        if pdf==True and frequency==False:
            plt.ylabel('PDF', fontsize=14)
        elif pdf==False and frequency==True:
            plt.ylabel('Frequency', fontsize=14)
        else:
            plt.ylabel('Counts', fontsize=14)
        if xlogscale == True:
            plt.xscale('log') 
        
        

        if plot_title == None:
            plot_title = str(data.attrs['title'])
            plot_title = re.split(r'[ ]', plot_title)[0]
        plt.title(plot_title, fontsize=16)

        if label == None:
            label = str(data.attrs['title'])
            label = re.split(r'[ ]', label)[0]

        plt.legend(loc='upper right', fontsize=12)
        # set the spacing between subplots
        plt.tight_layout()
        if save:
            if smooth:
                if pdf: 
                    plt.savefig('../notebooks/figures/'+str(label)+'_pdf_histogram_smooth.png')
                elif frequency:
                    plt.savefig('../notebooks/figures/'+str(label)+'_frequency_histogram_smooth.png')
                else:
                    plt.savefig('../notebooks/figures/'+str(label)+'_counts_histogram_smooth.png')
            elif step:
                if pdf: 
                    plt.savefig('../notebooks/figures/'+str(label)+'_pdf_histogram_step.png')
                elif frequency:
                    plt.savefig('../notebooks/figures/'+str(label)+'_frequency_histogram_step.png')
                else:
                    plt.savefig('../notebooks/figures/'+str(label)+'_counts_histogram_step.png')
            else:
                if pdf:
                    plt.savefig('../notebooks/figures/'+str(label)+'_pdf_histogram_viridis.png')
                if frequency:
                    plt.savefig('../notebooks/figures/'+str(label)+'_frequency_histogram_viridis.png')
                else: 
                    plt.savefig('../notebooks/figures/'+str(label)+'_counts_histogram_viridis.png') 
        return {fig, ax}   
        # For i amount of step
        # Function which compute the histogramn before ploting! 
        # Save the data
        # Probability to reset the hist values (bool parameters) 
        # Only after plot



    def twin_data_and_observations(self, data, model='era5', source='monthly', plev=0, trop_lat=10, space_grid_factor=-4, 
                              time_freq=None, new_time_length=None, time_grid_factor=None):  
        variable_obs='tprate'
        if model=='era5':
            reader = Reader(model="ERA5", exp="era5", source=source)
            observations = reader.retrieve()
            observations=observations.rename_vars(({'TP':'tprate'}))

            observations = self.precipitation_units_converter(observations.isel(plev=plev), variable_1=variable_obs)
        elif model=='mswep':
            reader = Reader(model="MSWEP", exp="past", source=source)
            observations = reader.retrieve()

            observations = self.precipitation_units_converter(observations, variable_1=variable_obs)
        else:
            raise Exception("Unknown model. Please, check the catalogue and tried one more time")


        if trop_lat<90:
            data = data.where(abs(data['lat']) <= trop_lat, drop=True) 
            observations = observations.where(abs(observations['lat']) <= trop_lat, drop=True) 

        data= self.precipitation_units_converter(data, variable_1 =variable_obs)
        data_regrided, observations_regrided = graphic_creator.mirror_dummy_grid(data=data[variable_obs], 
                                                                                    dummy_data=observations[variable_obs], 
                                                                                    space_grid_factor=space_grid_factor,
                                                                                    time_freq=time_freq, 
                                                                                    new_time_length=new_time_length, 
                                                                                    time_grid_factor=time_grid_factor)
        return data_regrided, observations_regrided

    def twins_discrepancy(self, data,  model='era5', source='monthly', plev=0, trop_lat=10, space_grid_factor=-4, 
                        time_freq=None, new_time_length=None, time_grid_factor=None):

        data_regrided, observations_regrided =  self.twin_data_and_observations(data=data, model=model, source=source, 
                                                                        plev=plev, trop_lat=trop_lat, 
                                                                        space_grid_factor=space_grid_factor, 
                                                                        time_freq=time_freq, new_time_length=new_time_length, 
                                                                        time_grid_factor=time_grid_factor)
        ratio = data_regrided.copy(deep=True)
        ratio.values = observations_regrided.values/data_regrided.values
        
        relative_err = data_regrided.copy(deep=True)
        relative_err.values = abs(data_regrided.values - observations_regrided.values)/abs(data_regrided.values)

        return ratio, relative_err

    def twins_mean_and_median_comparison(self, data,  model='era5', source='monthly', get_mean=True, get_median=False, plev=0, trop_lat=10, space_grid_factor=-4, 
                        time_freq=None, new_time_length=None, time_grid_factor=None):

        data_regrided, observations_regrided =  self.twin_data_and_observations(data=data, model=model, source=source, 
                                                                        plev=plev, trop_lat=trop_lat, 
                                                                        space_grid_factor=space_grid_factor, 
                                                                        time_freq=time_freq, new_time_length=new_time_length, 
                                                                        time_grid_factor=time_grid_factor)
        if get_mean:
            data_mean = data_regrided.mean('lat').mean('lon')
            observations_mean = observations_regrided.mean('lat').mean('lon')
            ratio = data_mean/observations_mean

            return data_mean, observations_mean, ratio
        
        if get_median:
            data_median = data_regrided.median('lat').median('lon')
            observations_median = observations_regrided.median('lat').median('lon')
            ratio = data_median/observations_median

            return data_median, observations_median, ratio




