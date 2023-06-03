import re
from statistics import mean
import numpy as np
import xarray 
import pandas as pd

"""The module contains time-related functions:
    - time_interpreter,
    - time_units_converter,
    - estimated_total_calc_time,
    - optimal_amout_of_timesteps,
    -  time_into_plot_title,
    - month_convert_num_to_str,
    - hour_convert_num_to_str


.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""

def data_size(data):
    """Returning the size of the Dataset or DataArray

    Args:
        data (xarray): Dataset or dataArray, the size of which we would like to return
    Returns:
        int: size of data
    """  
    if 'DataArray' in str(type(data)):
            size = data.size
    elif 'Dataset' in str(type(data)): 
        names = list(data.dims) #_coord_names)
        size = 1
        for i in names:
            size *= data[i].size
    return size


def estimated_total_calc_time(ds_part=None, calc_time=None, ds_full=None, units = 'H'):
    """Estimating the calculation time of the total Dataset

    Args:
        ds_part (xarray):       The part of the Dataset for which the timing test was performed
        calc_time (float):      The calculation time in seconds of the function or script for ds_part
        ds_full (xarray):       The Dataset, for which we want to estimate calculation time
        units (str, optional):  The units of time, in which we would like to estimate the calculation time for Dataset. 
                                Defaults to 'H'.

    Returns:
        float: estimated calculation time in seconds
        float  estimated calculation time in specified time units
    """
    ds_part_size = len(ds_part.time) 
    ds_full_size = len(ds_full.time) 

    calc_time_per_element = calc_time/ds_part_size

    expected_calc_time = calc_time_per_element * ds_full_size 
    
    return float(expected_calc_time),  (time_units_converter(expected_calc_time,   units))



def optimal_amount_of_timesteps(ds_part=None, calc_time=None, wanted_time=None, ds_full=None):
    """Estimating the number of indexes in time coordinate, for which we can perform calculations during the specified time

    Args:
        ds_part (xarray):   The part of the Dataset for which the timing test was performed
        calc_time (float):  The calculation time in seconds of the function or script for ds_part
        wanted_time (str):  The string, which contains the time unit and number. For example, '3H' or '1D' 
        ds_full (xarray):   The Dataset, for which we want to estimate calculation time

    Returns:
        float: The fraction of the 'ds_full' can be calculated during the 'wanted_time'
        int:  The amount of the time steps of the 'ds_full' can be calculated during the 'wanted_time'

    """    

    ds_part_size = len(ds_part.time) 
    ds_full_size = len(ds_full.time)

    calc_time_per_element = calc_time/ds_part_size

    expected_calc_time = calc_time_per_element * ds_full_size 

    number = float("".join([char for char in wanted_time if char.isnumeric()]))
    time_unit = "".join([char for char in wanted_time if char.isalpha()])

    expected_calc_time =  time_units_converter( old_value = expected_calc_time, time_unit = time_unit)
    
    fraction = float(number/expected_calc_time)
    return  fraction,   int(fraction*ds_full['time'].size )


def time_units_converter( old_value, time_unit): #check the names and correct 
    """Converting the seconds into the specified unit of time

    Args:
        old_value (float):  The amount of time in the seconds, which function converts in the different time unit
        time_unit (str):    The unit of time to which the function converts the old_value

    Raises:
        ValueError: The specified time unit was not implemented

    Returns:
        float: The amount of time in the specified unit
    """    
    
    if time_unit == 's' or time_unit == "":
        return old_value 
    elif time_unit == 'm':
        return old_value / (60)
    elif time_unit == 'H':
        return old_value / (60 * 60)
    elif time_unit == 'D':
        return old_value / (60 * 60 * 24)
    elif time_unit == 'M':
        return old_value / (60 * 60 * 24 * 30)
    else:
        raise ValueError("time unit not found") 


def month_convert_num_to_str(data, ind):
    """Convecting the month number into the first letter of the month

    Args:
        data (xarray):  The Dataset
        ind (int):      The time index in the Dataset

    Returns:
        str: The first letter of the month of input Dataset and input time index
    """    
    if int(data['time.month'][ind]) == 1: return 'J'
    elif int(data['time.month'][ind]) == 2: return 'F'
    elif int(data['time.month'][ind]) == 3: return 'M'   
    elif int(data['time.month'][ind]) == 4: return 'A'
    elif int(data['time.month'][ind]) == 5: return 'M'
    elif int(data['time.month'][ind]) == 6: return 'J'
    elif int(data['time.month'][ind]) == 7: return 'J'
    elif int(data['time.month'][ind]) == 8: return 'A'
    elif int(data['time.month'][ind]) == 9: return 'S'
    elif int(data['time.month'][ind]) == 10: return 'O'
    elif int(data['time.month'][ind]) == 11: return 'N'
    elif int(data['time.month'][ind]) == 12: return 'D'

def hour_convert_num_to_str(data, ind):
    """Converting the hour of input Dataset into 12-hour clock format

    Args:
        data (xarray):  The Dataset
        ind (int):      The time index in the Dataset

    Returns:
        str: The hour in 12-hour clock format of input Dataset and input time index
    """    
    if data['time.hour'][ind] > 12: return  str(data['time.hour'][ind].values - 12)+'PM' 
    else: return str(data['time.hour'][ind].values)+'AM'

""" """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ """ 
def time_interpreter(dataset):
    """Identifying unit of timestep in the Dataset

    Args:
        dataset (xarray): The Dataset

    Returns:
        str: The unit of timestep in input Dataset
    """
    
    if dataset['time'].size==1:
        return 'False. Load more timesteps then one'
    try:
        if np.count_nonzero(dataset['time.second'] == dataset['time.second'][0]) == dataset.time.size:
            if np.count_nonzero(dataset['time.minute'] == dataset['time.minute'][0]) == dataset.time.size:
                if  np.count_nonzero(dataset['time.hour'] == dataset['time.hour'][0]) == dataset.time.size:
                    if np.count_nonzero(dataset['time.day'] == dataset['time.day'][0] ) == dataset.time.size or \
                        np.count_nonzero([dataset['time.day'][i] in [1, 28, 29, 30, 31] for i in range(0, len(dataset['time.day']))]) == dataset.time.size:
                                            
                        if np.count_nonzero(dataset['time.month'] == dataset['time.month'][0]) == dataset.time.size:
                            return 'Y'
                        else:
                            return 'M'  
                    else:
                        return 'D'
                else:
                    timestep = dataset.time[1] - dataset.time[0]
                    n_hours = int(timestep/(60 * 60 * 10**9) )
                    return str(n_hours)+'H'
            else:
                timestep = dataset.time[1] - dataset.time[0]
                n_minutes = int(timestep/(60  * 10**9) )
                return str(n_minutes)+'m'
                # separate branch, PR, not part of this diagnostic
        else:
            return 1
    
    except KeyError and AttributeError:
        timestep = dataset.time[1] - dataset.time[0]
        if timestep >=28 and timestep <=31:
            return 'M'  


def time_into_plot_title(data, ind):
    """Converting the time element into a shorter/readable format. Function cuts the part of the time 
    which is smaller than the timestep of the Dataset

    Args:
        data (xarray):  The Dataset
        ind (int):      The time index in the Dataset

    Returns:
        str: The time element of Dataset units, which is bigger than the timestep of the Dataset 
    """    
    if 'm' in time_interpreter(data):
        if str(data['time.hour'][ind].values)<10:
            hour = '0'+str(data['time.hour'][ind].values)
        else:
            hour = str(data['time.hour'][ind].values)
        time_labels = str(data['time.year'][ind].values)+':'+str(data['time.month'][ind].values)+':'+str(data['time.day'][ind].values)+':'+hour #+':'+str(data['time.minute'][ind].values)   
    elif 'H' in time_interpreter(data):
        time_labels = str(data['time.year'][ind].values)+':'+str(data['time.month'][ind].values)+':'+str(data['time.day'][ind].values)+':'+str(data['time.hour'][ind].values)  
    elif time_interpreter(data) == 'D':
        time_labels = str(data['time.year'][ind].values)+':'+str(data['time.month'][ind].values)+':'+str(data['time.day'][ind].values) 
    elif time_interpreter(data) == 'M':   
        time_labels = str(data['time.year'][ind].values)+':'+str(data['time.month'][ind].values) 
    else:
        time_labels = data['time'][ind]
    return time_labels


def new_time_coord(data, factor = 1, new_unit = None):
    """Creating the new time coordinate

    Args:
        data (xarray):              The Dataset
        factor (float, optional):   The factor multiplies the original frequency of the Dataset and, therefore, 
                                    defines the new frequency of the Dataset. Defaults to 1.
        new_unit (str, optional):   New time unit of the Dataset. Defaults to None.

    Returns:
        DatetimeIndex: New time array with specified frequency 
    """    
    
    t_initial = int(data['time'][0] )
    old_unit = time_interpreter(data)
    old_number = float("".join([char for char in old_unit if char.isnumeric()]))
    old_time_unit_name = "".join([char for char in old_unit if char.isalpha()]) 
    if new_unit == None:
        if factor < 1:
            new_unit = str(old_number*abs(factor))+str(old_time_unit_name)
            print(new_unit) 
            return pd.date_range(t_initial, freq=new_unit, periods=int(data['time'].size*abs(factor)))
        else:
            new_unit = str(old_number*factor)+str(old_time_unit_name)
            print(new_unit) 
            return pd.date_range(t_initial, freq=new_unit, periods=int(data['time'].size/factor))
    else:
        new_number = float("".join([char for char in new_unit if char.isnumeric()]))
        new_time_unit_name = "".join([char for char in new_unit if char.isalpha()]) 
        factor = old_number/new_number
        return pd.date_range(t_initial, freq=new_unit, periods=int(data['time'].size*abs(factor))) 
    

def time_regrider(data, time_grid_factor =None, new_time_unit = None):
    """The time regrider of the Dataset

    Args:
        data (xarray):                      The Dataset
        time_grid_factor (int, optional):   The resolution of the new time grid. If the input value is negative, the time grid will be less dense 
                                            in time_grid_factor time. If the input value is positive, the time grid will be dense 
                                            in time_grid_factor times. Defaults to None.
        new_time_unit (str, optional):      The string, which contains the time unit and number. For example, '3H' or '1D'. 
                                            Defaults to None.

    Returns:
        xarray: The time regrided Dataset.
    """    
    # Add units converter!!!
    if new_time_unit!=None and  time_grid_factor ==None:
        old_unit = time_interpreter(data)
        old_number = float("".join([char for char in old_unit if char.isnumeric()]))
        old_time_unit_name = "".join([char for char in old_unit if char.isalpha()]) 
        new_number = float("".join([char for char in new_time_unit if char.isnumeric()]))
        new_time_unit_name = "".join([char for char in new_time_unit if char.isalpha()]) 
        
        if int(old_number/new_number)>0: 
            time_grid_factor  = int(old_number/new_number)
        else:
            time_grid_factor  = -  int(new_number/old_number) 

    if isinstance(time_grid_factor , int):
        if time_grid_factor >1:
            del_t = int((int(data['time'][1])- int(data['time'][0]))/time_grid_factor )
            ds = []
            for i in range(1, time_grid_factor ):
                new_dataset = data.copy(deep=True)
                new_dataset['time'] = data['time'][:]+del_t
                new_dataset.values = data.interp(time=new_dataset['time'][:])
                ds.append(new_dataset)
            combined = xarray.concat(ds, dim='time')
            return combined
        else:
            time_grid_factor  = abs(time_grid_factor )
            new_dataset = data[::time_grid_factor ]
            for i in range(0, data['time'].size):
                new_dataset[i]=data[i:i+time_grid_factor ].mean()
                return new_dataset
    

