import re
import os
from statistics import mean
import timeit, functools
import runpy

def data_size(data):
    if 'DataArray' in str(type(data)):
            _size = data.size
    elif 'Dataset' in str(type(data)): 
        _names = list(data.dims) #_coord_names)
        _size = 1
        for i in _names:
            _size *= data[i].size
    return _size

with os.popen("pwd ") as f:
    _pwd = f.readline()
pwd = re.split(r'[\n]', _pwd)[0]

def estimated_total_calc_time_opt(ds_full, number=2):

    res = timeit.timeit("run_path(path_name=str(pwd)+'/../notebooks/time_memory_estimation/time_test.py')", setup="from runpy import run_path",  number=number)
    res = res/number
    # if script is in the same folder
    #runpy.run_module(mod_name='time_test')
    
    from time_test import TEST_SIZE

    ds_full_size = data_size(ds_full)

    calc_time_per_element = res/TEST_SIZE

    expected_calc_time = calc_time_per_element * ds_full_size 
    
    return float(expected_calc_time)

def optimal_amout_of_timesteps_opt(wanted_time, ds_full):
    res = timeit.timeit("run_path(path_name=str(pwd)+'/../notebooks/time_memory_estimation/time_test.py')", setup="from runpy import run_path",  number=number)
    res = res/number
    
    from time_test import TEST_SIZE
    ds_full_size = data_size(ds_full)

    calc_time_per_element = res/TEST_SIZE

    expected_calc_time = calc_time_per_element * ds_full_size 

    number = float("".join([char for char in wanted_time if char.isnumeric()]))
    time_unit = "".join([char for char in wanted_time if char.isalpha()])

    expected_calc_time =  time_units_converter( old_value = expected_calc_time, desirable_time_unit = time_unit)
    
    fraction = float(number/expected_calc_time)
    return  fraction,   int(fraction*ds_full['time'].size )

def time_units_converter( old_value, desirable_time_unit):
    if desirable_time_unit == 's' or desirable_time_unit == "":
        return old_value 
    elif desirable_time_unit == 'm':
        return old_value / (60)
    elif desirable_time_unit == 'H':
        return old_value / (60 * 60)
    elif desirable_time_unit == 'D':
        return old_value / (60 * 60 * 24)
    elif desirable_time_unit == 'M':
        return old_value / (60 * 60 * 24 * 30)
    else:
        return 'unknown type'



