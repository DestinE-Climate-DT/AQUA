import re
from statistics import mean

def data_size(data):
    if 'DataArray' in str(type(data)):
            _size = data.size
    elif 'Dataset' in str(type(data)): 
        _names = list(data.dims) #_coord_names)
        _size = 1
        for i in _names:
            _size *= data[i].size
    return _size


def estimated_total_calc_time(ds_part, calc_time, ds_full):

    ds_part_size = data_size(ds_part)
    ds_full_size = data_size(ds_full)

    calc_time_per_element = calc_time/ds_part_size

    expected_calc_time = calc_time_per_element * ds_full_size 
    
    return float(expected_calc_time)

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

def optimal_amout_of_timesteps(ds_part, calc_time, wanted_time, ds_full):

    ds_part_size = data_size(ds_part)
    ds_full_size = data_size(ds_full)

    calc_time_per_element = calc_time/ds_part_size

    expected_calc_time = calc_time_per_element * ds_full_size 

    number = float("".join([char for char in wanted_time if char.isnumeric()]))
    time_unit = "".join([char for char in wanted_time if char.isalpha()])

    expected_calc_time =  time_units_converter( old_value = expected_calc_time, desirable_time_unit = time_unit)
    
    fraction = float(number/expected_calc_time)
    return  fraction,   int(fraction*ds_full['time'].size )

