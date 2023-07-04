import numpy as np
import re

def convert_length(value, from_unit, to_unit):
    """ Function to convert length units

    Args:
        value (float, xarray):          The value to be converted
        from_unit (str):                The unit of the value to be converted
        to_unit (str):                  The unit to be converted to

    Returns:
        float/xarray:                   The converted value
    """
    conversion_factors = {
        'm': {
            'm':    1,
            'cm':   100,
            'mm':   1000,
            'in':   39.3701,
            'ft':   3.28084
        },
        'cm': {
            'm':    0.01,
            'cm':   1,
            'mm':   10,
            'in':   0.393701,
            'ft':   0.0328084
        },
        'mm': {
            'm':    0.001,
            'cm':   0.1,
            'mm':   1,
            'in':   0.0393701,
            'ft':   0.00328084
        },
        'in': {
            'm':    0.0254,
            'cm':   2.54,
            'mm':   25.4,
            'in':   1,
            'ft':   0.0833333
        },
        'ft': {
            'm':    0.3048,
            'cm':   30.48,
            'mm':   304.8,
            'in':   12,
            'ft':   1
        }
    }

    if from_unit not in conversion_factors or to_unit not in conversion_factors:
        print("Invalid unit. Supported units: m, cm, mm, in, ft.")
        return None

    conversion_factor = conversion_factors[from_unit][to_unit]
    converted_value = value * conversion_factor

    return converted_value

def convert_time(value, from_unit, to_unit):
    """ Function to convert time units

    Args:
        value (float, xarray):          The value to be converted
        from_unit (str):                The unit of the value to be converted
        to_unit (str):                  The unit to be converted to

    Returns:
        float/xarray:                   The converted value
    """
    conversion_factors = {
        'year': {
            'year':     1,
            'month':    12,
            'days':     365,
            'hr':       8760,
            'min':      525600,
            's':        31536000,
            'ms':       3.1536e+10
        },
        'month': {
            'year':     0.0833333,
            'month':    1,
            'days':     30.4167,
            'hr':       730.001,
            'min':      43800,
            's':        2.628e+6,
            'ms':       2.628e+9
        },
        'days': {
            'year':     0.00273973,
            'month':    0.0328549,
            'days':     1,
            'hr':       24,
            'min':      1440,
            's':        86400,
            'ms':       8.64e+7
        },
        'hr': {
            'year':     0.000114155,
            'month':    0.00136986,
            'days':     0.0416667,
            'hr':       1,
            'min':      60,
            's':        3600,
            'ms':       3.6e+6
        },
        'min': {
            'year':     1.90132e-6,
            'month':    2.28311e-5,
            'days':     0.000694444,
            'hr':       0.0166667,
            'min':      1,
            's':        60,
            'ms':       60000
        },
        's': {
            'year':     3.17098e-8,
            'month':    3.80517e-7,
            'days':     1.15741e-5,
            'hr':       0.000277778,
            'min':      0.0166667,
            's':        1,
            'ms':       1000
        },
        'ms': {
            'year':     3.16888e-11,
            'month':    3.80266e-10,
            'days':     1.15741e-8,
            'hr':       2.77778e-7,
            'min':      1.66667e-5,
            's':        0.001,
            'ms':       1
        }
    }

    if from_unit not in conversion_factors or to_unit not in conversion_factors:
        print("Invalid unit. Supported units: year, month, days, hr, min, s, ms.")
        return None

    conversion_factor   = conversion_factors[from_unit][to_unit]
    conversion_factor   = 1/conversion_factor
    converted_value     = value * conversion_factor

    return converted_value

def unit_splitter(unit):
    """ Function to split units into space and time units

    Args:
        unit (str):             The unit to be split

    Returns:
        tuple:                  The space and time units
    """
    filtered_unit                           = list(filter(None, re.split(r'\s+|/+|\*\*-1+|\*\*-2', unit)))
    try:
        mass_unit, space_unit, time_unit    = filtered_unit
    except ValueError:
        try:
            mass_unit                       = None
            space_unit, time_unit           = filtered_unit
        except ValueError:
            mass_unit                       = None
            space_unit                      = filtered_unit[0]
            time_unit                       = 'days'
    print(mass_unit, space_unit, time_unit)
    return mass_unit, space_unit, time_unit

def time_interpreter(dataset):
    """Identifying unit of timestep in the Dataset

    Args:
        dataset (xarray):       The Dataset

    Returns:
        str:                    The unit of timestep in input Dataset
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
        else:
            return 1

    except KeyError and AttributeError:
        timestep = dataset.time[1] - dataset.time[0]
        if timestep >=28 and timestep <=31:
            return 'M'

def extract_directory_path(string):
    """
    Extracts the directory path from a string.
    """
    return "/".join(string.split("/")[:-1]) + "/"