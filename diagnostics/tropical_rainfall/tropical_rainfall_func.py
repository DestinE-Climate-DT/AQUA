import re
import numpy as np
import pandas as pd
import xarray


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
            'day':      365,
            'hr':       8760,
            'min':      525600,
            's':        31536000,
            'ms':       3.1536e+10
        },
        'month': {
            'year':     0.0833333,
            'month':    1,
            'day':      30.4167,
            'hr':       730.001,
            'min':      43800,
            's':        2.628e+6,
            'ms':       2.628e+9
        },
        'day': {
            'year':     0.00273973,
            'month':    0.0328549,
            'day':      1,
            'hr':       24,
            'min':      1440,
            's':        86400,
            'ms':       8.64e+7
        },
        'hr': {
            'year':     0.000114155,
            'month':    0.00136986,
            'day':      0.0416667,
            'hr':       1,
            'min':      60,
            's':        3600,
            'ms':       3.6e+6
        },
        'min': {
            'year':     1.90132e-6,
            'month':    2.28311e-5,
            'day':      0.000694444,
            'hr':       0.0166667,
            'min':      1,
            's':        60,
            'ms':       60000
        },
        's': {
            'year':     3.17098e-8,
            'month':    3.80517e-7,
            'day':      1.15741e-5,
            'hr':       0.000277778,
            'min':      0.0166667,
            's':        1,
            'ms':       1000
        },
        'ms': {
            'year':     3.16888e-11,
            'month':    3.80266e-10,
            'day':      1.15741e-8,
            'hr':       2.77778e-7,
            'min':      1.66667e-5,
            's':        0.001,
            'ms':       1
        }
    }

    if from_unit not in conversion_factors or to_unit not in conversion_factors:
        print("Invalid unit. Supported units: year, month, day, hr, min, s, ms.")
        return None

    conversion_factor = conversion_factors[from_unit][to_unit]
    conversion_factor = 1/conversion_factor
    converted_value = value * conversion_factor

    return converted_value


def unit_splitter(unit):
    """ Function to split units into space and time units

    Args:
        unit (str):             The unit to be split

    Returns:
        tuple:                  The space and time units
    """
    filtered_unit = list(
        filter(None, re.split(r'\s+|/+|\*\*-1+|\*\*-2', unit)))
    try:
        mass_unit, space_unit, time_unit = filtered_unit
    except ValueError:
        try:
            mass_unit = None
            space_unit, time_unit = filtered_unit
        except ValueError:
            mass_unit = None
            space_unit = filtered_unit[0]
            time_unit = 'day'
    return mass_unit, space_unit, time_unit


def time_interpreter(dataset):
    """Identifying unit of timestep in the Dataset

    Args:
        dataset (xarray):       The Dataset

    Returns:
        str:                    The unit of timestep in input Dataset
    """

    if dataset['time'].size == 1:
        return 'False. Load more timesteps then one'
    try:
        if np.count_nonzero(dataset['time.second'] == dataset['time.second'][0]) == dataset.time.size:
            if np.count_nonzero(dataset['time.minute'] == dataset['time.minute'][0]) == dataset.time.size:
                if np.count_nonzero(dataset['time.hour'] == dataset['time.hour'][0]) == dataset.time.size:
                    if np.count_nonzero(dataset['time.day'] == dataset['time.day'][0]) == dataset.time.size or \
                            np.count_nonzero([dataset['time.day'][i] in [1, 28, 29, 30, 31] for i in range(0, len(dataset['time.day']))]) == dataset.time.size:

                        if np.count_nonzero(dataset['time.month'] == dataset['time.month'][0]) == dataset.time.size:
                            return 'Y'
                        else:
                            return 'M'
                    else:
                        return 'D'
                else:
                    timestep = dataset.time[1] - dataset.time[0]
                    n_hours = int(timestep/(60 * 60 * 10**9))
                    return str(n_hours)+'H'
            else:
                timestep = dataset.time[1] - dataset.time[0]
                n_minutes = int(timestep/(60 * 10**9))
                return str(n_minutes)+'m'
        else:
            return 1

    except KeyError and AttributeError:
        timestep = dataset.time[1] - dataset.time[0]
        if timestep >= 28 and timestep <= 31:
            return 'M'


def convert_24hour_to_12hour_clock(data, ind):
    """ Function to convert 24 hour clock to 12 hour clock

    Args:
        data (xarray):                  The Dataset
        ind (int):                      The index of the timestep

    Returns:
        str:                            The converted timestep
    """
    if data['time.hour'][ind] > 12:
        return str(data['time.hour'][ind].values - 12)+'PM'
    else:
        return str(data['time.hour'][ind].values)+'AM'


def convert_monthnumber_to_str(data, ind):
    """ Function to convert month number to string

    Args:
        data (xarray):                  The Dataset
        ind (int):                      The index of the timestep

    Returns:
        str:                            The converted timestep
    """
    if int(data['time.month'][ind]) == 1:
        return 'J'
    elif int(data['time.month'][ind]) == 2:
        return 'F'
    elif int(data['time.month'][ind]) == 3:
        return 'M'
    elif int(data['time.month'][ind]) == 4:
        return 'A'
    elif int(data['time.month'][ind]) == 5:
        return 'M'
    elif int(data['time.month'][ind]) == 6:
        return 'J'
    elif int(data['time.month'][ind]) == 7:
        return 'J'
    elif int(data['time.month'][ind]) == 8:
        return 'A'
    elif int(data['time.month'][ind]) == 9:
        return 'S'
    elif int(data['time.month'][ind]) == 10:
        return 'O'
    elif int(data['time.month'][ind]) == 11:
        return 'N'
    elif int(data['time.month'][ind]) == 12:
        return 'D'


def extract_directory_path(string):
    """
    Extracts the directory path from a string.
    """
    return "/".join(string.split("/")[:-1]) + "/"


def new_time_coordinate(data, dummy_data, freq=None, time_length=None, factor=None):
    """ Function to create new time coordinate

    Args:
        data (xarray):                  The Dataset
        dummy_data (xarray):            The Dataset
        freq (str):                     The frequency of the time coordinate
        time_length (int):              The length of the time coordinate
        factor (int):                   The factor of the time coordinate

    Returns:
        pd.date_range:                  The time coordinate
    """
    if data.time.size > 1 and dummy_data.time.size > 1:
        if data['time'][0] > dummy_data['time'][0]:
            starting_time = str(data['time'][0].values)
        elif data['time'][0] <= dummy_data['time'][0]:
            starting_time = str(dummy_data['time'][0].values)

        if freq is None:
            if time_interpreter(data) == time_interpreter(dummy_data):
                freq = time_interpreter(data)
            else:
                if (data['time'][1] - data['time'][0]) > (dummy_data['time'][1] - dummy_data['time'][0]):
                    freq = time_interpreter(data)
                else:
                    freq = time_interpreter(dummy_data)

        if time_length is None:
            if factor is None:
                if data['time'][-1] < dummy_data['time'][-1]:
                    final_time = str(data['time'][-1].values)
                elif data['time'][-1] >= dummy_data['time'][-1]:
                    final_time = str(dummy_data['time'][-1].values)
                return pd.date_range(start=starting_time, end=final_time, freq=freq)
            elif isinstance(factor, int) or isinstance(factor, float):
                time_length = data.time.size*abs(factor)
                return pd.date_range(start=starting_time, freq=freq, periods=time_length)

        else:
            return pd.date_range(start=starting_time, freq=freq, periods=time_length)
    else:
        if data.time == dummy_data.time:
            return data.time
        else:
            raise ValueError(
                'The two datasets have different time coordinates')


def new_space_coordinate(data, coord_name, new_length):
    """ Function to create new space coordinate

    Args:
        data (xarray):                  The Dataset
        coord_name (str):               The name of the space coordinate
        new_length (int):               The length of the space coordinate

    Returns:
        list:                          The space coordinate
    """
    if data[coord_name][0] > 0:
        old_lenght = data[coord_name][0].values-data[coord_name][-1].values
        delta = (old_lenght-1) / (new_length-1)
        new_coord = [data[coord_name][0].values -
                     i*delta for i in range(0, new_length)]
    else:
        old_lenght = data[coord_name][-1].values - data[coord_name][0].values
        delta = (old_lenght-1) / (new_length-1)
        new_coord = [data[coord_name][0].values +
                     i*delta for i in range(0, new_length)]
    return new_coord


def space_regrider(data, space_grid_factor=None, lat_length=None, lon_length=None):
    """ Function to regrid the space coordinate

    Args:
        data (xarray):                  The Dataset
        space_grid_factor (int):        The factor of the space coordinate
        lat_length (int):               The length of the space coordinate
        lon_length (int):               The length of the space coordinate

    Returns:
        xarray:                        The regridded Dataset
    """
    # work only for lat and lon only for now. Check the line with interpolation command and modify it in the future
    if isinstance(space_grid_factor, int):
        if space_grid_factor > 0:
            del_lat = float((float(data['lat'][1]) - float(data['lat'][0]))/2)
            del_lon = float((float(data['lon'][1]) - float(data['lon'][0]))/2)
            ds = []
            ds_element = data.copy(deep=True)
            for i in range(1, space_grid_factor):
                ds_element = ds_element.interp(
                    lat=ds_element['lat'][:] + del_lat, method="linear", kwargs={"fill_value": "extrapolate"})
                ds_element = ds_element.interp(
                    lon=ds_element['lon'][:] + del_lon, method="linear", kwargs={"fill_value": "extrapolate"})
                ds.append(ds_element)
                del_lat = del_lat/2
                del_lon = del_lon/2
            new_dataset = xarray.concat(ds, dim='lat')
            new_dataset = new_dataset.sortby(new_dataset['lat'])

        elif space_grid_factor < 0:
            space_grid_factor = abs(space_grid_factor)
            new_dataset = data.isel(
                lat=[i for i in range(0, data.lat.size, space_grid_factor)])
            new_dataset = data.isel(
                lon=[i for i in range(0, data.lon.size, space_grid_factor)])
    else:
        new_dataset = data

    if lon_length is not None and lat_length is not None:
        new_lon_coord = new_space_coordinate(
            new_dataset, coord_name='lon', new_length=lon_length)
        new_lat_coord = new_space_coordinate(
            new_dataset, coord_name='lat', new_length=lat_length)
        new_dataset = new_dataset.interp(lon=new_lon_coord, method="linear", kwargs={
                                         "fill_value": "extrapolate"})
        new_dataset = new_dataset.interp(lat=new_lat_coord, method="linear", kwargs={
                                         "fill_value": "extrapolate"})

    return new_dataset


def mirror_dummy_grid(data,  dummy_data, space_grid_factor=None, time_freq=None, time_length=None, time_grid_factor=None):
    """ Function to mirror the dummy grid

    Args:
        data (xarray):                  The Dataset
        dummy_data (xarray):            The Dataset
        space_grid_factor (int):        The factor of the space coordinate
        time_freq (str):                The frequency of the time coordinate
        time_length (int):              The length of the time coordinate
        time_grid_factor (int):         The factor of the time coordinate

    Returns:
        xarray:                        The regridded Dataset
    """
    # work only for lat and lon only for now. Check the line with interpolation command and modify it in the future
    if 'xarray' in str(type(dummy_data)):
        new_dataset_lat_lon = space_regrider(
            data, space_grid_factor=space_grid_factor, lat_length=dummy_data.lat.size, lon_length=dummy_data.lon.size)

        if data.time.size > 1 and dummy_data.time.size > 1:
            new_time_coord = new_time_coordinate(data=data, dummy_data=dummy_data, freq=time_freq,
                                                 time_length=time_length, factor=time_grid_factor)
            new_data = new_dataset_lat_lon.interp(
                time=new_time_coord, method="linear", kwargs={"fill_value": "extrapolate"})
            new_dummy_data = dummy_data.interp(
                time=new_time_coord, method="linear", kwargs={"fill_value": "extrapolate"})

            return new_data, new_dummy_data
        else:
            return new_dataset_lat_lon, dummy_data


def data_size(data):
    """ Function to get the size of the data

    Args:
        data (xarray):                  The Dataset

    Returns:
        int:                           The size of the data
    """
    if 'DataArray' in str(type(data)):
        _size = data.size
    elif 'Dataset' in str(type(data)):
        _names = list(data.dims)
        _size = 1
        for i in _names:
            _size *= data[i].size
    return _size
