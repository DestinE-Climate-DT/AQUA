'''
This module contains functions to evaluate teleconnection indices 
for different teleconnections.
'''
from tools import *

def station_based_index(field,namelist,telecname,months_window=3):
    """
    Evaluate station based index for a teleconnection.

    Args:
        field (DataArray):        field over which evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3

    Returns:
        indx (DataArray): standardized station based index
    """
    # 1. -- Monthly field average and anomalies--
    field_av  = field.groupby("time.month").mean(dim="time")
    field_an  = field.groupby("time.month") - field_av

    # 2. -- Acquiring latitude and longitude of stations --
    lon1 = lon_180_to_360(namelist[telecname]['lon1'])
    lat1 = namelist[telecname]['lat1']
    lon2 = lon_180_to_360(namelist[telecname]['lon2'])
    lat2 = namelist[telecname]['lat2']

    # 3. -- Extracting field data at the acquired coordinates --
    field_an1 = field_an.sel(lon=lon1,lat=lat1,method='nearest')
    field_an2 = field_an.sel(lon=lon2,lat=lat2,method='nearest')

    # 4. -- Rolling average over months = months_window --
    field_an1_ma = field_an1.rolling(time=months_window,center=True).mean() # to be generalized to data not gridded monthly
    field_an2_ma = field_an2.rolling(time=months_window,center=True).mean()

    # 5. -- Evaluate average and std for the station based difference --
    diff_ma = field_an1_ma-field_an2_ma
    mean_ma = diff_ma.mean()
    std_ma  = diff_ma.std()

    # 6. -- Evaluate the index and rename the variable in the DataArray --
    indx = (diff_ma-mean_ma)/std_ma
    indx = indx.rename('index')
    
    return indx

def regional_mean_index(field,namelist,telecname,months_window=3):
    """
    Evaluate field mean for a teleconnection.

    Args:
        field (DataArray):        field over which evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3

    Returns:
        field mean (DataArray): field mean
    """
    # 1. -- Acquire coordinates --
    lonW = lon_180_to_360(namelist[telecname]['lonW']) # conversion can be inserted as a check in other funcs using coordinates
    lonE = lon_180_to_360(namelist[telecname]['lonE'])
    latN = namelist[telecname]['latN']
    latS = namelist[telecname]['latS']

    # 2. -- Evaluate mean value of the field and then the rolling mean --
    field_mean = wgt_area_mean(field, latN, latS, lonW, lonE)
    field_mean = field_mean.rolling(time=months_window,center=True).mean(skipna=True)

    return field_mean