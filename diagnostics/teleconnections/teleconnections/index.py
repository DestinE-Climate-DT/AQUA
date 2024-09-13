'''
This module contains functions to evaluate teleconnection indices
for different teleconnections.

Functions:
    station_based_index: evaluate station based index
    regional_mean_index: evaluate regional mean index
    regional_mean_anomalies: evaluate regional mean anomalies
'''
import xarray as xr

from aqua.logger import log_configure
from teleconnections.tools import lon_180_to_360, wgt_area_mean

xr.set_options(keep_attrs=True)


def station_based_index(field: xr.DataArray,
                        namelist: dict,
                        telecname='NAO',
                        months_window=3,
                        loglevel='WARNING'):
    """
    Evaluate station based index for a teleconnection.
    Field data must be monthly gridded data.

    Args:
        field (xr.DataArray):     field over which evaluate the index
        namelist (dict):          teleconnection yaml infos
        telecname (str, opt):     name of the teleconnection to be evaluated
                                  Default is NAO
        months_window (int, opt): months for rolling average, default is 3
        loglevel (str, opt):      log level, default is WARNING

    Returns:
        (xarray.DataArray): standardized station based index
    """
    logger = log_configure(loglevel, 'station based index')
    logger.debug('Evaluating station based index for %s', telecname)

    if field.lon.min() < 0:
        logger.debug('Data longitudes are -180-180, not converting')
        lon1 = namelist[telecname]['lon1']
        lon2 = namelist[telecname]['lon2']
    else:
        logger.debug('Data longitudes are 0-360, converting teleconnection coords')
        lon1 = lon_180_to_360(namelist[telecname]['lon1'])
        lon2 = lon_180_to_360(namelist[telecname]['lon2'])

    lat1 = namelist[telecname]['lat1']
    lat2 = namelist[telecname]['lat2']

    logger.debug('Station 1: lon = %s, lat = %s', lon1, lat1)
    logger.debug('Station 2: lon = %s, lat = %s', lon2, lat2)

    # We select first the field data at the two stations
    field1 = field.sel(lon=lon1, lat=lat1, method='nearest')
    field2 = field.sel(lon=lon2, lat=lat2, method='nearest')

    logger.debug("Loading data in memory")
    field1 = field1.load()
    field2 = field2.load()

    # Monthly field average and anomalies
    field1_av = field1.groupby("time.month").mean(dim="time")
    field1_an = field1.groupby("time.month") - field1_av

    field2_av = field2.groupby("time.month").mean(dim="time")
    field2_an = field2.groupby("time.month") - field2_av

    # Rolling average over months = months_window
    field1_an_ma = field1_an.rolling(time=months_window, center=True).mean()
    field2_an_ma = field2_an.rolling(time=months_window, center=True).mean()

    # Evaluate average and std for the station based difference
    diff_ma = field1_an_ma-field2_an_ma
    mean_ma = diff_ma.mean()
    std_ma = diff_ma.std()

    # Evaluate the index and rename the variable in the DataArray
    indx = (diff_ma-mean_ma)/std_ma
    indx = indx.rename('index')

    # Drop NaNs
    logger.debug('Dropping NaNs')
    indx = indx.dropna(dim='time')

    logger.debug('Index evaluated')
    return indx


def regional_mean_index(field, namelist, telecname, months_window=3,
                        loglevel='WARNING'):
    """
    Evaluate regional field mean for a teleconnection.
    Data must be monthly gridded data.

    Args:
        field (xarray.DataArray): field over which evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3
        loglevel (str, opt):      log level, default is WARNING

    Returns:
        (xarray.DataArray): regional field mean
    """
    # 0. -- Logging --
    logger = log_configure(loglevel, 'regional mean index')
    logger.debug('Evaluating regional mean index for %s', telecname)

    # 1. -- Acquire coordinates --
    if field.lon.min() < 0:
        logger.info('Data longitudes are -180-180, not converting')
        lonW = namelist[telecname]['lonW']
        lonE = namelist[telecname]['lonE']
    else:
        logger.info('Data longitudes are 0-360, converting teleconnection coords')
        lonW = lon_180_to_360(namelist[telecname]['lonW'])
        lonE = lon_180_to_360(namelist[telecname]['lonE'])

    latN = namelist[telecname]['latN']
    latS = namelist[telecname]['latS']

    logger.debug('Region: lon = %s; %s, lat = %s; %s', lonW, lonE, latS, latN)

    # 2. -- Evaluate mean value of the field and then the rolling mean --
    field_mean = wgt_area_mean(field, latN, latS, lonW, lonE)
    field_mean = field_mean.rolling(time=months_window,
                                    center=True).mean(skipna=True)

    # 7. -- Drop NaNs --
    logger.debug('Dropping NaNs')
    field_mean = field_mean.dropna(dim='time')
    field_mean = field_mean.rename('index')

    logger.debug('Index evaluated')
    return field_mean


def regional_mean_anomalies(field, namelist, telecname, months_window=3,
                            loglevel='WARNING'):
    """
    Evaluate regional field mean anomalies for a teleconnection.
    Data must be monthly gridded data.

    Args:
        field (xarray.DataArray): field over which evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3
        loglevel (str, opt):      log level, default is WARNING

    Returns:
        (xarray.DataArray): regional field mean anomalies
    """
    logger = log_configure(loglevel, 'regional mean anomalies')
    logger.debug('Evaluating regional mean anomalies for %s', telecname)

    # Acquire coordinates
    if field.lon.min() < 0:
        logger.debug('Data longitudes are -180-180, not converting')
        lonW = namelist[telecname]['lonW']
        lonE = namelist[telecname]['lonE']
    else:
        logger.debug('Data longitudes are 0-360, converting')
        lonW = lon_180_to_360(namelist[telecname]['lonW'])
        lonE = lon_180_to_360(namelist[telecname]['lonE'])

    latN = namelist[telecname]['latN']
    latS = namelist[telecname]['latS']

    logger.debug('Region: lon = %s; %s, lat = %s; %s', lonW, lonE, latS, latN)

    # Evaluate mean value of the field and then the rolling mean
    field_mean = wgt_area_mean(field, latN, latS, lonW, lonE,
                               loglevel=loglevel)

    logger.debug("Loading data in memory")
    field_mean = field_mean.load()

    field_mean_an = field_mean.groupby("time.month") -\
        field_mean.groupby("time.month").mean(dim="time")

    field_mean_an = field_mean_an.rolling(time=months_window,
                                          center=True).mean(skipna=True)
    field_mean_an = field_mean_an.rename('index')

    logger.debug('Index evaluated')
    return field_mean_an
