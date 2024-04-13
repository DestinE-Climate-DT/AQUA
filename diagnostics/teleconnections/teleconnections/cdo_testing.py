'''
This module contains functions to compare and test the teleconnections
libraries with similar procedures done with cdo bindings.
'''
import numpy as np
import xarray as xr
from cdo import Cdo
from aqua.logger import log_configure

from teleconnections.index import station_based_index, regional_mean_index
from teleconnections.tools import lon_180_to_360

xr.set_options(keep_attrs=True)


def station_based_cdo(infile, namelist, telecname, months_window=3,
                      loglevel='WARNING'):
    """
    Evaluate station based index for a teleconnection with cdo
    bindings.

    Args:
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3
        loglevel (str, opt):      log level, default is WARNING

    Returns:
        indx (DataArray): standardized station based index
    """
    logger = log_configure(loglevel, 'cdo station based index')

    logger.info('Evaluating station based index for %s with cdo bindings',
                telecname)

    logger.debug('Input file: %s', infile)
    logger.debug('Teleconnection name: %s', telecname)
    logger.debug('Months window: %s', months_window)
    logger.debug('Opening cdo class instance')
    cdo = Cdo()

    # Monthly field average and anomalies
    field_ma = cdo.monmean(input=infile)
    field_ma_av = cdo.ymonmean(input=field_ma)
    field_an = cdo.sub(input=[field_ma, field_ma_av])

    # Acquiring latitude and longitude of stations
    data = xr.open_dataset(infile)
    if data.lon.min() < 0:
        logger.debug('Data longitudes are -180-180, not converting')
        lon1 = namelist[telecname]['lon1']
        lon2 = namelist[telecname]['lon2']
    else:
        logger.info('Data longitudes are 0-360, '
                    'converting teleconnection coords')
        lon1 = lon_180_to_360(namelist[telecname]['lon1'])
        lon2 = lon_180_to_360(namelist[telecname]['lon2'])

    lat1 = namelist[telecname]['lat1']
    lat2 = namelist[telecname]['lat2']

    logger.debug('Station 1: lon = %s, lat = %s', lon1, lat1)
    logger.debug('Station 2: lon = %s, lat = %s', lon2, lat2)

    # Extracting field data at the acquired coordinates
    field_an1 = cdo.remapnn("lon={0}_lat={1}".format(lon1, lat1),
                            input=field_an)
    field_an2 = cdo.remapnn("lon={0}_lat={1}".format(lon2, lat2),
                            input=field_an)

    # Rolling average over months = months_window
    field_an1_ma = cdo.runmean("{0}".format(months_window), input=field_an1)
    field_an2_ma = cdo.runmean("{0}".format(months_window), input=field_an2)

    # Evaluate average and std for the station based difference
    diff_ma = cdo.sub(input=[field_an1_ma, field_an2_ma])
    mean_ma = cdo.timmean(input=diff_ma)
    std_ma = cdo.timstd(input=diff_ma)

    # Evaluate the index
    sub_indx = cdo.sub(input=[diff_ma, mean_ma])
    indx = cdo.div(input=[sub_indx, std_ma], returnXDataset=True)

    logger.info('Index evaluation completed')

    # Cleaning the temporary directory
    logger.debug('Cleaning temporary directory')
    cdo.cleanTempDir()

    return indx


def regional_mean_cdo(infile, namelist, telecname, months_window=3,
                      loglevel='WARNING'):
    """
    Evaluate regional mean for a teleconnection with cdo bindings.

    Args:
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3
        loglevel (str, opt):      log level, default is WARNING

    Returns:
        indx (DataArray): standardized station based index
    """
    logger = log_configure(loglevel, 'cdo regional mean index')
    logger.info('Evaluating regional mean index for %s with cdo bindings',
                telecname)

    logger.debug('Input file: %s', infile)
    logger.debug('Teleconnection name: %s', telecname)
    logger.debug('Months window: %s', months_window)
    logger.debug('Opening cdo class instance')
    cdo = Cdo()

    # Evaluate box coordinates
    data = xr.open_dataset(infile)
    if data.lon.min() < 0:
        logger.debug('Data longitudes are -180-180, not converting')
        lonW = namelist[telecname]['lonW']
        lonE = namelist[telecname]['lonE']
    else:
        logger.info('Data longitudes are 0-360, '
                    'converting teleconnection coords')
        lonW = lon_180_to_360(namelist[telecname]['lonW'])
        lonE = lon_180_to_360(namelist[telecname]['lonE'])

    latN = namelist[telecname]['latN']
    latS = namelist[telecname]['latS']

    logger.debug('Box coordinates: lonW = %s, lonE = %s, latN = %s, latS = %s',
                 lonW, lonE, latN, latS)

    # 2. -- Select field in the box and evaluate the average
    field_sel = cdo.sellonlatbox('{},{},{},{}'.format(lonW, lonE, latS, latN),
                                 input=infile)
    field_mean = cdo.fldmean(input=field_sel)

    # 3. -- Evaluate the value with the months window --
    indx = cdo.runmean("{0}".format(months_window), input=field_mean,
                       returnXDataset=True)

    logger.info('Index evaluation completed')

    # 4. -- Cleaning the temporary directory --
    logger.debug('Cleaning temporary directory')
    cdo.cleanTempDir()

    return indx


def regional_anomalies_cdo(infile, namelist, telecname, months_window=3,
                           loglevel='WARNING'):
    """
    Evaluate regional mean for a teleconnection with cdo bindings.

    Args:
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3
        loglevel (str, opt):      log level, default is WARNING

    Returns:
        indx (DataArray): standardized station based index
    """
    logger = log_configure(loglevel, 'cdo regional anomalies')
    logger.info('Evaluating regional anomalies for %s with cdo bindings',
                telecname)

    logger.debug('Input file: %s', infile)
    logger.debug('Teleconnection name: %s', telecname)
    logger.debug('Months window: %s', months_window)
    logger.debug('Opening cdo class instance')
    cdo = Cdo()

    # Evaluate box coordinates
    data = xr.open_dataset(infile)
    if data.lon.min() < 0:
        logger.debug('Data longitudes are -180-180, not converting')
        lonW = namelist[telecname]['lonW']
        lonE = namelist[telecname]['lonE']
    else:
        logger.info('Data longitudes are 0-360, '
                    'converting teleconnection coords')
        lonW = lon_180_to_360(namelist[telecname]['lonW'])
        lonE = lon_180_to_360(namelist[telecname]['lonE'])

    latN = namelist[telecname]['latN']
    latS = namelist[telecname]['latS']

    logger.info('Box coordinates: lonW = %s, lonE = %s, latN = %s, latS = %s',
                lonW, lonE, latN, latS)

    # Select field in the box and evaluate the average
    field_sel = cdo.sellonlatbox('{},{},{},{}'.format(lonW, lonE, latS, latN),
                                 input=infile)
    field_mean = cdo.fldmean(input=field_sel)

    # Evaluate the value with the months window --
    indx = cdo.runmean("{0}".format(months_window), input=field_mean,
                       returnXDataset=True)

    # Evaluate the anomalies

    # TO DO

    # Cleaning the temporary directory
    logger.debug('Cleaning temporary directory')
    cdo.cleanTempDir()

    return indx


def cdo_station_based_comparison(infile, namelist, telecname, months_window=3,
                                 rtol=1.e-5, atol=1.e-8, loglevel='WARNING'):
    """
    Compare station based index evaluated with cdo and libraries from index.py

    Args:import index
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3
        rtol (float, opt):        relative tolerance, default is 1.e-5
        atol (float, opt):        absolute tolerance, default is 1.e-8
        loglevel (str, opt):      log level, default is WARNING
    """
    logger = log_configure(loglevel, 'cdo station based comparison')
    logger.info('Comparing station based index for %s with cdo bindings',
                telecname)

    fieldname = namelist[telecname]['field']
    logger.debug('Field name: %s', fieldname)

    logger.info('Evaluating index with cdo bindings')
    index_cdo = station_based_cdo(infile, namelist, telecname,
                                  months_window=months_window,
                                  loglevel=loglevel)

    logger.info('Evaluating index with AQUA library')
    field = xr.open_mfdataset(infile)[fieldname]
    index_lib = station_based_index(field, namelist, telecname,
                                    months_window=months_window,
                                    loglevel=loglevel)

    logger.debug('Adapting index for comparison')
    index_cdo = index_cdo.squeeze(['lat', 'lon'], drop=True)

    logger.debug('Performing assert_allclose() test')
    np.testing.assert_allclose(index_lib.values,
                               index_cdo[namelist[telecname]['field']].values,
                               rtol=rtol, atol=atol)
    logger.info('Test passed')


def cdo_regional_mean_comparison(infile, namelist, telecname, months_window=3,
                                 rtol=1.e-5, atol=1.e-8, loglevel='WARNING'):
    """
    Compare regional mean evaluated with cdo and libraries from index.py

    Args:import index
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3
        rtol (float, opt):        relative tolerance, default is 1.e-5
        atol (float, opt):        absolute tolerance, default is 1.e-8
        loglevel (str, opt):      log level, default is WARNING

    Returns:
        None                      returns None and perform assert_allclose().
    """
    logger = log_configure(loglevel, 'cdo regional mean comparison')
    logger.info('Comparing regional mean for %s with cdo bindings',
                telecname)

    fieldname = namelist[telecname]['field']
    logger.debug('Field name: %s', fieldname)

    logger.info('Evaluating average with cdo bindings')
    avg_cdo = regional_mean_cdo(infile, namelist, telecname,
                                months_window=months_window,
                                loglevel=loglevel)

    logger.info('Evaluating average with library')
    field = xr.open_mfdataset(infile)[fieldname]
    avg_lib = regional_mean_index(field, namelist, telecname,
                                  months_window=months_window,
                                  loglevel=loglevel)

    logger.debug('Adapting index for comparison')
    avg_cdo = avg_cdo.squeeze(['lat', 'lon'], drop=True)

    logger.debug('Performing assert_allclose() test')
    np.testing.assert_allclose(avg_lib.values,
                               avg_cdo[namelist[telecname]['field']].values,
                               rtol=rtol, atol=atol)
