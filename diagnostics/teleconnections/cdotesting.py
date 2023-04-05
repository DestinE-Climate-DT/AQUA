'''
This module contains functions to compare and test the teleconnections
libraries with similar procedures done with cdo bindings.
'''
import numpy as np
import xarray as xr
from cdo import Cdo
from index import station_based_index, regional_mean_index
from tools import lon_180_to_360


def station_based_cdo(infile, namelist, telecname, months_window=3):
    """
    Evaluate station based index for a teleconnection with cdo
    bindings.

    Args:
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3

    Returns:
        indx (DataArray): standardized station based index
    """
    cdo = Cdo()

    # 1. -- Monthly field average and anomalies--
    field_ma = cdo.monmean(input=infile)
    field_ma_av = cdo.ymonmean(input=field_ma)
    field_an = cdo.sub(input=[field_ma, field_ma_av])

    # 2. -- Acquiring latitude and longitude of stations --
    lon1 = lon_180_to_360(namelist[telecname]['lon1'])
    lat1 = namelist[telecname]['lat1']
    lon2 = lon_180_to_360(namelist[telecname]['lon2'])
    lat2 = namelist[telecname]['lat2']

    # 3. -- Extracting field data at the acquired coordinates --
    field_an1 = cdo.remapnn("lon={0}_lat={1}".format(lon1, lat1),
                            input=field_an)
    field_an2 = cdo.remapnn("lon={0}_lat={1}".format(lon2, lat2),
                            input=field_an)

    # 4. -- Rolling average over months = months_window --
    field_an1_ma = cdo.runmean("{0}".format(months_window), input=field_an1)
    field_an2_ma = cdo.runmean("{0}".format(months_window), input=field_an2)

    # 5. -- Evaluate average and std for the station based difference --
    diff_ma = cdo.sub(input=[field_an1_ma, field_an2_ma])
    mean_ma = cdo.timmean(input=diff_ma)
    std_ma = cdo.timstd(input=diff_ma)

    # 6. -- Evaluate the index --
    sub_indx = cdo.sub(input=[diff_ma, mean_ma])
    indx = cdo.div(input=[sub_indx, std_ma], returnXDataset=True)

    # 7. -- Cleaning the temporary directory --
    cdo.cleanTempDir()

    return indx


def regional_mean_cdo(infile, namelist, telecname, months_window=3):
    """
    Evaluate regional mean for a teleconnection with cdo bindings.

    Args:
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3

    Returns:
        indx (DataArray): standardized station based index
    """
    cdo = Cdo()

    # 1. -- Evaluate box coordinates --
    lonW = lon_180_to_360(namelist[telecname]['lonW'])
    latN = namelist[telecname]['latN']
    lonE = lon_180_to_360(namelist[telecname]['lonE'])
    latS = namelist[telecname]['latS']

    # 2. -- Select field in the box and evaluate the average
    field_sel = cdo.sellonlatbox('{},{},{},{}'.format(lonW, lonE, latS, latN),
                                 input=infile)
    field_mean = cdo.fldmean(input=field_sel)

    # 3. -- Evaluate the value with the months window --
    indx = cdo.runmean("{0}".format(months_window), input=field_mean,
                       returnXDataset=True)

    cdo.cleanTempDir()

    return indx


def regional_anomalies_cdo(infile, namelist, telecname, months_window=3):
    """
    Evaluate regional mean for a teleconnection with cdo bindings.

    Args:
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3

    Returns:
        indx (DataArray): standardized station based index
    """
    cdo = Cdo()

    # 1. -- Evaluate box coordinates --
    lonW = lon_180_to_360(namelist[telecname]['lonW'])
    latN = namelist[telecname]['latN']
    lonE = lon_180_to_360(namelist[telecname]['lonE'])
    latS = namelist[telecname]['latS']

    # 2. -- Select field in the box and evaluate the average
    field_sel = cdo.sellonlatbox('{},{},{},{}'.format(lonW, lonE, latS, latN),
                                 input=infile)
    field_mean = cdo.fldmean(input=field_sel)

    # 3. -- Evaluate the value with the months window --
    indx = cdo.runmean("{0}".format(months_window), input=field_mean,
                       returnXDataset=True)

    cdo.cleanTempDir()

    return indx


def cdo_station_based_comparison(infile, namelist, telecname, months_window=3,
                                 rtol=1.e-5, atol=1.e-8):
    """
    Compare station based index evaluated with cdo and libraries from index.py

    Args:import index
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3
        rtol (float, opt):        relative tolerance
        atol (float, opt):        absolute tolerance

    Returns:
        None                      returns None and perform assert_allclose().
    """
    fieldname = namelist[telecname]['field']

    # 1. -- cdo index evaluation --
    index_cdo = station_based_cdo(infile, namelist, telecname,
                                  months_window=months_window)

    # 2. -- library index evaluation --
    field = xr.open_mfdataset(infile)[fieldname]
    index_lib = station_based_index(field, namelist, telecname,
                                    months_window=months_window)

    # 3. -- adapt index for comparison --
    index_cdo = index_cdo.squeeze(['lat', 'lon'], drop=True)

    # 4. -- perform the asser_allclose() test
    np.testing.assert_allclose(index_lib.values,
                               index_cdo[namelist[telecname]['field']].values,
                               rtol=rtol, atol=atol)
    return


def cdo_regional_mean_comparison(infile, namelist, telecname, months_window=3,
                                 rtol=1.e-5, atol=1.e-8):
    """
    Compare regional mean evaluated with cdo and libraries from index.py

    Args:import index
        infile:                   path to nc file containing the field to
                                  evaluate the index
        namelist:                 teleconnection yaml infos
        telecname (str):          name of the teleconnection to be evaluated
        months_window (int, opt): months for rolling average, default is 3
        rtol (float, opt):        relative tolerance
        atol (float, opt):        absolute tolerance

    Returns:
        None                      returns None and perform assert_allclose().
    """
    fieldname = namelist[telecname]['field']

    # 1. -- cdo average evaluation --
    avg_cdo = regional_mean_cdo(infile, namelist, telecname,
                                months_window=months_window)

    # 2. -- library average evaluation --
    field = xr.open_mfdataset(infile)[fieldname]
    avg_lib = regional_mean_index(field, namelist, telecname,
                                  months_window=months_window)

    # 3. -- adapt index for comparison --
    avg_cdo = avg_cdo.squeeze(['lat', 'lon'], drop=True)

    # 4. -- perform the asser_allclose() test
    np.testing.assert_allclose(avg_lib.values,
                               avg_cdo[namelist[telecname]['field']].values,
                               rtol=rtol, atol=atol)

    return
