'''
This module contains functions to compare and test the teleconnections
libraries with similar procedures done with cdo bindings.
'''
import math
import numpy as np
import xarray as xr
from cdo import *
from index import *
from tools import *

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
    field_an1 = cdo.remapnn("lon={0}_lat={1}".format(lon1,lat1),input=field_an)
    field_an2 = cdo.remapnn("lon={0}_lat={1}".format(lon2,lat2),input=field_an)

    # 4. -- Rolling average over months = months_window --
    field_an1_ma = cdo.runmean("{0}".format(months_window),input=field_an1)
    field_an2_ma = cdo.runmean("{0}".format(months_window),input=field_an2)

    # 5. -- Evaluate average and std for the station based difference --
    diff_ma = cdo.sub(input=[field_an1_ma, field_an2_ma])
    mean_ma = cdo.timmean(input=diff_ma)
    std_ma = cdo.timstd(input=diff_ma)

    # 6. -- Evaluate the index and rename the variable in the DataArray --
    sub_indx = cdo.sub(input=[diff_ma,mean_ma])

    ofile = "temp.nc" #solve directory with output
    cdo.div(input=[sub_indx,std_ma],output=ofile)

    indx = xr.open_dataset(ofile)
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
    field_sel = cdo.sellonlatbox('{},{},{},{}'.format(lonW,lonE,latS,latN),
                                 input=infile)
    field_mean = cdo.fldmean(input=field_sel)

    # 3. -- Evaluate the value with the months window and save as xarray
    ofile = "temp.nc"
    indx = cdo.runmean("{0}".format(months_window),input=field_mean,output=ofile) #return xdataset

    indx = xr.open_dataset(ofile)
    cdo.cleanTempDir()

    return indx

def cdo_station_based_comparison(infile, namelist, telecname, months_window=3,
                                 rtol=1.e-5,atol=1.e-8,ret_diff=False):
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
        ret_diff (bool,opt):      return difference instead of assert

    Returns:
        None or xarray.DataArray: if ret_diff is True, returns an xarray.DataArray
                                  with the difference between the two methods.
                                  If False, returns None and perform assert_allclose().
    """
    fieldname = namelist[telecname]['field']

    # 1. -- cdo index evaluation --
    index_cdo = station_based_cdo(infile,namelist,telecname,months_window=months_window)

    # 2. -- library index evaluation --
    field = xr.open_mfdataset(infile)[fieldname]
    index_lib = station_based_index(field,namelist,telecname,months_window=months_window)
    
    # 3. -- adapt index for comparison --
    index_lib = index_lib.dropna(dim='time').drop_vars('month')
    index_cdo = index_cdo.drop_vars('lon').drop_vars('lat')
    index_cdo = index_cdo.squeeze(['lat','lon'],drop=True)

    # 4. -- reurm difference or perform the asser_allclose() test
    if ret_diff:
        return index_lib-index_cdo[namelist[telecname]['field']]
    else:
        xr.testing.assert_allclose(index_lib,index_cdo[namelist[telecname]['field']],
                                   rtol=rtol,atol=atol)
        return


def cdo_regional_mean_comparison(infile, namelist, telecname, months_window=3,
                                 rtol=1.e-5,atol=1.e-8,ret_diff=False):
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
        ret_diff (bool,opt):      return difference instead of assert

    Returns:
        None or xarray.DataArray: if ret_diff is True, returns an xarray.DataArray
                                  with the difference between the two methods.
                                  If False, returns None and perform assert_allclose().
    """
    fieldname = namelist[telecname]['field']

    # 1. -- cdo average evaluation --
    avg_cdo = regional_mean_cdo(infile,namelist,telecname,months_window=months_window)

    # 2. -- library average evaluation --
    field = xr.open_mfdataset(infile)[fieldname]
    avg_lib = regional_mean_index(field,namelist,telecname,months_window=months_window)

    # 3. -- adapt index for comparison --
    avg_lib = avg_lib.dropna(dim='time')
    avg_cdo = avg_cdo.drop_vars('lon').drop_vars('lat')
    avg_cdo = avg_cdo.squeeze(['lat','lon'],drop=True)
    avg_cdo = avg_cdo.drop_vars('time_bnds')

    # 4. -- reurm difference or perform the asser_allclose() test
    if ret_diff:
        return avg_lib-avg_cdo[namelist[telecname]['field']]
    else:
        xr.testing.assert_allclose(avg_lib,avg_cdo[namelist[telecname]['field']],
                                   rtol=rtol,atol=atol)
        return