'''
This module contains functions to compare and test the teleconnections
libraries with similar procedures done with cdo bindings.
'''
import cdo
from tools import *

def station_based_cdo(infile, namelist, telecname, months_window=3):
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
    field_ma = cdo.monmean(input=infile)
    field_ma_av = cdo.timmean(input=field_ma)
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
    indx = cdo.div(input=[sub_indx,std_ma])
    
    return indx