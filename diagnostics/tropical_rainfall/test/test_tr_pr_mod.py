"""Test checking if diagnostic can be load/read"""

import pytest
import numpy as np
from aqua import Reader, catalogue

import os
import re 
import sys
import setuptools 

with os.popen("pwd ") as f:
    _pwd = f.readline()
pwd = re.split(r'[\n]', _pwd)[0]

sys.path.append(str(pwd)+'/../')
#import src.shared_func

from  src.shared_func import xarray_attribute_update, data_size
from  src.tr_pr_mod import TR_PR_Diagnostic
diag = TR_PR_Diagnostic(num_of_bins = 15, first_edge = 0, width_of_bin = 1*10**(-4)/15)

"""Checking that histogram contain the data"""
configdir = '../../../config/'

@pytest.fixture(params=[
    Reader(model="ICON", exp="ngc2009",  configdir=configdir, source="atm_2d_ml_R02B09", regrid="r200"),
    Reader(model="IFS", exp="tco3999-ng5", source="ICMGG_atm2d",configdir=configdir),
    Reader(model="MSWEP", exp="past", source="monthly",configdir=configdir)
])

def reader(request):
    """Reader instance fixture"""
    data = request.param
    retrieved = data.retrieve()
    return data, retrieved  



@pytest.fixture
def check_precipitation_in_data(reader):
    ds, ret = reader
    try:
        ret['tprate']
        return True
    except KeyError:
        print('not contain the precipitation rate')
        return False


def test_check_precipitation_in_data(check_precipitation_in_data):
    assert check_precipitation_in_data == True



def test_not_null_hist(reader):
    ds, ret = reader
    try:
        ret_tprate =  ret['tprate'][10:11,:,:]
    except IndexError:
        ret_tprate =  ret['tprate'][10:11,:] 
    ret_tprate = xarray_attribute_update(ret_tprate, ret)
    ret_tprate = ret_tprate.compute()

    hist_xarray = diag.hist1d_fast(ret_tprate,  preprocess = False)
    assert sum(hist_xarray) > 0 

def test_hist_size(reader):
    ds, ret = reader
    try:
        ret_tprate =  ret['tprate'][10:11,:,:]
    except IndexError:
        ret_tprate =  ret['tprate'][10:11,:] 
    ret_tprate = xarray_attribute_update(ret_tprate, ret)
    ret_tprate = ret_tprate.compute()

    hist_xarray = diag.hist1d_fast(ret_tprate,  preprocess = False)
    assert data_size(ret_tprate) >= sum(hist_xarray)  
    print(data_size(ret_tprate), sum(hist_xarray.values))


def test_mean_prec_value(reader):
    ds, ret = reader
    try:
        ret_tprate =  ret['tprate'][10:11,:,:]
    except IndexError:
        ret_tprate =  ret['tprate'][10:11,:] 
    ret_tprate = xarray_attribute_update(ret_tprate, ret)
    ret_tprate = ret_tprate.compute()
    mean_val = diag.median_per_timestep(data = ret_tprate) #, variable_1 = 'tprate', s_year = ret['time.year'][0], s_month=1, f_month=1)
    print([mean_val, mean_val[0]])
    assert mean_val > 0  
