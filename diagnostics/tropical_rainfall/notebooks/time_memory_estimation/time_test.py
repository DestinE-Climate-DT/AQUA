import sys
import os
import re

with os.popen("pwd ") as f:
    _pwd = f.readline()

pwd = re.split(r'[\n]', _pwd)[0]

sys.path.append(str(pwd)+'/../../')
import src.shared_func
from  src.shared_func import data_size
import src.tr_pr_mod
from  src.tr_pr_mod import TR_PR_Diagnostic

configdir = '../../../../config/'
diagname  = 'tr_pr'
machine   = 'levante'

import aqua

from aqua import Reader
from aqua.reader import catalogue

reader = Reader(model="ICON", exp="ngc2009",  configdir=configdir, source="atm_2d_ml_R02B09", regrid="r200")
ICON_2009_reg = reader.retrieve()

ICON_chunk = ICON_2009_reg['tprate'][0:20,:]
TEST_SIZE = data_size(ICON_chunk)

diag = TR_PR_Diagnostic()

diag.num_of_bins = 15
diag.first_edge = 0
diag.width_of_bin = 1*10**(-4)/diag.num_of_bins

last_edge = diag.first_edge  + diag.num_of_bins*diag.width_of_bin

diag.hist1d_fast(ICON_chunk,  preprocess = False)
