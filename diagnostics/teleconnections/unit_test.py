'''
This script contains usage of library functions in order
to test if the modifications in the teleconnections 
diagnostic 
'''
import argparse
from cdotesting import *
from index import *
from tools import *

# 1. -- Configuration command line arguments -
parser = argparse.ArgumentParser()
parser.add_argument('--machine',type=str,default='wilma',
                    help='machine name')
parser.add_argument('--diagname',type=str,default='teleconnections',
                    help='diagnostic name')

args = parser.parse_args()

machine = args.machine
diagname = args.diagname

# 2. -- Loading yaml files --
namelist = load_namelist(diagname)
config = load_config(machine)

# 3. -- NAO testing --
telecname = 'NAO'
infile = config[diagname][telecname]['input']

cdo_regional_mean_comparison(infile,namelist,telecname,months_window=1)
cdo_regional_mean_comparison(infile,namelist,telecname,months_window=3)
cdo_regional_mean_comparison(infile,namelist,telecname,months_window=5)

# 4. -- ENSO testing --
telecname = 'ENSO'
infile = config[diagname][telecname]['input']

cdo_regional_mean_comparison(infile,namelist,telecname,months_window=1)
cdo_regional_mean_comparison(infile,namelist,telecname,months_window=3)
cdo_regional_mean_comparison(infile,namelist,telecname,months_window=5)

print('Test passed!')