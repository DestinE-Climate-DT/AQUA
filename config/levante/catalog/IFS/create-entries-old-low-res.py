#!/usr/bin/env python3
"""Small script to add into the catalog the old-low resolution version for IFS NextGEMS data"""

import os
import copy
import sys
import yaml
import xarray as xr
from aqua.util import load_yaml

RESO = '2.8km'

if RESO == '9km':
    expname = 'tco1279-orca025'
elif RESO == '4km':
    expname = 'tco2559-ng5'
elif RESO == '2.8km':
    expname = 'tco3999-ng5'
else:
    sys.exit()

# Set the path to the folder containing the .nc files
folder_path = os.path.join('/work/bm1235/a270046/cycle2-sync/monthly_means', RESO)

fff = f'{expname}.yaml'
yaml_file = load_yaml(fff)
basedict = {
    'old-low-res': {
        'driver': 'netcdf',
        'args': {
            'chunks': {}
            }
        }
}

# Use os.listdir() to get a list of all files in the folder
all_files = os.listdir(folder_path)

# Use a for loop to iterate through the list and find all .nc files
nc_files = []
for file in all_files:
    if file.endswith('.nc'):
        nc_files.append(file)

# Use a second for loop to print the list of .nc files to the screen
list21 = []
list15 = []
list20 = []
for nc_file in sorted(nc_files):
    full= os.path.join(folder_path, nc_file)
    xfield = xr.open_dataset(full)
    # this is a weird check to have all on the same time axis, some variable is not
    if xfield.time[0].dt.day.values == 21:
        list21.append(full)
    elif  xfield.time[0].dt.day.values == 15:
        list15.append(full)
    elif xfield.time[0].dt.day.values == 20:
        list20.append(full)

for source in ['low-res-bulk', 'low-res-std', 'low-res-extra']:
    if source == 'low-res-bulk':
        newlist = copy.deepcopy(list20)
    elif source == 'low-res-std':
        newlist = copy.deepcopy(list15)
    elif source == 'low-res-extra':
        newlist = copy.deepcopy(list21)

    print(newlist)

    basedict['old-low-res']['args']['urlpath'] = newlist
    #print(basedict)
    yaml_file['sources'][source] = copy.deepcopy(basedict['old-low-res'])
    
with open(fff, 'w', encoding='utf-8') as file:
    yaml.dump(yaml_file, file, sort_keys=False)

with open(fff, 'w', encoding='utf-8') as file:
    yaml.dump(yaml_file, file, sort_keys=False)
