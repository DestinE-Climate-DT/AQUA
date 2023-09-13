#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Script to generate a json file for zarr files for AVISO dataset
From Nikolay Koldunov, used on Lumi, can be adapted for other sources
You will need to install kerchunk and h5py (via pip) to use it within AQUA
'''

import json
import glob
import os
import tqdm
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr
import fsspec

# where the files are
root_directory = "/pfs/lustrep3/projappl/project_465000454/data/AQUA/datasets/AVISO/data"

# where the output will be
output_filename = "/pfs/lustrep3/projappl/project_465000454/data/AQUA/datasets/AVISO/json/AVISO.json"
files = "dt_global_twosat_phy_l4*.nc"

# Initialize an empty list to store the file paths
file_paths = []

print('Generate file paths...')
# Iterate over all year and month directories
for year in range(1993, 2023):  # Replace YYYY with the start year and n with the number of years
    for month in range(1, 13):
        # Create a pattern to match all daily files in the current year and month directory
        pattern = os.path.join(root_directory, f"{year}/{month:02d}/*")

        # Use glob to find all files matching the pattern
        files = glob.glob(pattern)
        files.sort()

        # Add the absolute paths to the list
        file_paths.extend([os.path.abspath(file) for file in files])


# open files with kerchunk as hdf
print('Create hdf files...')
singles = []
for u in tqdm.tqdm(file_paths):
    with fsspec.open(u) as inf:
        h5chunks = kerchunk.hdf.SingleHdf5ToZarr(inf, u, inline_threshold=100)
        singles.append(h5chunks.translate())

# concatenate as a single zarr 
print('Create single zarr and translate...')
mzz = MultiZarrToZarr(
     singles,
    concat_dims=["time"]
)

# conver to json
out = mzz.translate()

#dump to fle
print('Create json file...')
if os.path.exists(output_filename):
  os.remove(output_filename)
with open(output_filename, "w") as outfile:
    json.dump(out, outfile)