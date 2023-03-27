#!/bin/bash

# Activate base environment
conda activate base

# Run your Python script
python conda_req_for_yml_UPD.py/conda_req_for_yml.py

# Install packages required for the environment
conda env create -v  --file environment.yml
# mamba env create -f environment.yml 

# Activate the new environment
conda activate aqua

