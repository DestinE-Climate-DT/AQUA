#!/bin/bash

#exec bash

manager=conda 


$manager init

# Activate base environment
$manager activate base

# Run your Python script
python3 /work/users/nnazarova/AQUA/requirements/conda_req_for_yml.py


#export PATH="/anaconda/bin:$PATH"

# Install packages required for the environment
$manager env create -v  --file /work/users/nnazarova/AQUA/environment.yml
$manager env create -f environment.yml 

# Activate the new environment
$manager activate aqua

