#!/bin/bash


manager=conda 

current_dir=$(pwd)

# Activate base environment
# $manager activate base

# Python script deactivate and delete previous aqua environment if necessary 
python3 ${current_dir}/requirements/conda_req_for_yml.py

python3 ${current_dir}/requirements/check_aqua_env.py

# Install packages required for the environment
$manager env create -v  -f environment.yml 

# Activate the new environment
source activate aqua
$manager  activate aqua
