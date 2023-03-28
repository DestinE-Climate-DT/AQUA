#!/bin/bash

# This script is tentative way to incorporate the ECmean4 package 
# within AQUA: it is incomplete and only based on mamba, but has been tested 

# flag to clean environ,ent
do_clean=true

# find mamba/conda (to be refined)
whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
source $whereconda/etc/profile.d/conda.sh

# check if you want to remove enviornment and repo
if [[ $do_clean == true ]] ; then
         echo "Removing the env..."
        rm -rf ECmean4
        mamba env remove -n aqua-ecmean
fi

sleep 5

# create the env
if [[ -z $(mamba env list | grep aqua-ecmean) ]] ; then
        echo "Creating... the env..."
        mamba env create -f aqua-ecmean.yml
fi

sleep 5

# clone
if [[ ! -d ECmean4 ]] ; then
        echo "Cloning..."
        git clone https://github.com/oloapinivad/ECmean4.git
fi

sleep 5

# checkout installable version: we might want to change it 
# to have it running on 
cd ECmean4
git fetch
git checkout main
git pull

# create env and install ECmean4
conda activate aqua-ecmean
pip install -e .
conda deactivate

