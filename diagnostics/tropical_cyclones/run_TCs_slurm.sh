#!/bin/bash

DIR=/home/b/b382216/AQUA/diagnostics/tempest

# Begin of section with executable commands

set -e
sacctmgr -s show user name=$USER
# find mamba/conda (to be refined)
#whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
whereconda=/home/b/b382216/work/mambaforge

#echo $whereconda/etc/profile.d/conda.sh
source $whereconda/etc/profile.d/conda.sh
conda activate TCs
python $DIR/TCs_intake_slurm.py


