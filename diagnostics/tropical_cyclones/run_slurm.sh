#!/bin/bash
# The path is changed to mine working directory. Do not forget to change it back. 
DIR=/work/bb1153/b382267/AQUA/diagnostics/tropical_cyclones
# Begin of section with executable commands

set -e
sacctmgr -s show user name=$USER
# find mamba/conda (to be refined)
#whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
whereconda=/home/b/b382216/work/mambaforge

#echo $whereconda/etc/profile.d/conda.sh
source $whereconda/etc/profile.d/conda.sh
conda activate TCs

# The script, which contains the job submitter inside
python $DIR/script_with_dask_cluster.py 
# Good luck! 


