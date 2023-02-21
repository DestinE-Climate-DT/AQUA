#!/bin/bash
#SBATCH --partition=compute
#SBATCH --job-name=regrid_test_mon_8
#SBATCH --output=output_mon_8.txt
#SBATCH --error=error_mon_8.txt
#SBATCH --account=bb1153
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=8:00:00
set -e

# find mamba/conda (to be refined)
whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
source $whereconda/etc/profile.d/conda.sh

# activate conda environment
conda activate aqua

# config
model='IFS'
exp='tco2559-ng5'
source='ICMGG_atm2d'
res='r100'
freq='mon'
workers=8

# run the Python script
./regridder.py -m ${model} -e ${exp} -s ${source} -r ${res} -f ${freq} -w ${workers} -o /work/bb1153/b382289/IFSlowres
