#!/bin/bash
#SBATCH --partition=compute
#SBATCH --job-name=regrid_test_mon_8
#SBATCH --output=output_mon_8.txt
#SBATCH --error=error_mon_8.txt
#SBATCH --account=bb1153
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=8:00:00
set -e

# find mamba/conda (to be refined)
whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
source $whereconda/etc/profile.d/conda.sh

# activate conda environment
conda activate aqua

# set the number of dask workers
workers=8

# run the Python script
# -d to create the files (otherwise only inspect the catalogs and tests)
# -o to overwrite the files
./lra-regridder.py --config config_lra.yml -w ${workers} -d
