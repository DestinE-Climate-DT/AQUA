#!/bin/bash
#SBATCH -p compute
#SBATCH -A bb1153
#SBATCH -n 1
#SBATCH --cpus-per-task=256
#SBATCH --mem=50G
#SBATCH -t 04:30:50
#SBATCH --mail-type=FAIL       # Notify user by email in case of job failure
#SBATCH --output=/home/b/b382216/AQUA/diagnostics/tempest/slurm_jobs/tempest_TCs.o%j    # File name for standard output
#SBATCH --error=/home/b/b382216/AQUA/diagnostics/tempest/slurm_jobs/tempest_TCs.e%j


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


