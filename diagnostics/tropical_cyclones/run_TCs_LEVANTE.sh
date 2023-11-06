#!/bin/bash
#SBATCH --qos=bb1153
#SBATCH --partition=compute
#SBATCH -A bb1153
#SBATCH -n 4
#SBATCH --cpus-per-task=64
#SBATCH --mem=50G
#SBATCH -t 6-00:00:00          # walltime
#SBATCH --mail-type=FAIL       # Notify user by email in case of job failure
#SBATCH --output=/home/b/b382216/AQUA/diagnostics/tropical_cyclones/slurm_jobs/TCs_%j.out    # File name for standard output
#SBATCH --error=/home/b/b382216/AQUA/diagnostics/tropical_cyclones/slurm_jobs/TCs_%j.err     # File name for errors


DIR=/home/b/b382216/AQUA/diagnostics/tropical_cyclones

# Begin of section with executable commands

set -e
sacctmgr -s show user name=$USER
# find mamba/conda (to be refined)
#whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
whereconda=/home/b/b382216/work/mambaforge

#echo $whereconda/etc/profile.d/conda.sh
source $whereconda/etc/profile.d/conda.sh
conda activate TCs
python $DIR/tropical_cyclones_slurm.py


