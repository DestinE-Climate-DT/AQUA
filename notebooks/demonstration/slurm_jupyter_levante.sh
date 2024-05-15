#!/bin/bash
#SBATCH --partition=compute
#SBATCH --job-name=jupyter
#SBATCH --output=output_%j.out
#SBATCH --error=output_%j.err
#SBATCH --account=bb1153
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=08:00:00
#SBATCH --mem=0 
set -e

# export AQUA = PATH_TO_AQUA_repo

cd $AQUA/home/b/b382397/AQUA/diagnostics/ocean3d/cli
# conda activate aqua

