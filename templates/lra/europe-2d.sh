#!/bin/bash
#SBATCH --partition=small
#SBATCH --job-name=2d-europe
#SBATCH --output=2d_europe_%j.out
#SBATCH --error=2d_europe_%j.err
#SBATCH --account=project_462000911
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=02:00:00
#SBATCH --mem=64G 
set -e

# set the number of dask workers
# should be equal to the number of the total tasks available
workers=8

# run the Python script
# -c to specify the configuration file
# -f to use the fixer on data
# -d to perform a dry run (to check generated lra)
# -o to overwrite existing lra
# -l to set the log level (default is WARNING)
aqua lra --config lra_europe_prec-lowres.yaml -w 8 -d -l DEBUG
