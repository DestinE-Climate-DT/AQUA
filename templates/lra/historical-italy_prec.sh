#!/bin/bash
#SBATCH --partition=small
#SBATCH --job-name=LRA_italy
#SBATCH --output=historical_italy_%j.out
#SBATCH --error=historical_italy_%j.err
#SBATCH --account=project_465000454
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=24:00:00
#SBATCH --mem=200G 
set -e

# set the number of dask workers
# should be equal to the number of the total tasks available
workers=4

# run the Python script
# -c to specify the configuration file
# -f to use the fixer on data
# -d to perform a dry run (to check generated lra)
# -o to overwrite existing lra
# -l to set the log level (default is WARNING)
aqua lra --config lra_italy_prec-historical.yaml -w 4 -d -l INFO
