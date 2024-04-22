#!/bin/bash
#SBATCH --partition=compute # compute is suggested on levante
#SBATCH --job-name=timeseries
#SBATCH --output=timeseries_%j.out
#SBATCH --error=timeseries_%j.err
#SBATCH --account=bb1153
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=128
#SBATCH --time=01:00:00
#SBATCH --mem=0
set -e

whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
source $whereconda/etc/profile.d/conda.sh
mamba activate aqua

configfile="${AQUA}/diagnostics/global_time_series/deliverable/config_deliverable.yaml"
scriptfile="${AQUA}/diagnostics/global_time_series/cli/cli_global_time_series.py"
outputdir="${AQUA}/diagnostics/global_time_series/deliverable/output"
workers=8

echo "Running the global time series diagnostic with $workers workers"
echo "Script: $scriptfile"
echo "Config: $configfile"

python $scriptfile -c $configfile -l DEBUG -n $workers --outputdir $outputdir
