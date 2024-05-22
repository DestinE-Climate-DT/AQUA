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
conda activate aqua

configfile_atm="${AQUA}/diagnostics/global_time_series/cli/config_time_series_oce.yaml"
configfile_oce="${AQUA}/diagnostics/global_time_series/cli/config_time_series_atm.yaml"
configfile_cycle="${AQUA}/diagnostics/global_time_series/cli/config_seasonal_cycles_atm.yaml"
scriptfile="${AQUA}/diagnostics/global_time_series/cli/cli_global_time_series.py"
outputdir="${AQUA}/diagnostics/global_time_series/cli/output"
workers=32

loglevel="INFO"

# run the diagnostics
#python $scriptfile -c $configfile_cycle -l $loglevel -n $workers --outputdir $outputdir
python $scriptfile -c $configfile_atm -l $loglevel -n $workers --outputdir $outputdir
python $scriptfile -c $configfile_oce -l $loglevel -n $workers --outputdir $outputdir
