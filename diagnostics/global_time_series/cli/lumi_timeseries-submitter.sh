#!/bin/bash
#SBATCH --partition=debug # compute is suggested on levante
#SBATCH --job-name=timeseries_debug
#SBATCH --output=timeseries_%j.out
#SBATCH --error=timeseries_%j.err
#SBATCH --account=project_465000454
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=128
#SBATCH --time=00:29:00
#SBATCH --mem=0
set -e

# run the Python script, both for the atmosphere and the ocean
# See README.md for more information on how to run the diagnostics and the options available

scriptpy="$AQUA/diagnostics/global_time_series/cli/cli_global_time_series.py"
config="$AQUA/diagnostics/global_time_series/cli/config_time_series_atm.yaml"
loglevel="DEBUG"
workers=32
model="IFS-NEMO"
exp="ssp370"

# run the diagnostics
python $scriptpy --config $config -l $loglevel -n $workers --model $model --exp $exp
