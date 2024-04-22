#!/bin/bash
#SBATCH --partition=compute # compute is suggested on levante
#SBATCH --job-name=teleconnections
#SBATCH --output=teleconnections_%j.out
#SBATCH --error=teleconnections_%j.err
#SBATCH --account=bb1153
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=128
#SBATCH --time=01:00:00
#SBATCH --mem=0
set -e

whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
source $whereconda/etc/profile.d/conda.sh
conda activate aqua

configfile_atm="${AQUA}/diagnostics/teleconnections/deliverable/config_deliverable_atm.yaml"
configfile_oce="${AQUA}/diagnostics/teleconnections/deliverable/config_deliverable_oce.yaml"
scriptfile="${AQUA}/diagnostics/teleconnections/cli/cli_teleconnections.py"
outputdir="${AQUA}/diagnostics/teleconnections/deliverable/output"
workers=32

echo "Running the global time series diagnostic with $workers workers"
echo "Script: $scriptfile"

echo "Running the atmospheric diagnostic"
echo "Config: $configfile_atm"
python $scriptfile -c $configfile_atm -l DEBUG -n $workers --outputdir $outputdir --ref

echo "Running the oceanic diagnostic"
echo "Config: $configfile_oce"
python $scriptfile -c $configfile_oce -l DEBUG -n $workers --outputdir $outputdir --ref

echo "Running the concordance map script"
python ${AQUA}/diagnostics/teleconnections/cli/cli_bootstrap.py -l DEBUG --outputdir $outputdir --config $configfile_oce -n $workers

echo "Done"