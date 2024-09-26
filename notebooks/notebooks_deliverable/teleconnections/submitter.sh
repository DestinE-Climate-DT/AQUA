# #!/bin/bash
# 
# #SBATCH -A project_465000454
# #SBATCH --cpus-per-task=1
# #SBATCH -n 1
# #SBATCH -t 00:30:00 #change the wallclock
# #SBATCH -J aqua_jupyter
# #SBATCH --output=output_%j.out
# #SBATCH --error=output_%j.err
# #SBATCH -p debug    #change the partition
# set -e
# 
# # whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
# # source $whereconda/etc/profile.d/conda.sh
# # conda activate aqua


configfile_atm="/scratch/project_465000454/sughosh/AQUA/notebooks/notebooks_deliverable/teleconnections/config_deliverable_atm.yaml"
configfile_oce="/scratch/project_465000454/sughosh/AQUA/notebooks/notebooks_deliverable/teleconnections/config_deliverable_oce.yaml"
scriptfile="${AQUA}/diagnostics/teleconnections/cli/cli_teleconnections.py"
outputdir="./output"
workers=5

echo "Running the global time series diagnostic with $workers workers"
echo "Script: $scriptfile"

# echo "Running the atmospheric diagnostic"
# echo "Config: $configfile_atm"
# python $scriptfile -c $configfile_atm -l DEBUG -n $workers --outputdir $outputdir --ref

# echo "Running the oceanic diagnostic"
# echo "Config: $configfile_oce"
# python $scriptfile -c $configfile_oce -l DEBUG -n $workers --outputdir $outputdir --ref

echo "Running the concordance map script"
python ${AQUA}/diagnostics/teleconnections/cli/cli_bootstrap.py -l DEBUG --outputdir $outputdir --config $configfile_oce -n $workers

echo "Done"