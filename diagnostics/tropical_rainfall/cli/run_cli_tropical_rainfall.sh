#!/bin/bash
# This script sets up and submits a SLURM job based on configurations from YAML files.

set -e # Exit immediately if a command exits with a non-zero status.

# Read the machine name from the config-aqua.yaml file
machine=$(python -c "
import yaml
import sys

try:
    with open('$AQUA/config/config-aqua.yaml') as f:
        config = yaml.safe_load(f)
        print(config['machine'])
except Exception as e:
    sys.stderr.write('Error reading machine name: ' + str(e))
    sys.exit(1)
")

# Check if the machine name was read correctly
if [ -z "$machine" ]; then
    echo "Failed to read machine name from config-aqua.yaml."
    exit 1
else
    echo "Machine Name: $machine"
fi

# Pass the machine name to the Python script as an environment variable and read the output
read -r nproc nodes walltime memory lumi_version account partition run_on_sunday <<< $(python -c "
import yaml
import sys

try:
    machine = '$machine'
    with open('$AQUA/diagnostics/tropical_rainfall/cli/cli_config_trop_rainfall.yml') as f:
        config = yaml.safe_load(f)['compute_resources']
        print(config['nproc'], config['nodes'], config['walltime'], config['memory'], config['lumi_version'],
              config['account'][machine], config['partition'][machine], config['run_on_sunday'])
except Exception as e:
    sys.stderr.write('Error reading compute resources: ' + str(e))
    sys.exit(1)
")

# Check if the compute resources were read correctly
if [ -z "$nproc" ]; then
    echo "Failed to read compute resources from cli_config_trop_rainfall.yml."
    exit 1
fi

# Set the job's start time based on 'run_on_sunday' flag
if [ "$run_on_sunday" == "True" ]; then
    echo "Scheduling job for next Sunday"
    begin_time=$(date -d "next Sunday 21:00" +"%Y-%m-%dT%H:%M:%S")
else
    echo "Scheduling job for immediate execution"
    begin_time=$(date +"%Y-%m-%dT%H:%M:%S")
fi

echo "Begin run time: $begin_time"

# Environment setup for different machines
if [ $machine == "levante" ]; then
    # find mamba/conda (to be refined)
    whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
    source $whereconda/etc/profile.d/conda.sh
    conda init
    # activate conda environment
    conda activate aqua
fi
# Submitting the SLURM job
echo "Submitting the SLURM job"
if [ "$machine" == 'levante' ] || [ "$machine" == 'lumi' ]; then
    # Submit the SLURM job with submission_time variable
    sbatch --begin="$begin_time" <<EOL
#!/bin/bash
#SBATCH --account=$account
#SBATCH --partition=$partition
#SBATCH --job-name=cli
#SBATCH --output=cli_%j.out
#SBATCH --error=cli_%j.log
#SBATCH --nodes=$nodes
#SBATCH --ntasks-per-node=$nproc
#SBATCH --time=$walltime
#SBATCH --mem=$memory

echo 'Hello from SLURM job!'
echo "Number of processes (nproc): $nproc"
echo "Number of nodes: $nodes"
echo "Walltime: $walltime"
echo "Memory: $memory"
echo "Account: $account"
echo "Partition: $partition"

# if machine is lumi use modules
if [ $machine == "lumi" ]; then
    source $HOME/.bash_profile
    source $HOME/.bashrc
fi
cd $AQUA/diagnostics/tropical_rainfall/cli
/usr/bin/env python3 cli_tropical_rainfall.py --nproc=$nproc
EOL

else
    cd $AQUA/diagnostics/tropical_rainfall/cli
    /usr/bin/env python3 cli_tropical_rainfall.py --nproc=$nproc
fi
