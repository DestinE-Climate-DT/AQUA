#!/bin/bash
# This script sets up and submits a SLURM job based on configurations from YAML files.

set -e # Exit immediately if a command exits with a non-zero status.

# define the aqua installation path
AQUA=$(aqua --path)/../..

echo $AQUA
if [ ! -d $AQUA ]; then
    echo -e "\033[0;31mError: AQUA is not installed."
    echo -e "\x1b[38;2;255;165;0mPlease install AQUA with aqua install command"
    exit 1  # Exit with status 1 to indicate an error
fi

source "$AQUA/cli/util/logger.sh"
log_message DEBUG "Sourcing logger.sh from: $AQUA/cli/util/logger.sh"

setup_log_level 2 # 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL
aqua=$AQUA

# Read and log the machine name from the YAML configuration file
log_message "Reading machine name from YAML"
machine=$(python -c "
try:
    import yaml
    with open('$AQUA/config/config-aqua.yaml') as f:
        config = yaml.safe_load(f)
        print(config['machine'])
except Exception as e:
    print('Error:', e)
")
if [[ $machine == Error:* ]]; then
    log_message ERROR "Error reading machine name: ${machine#Error: }"
    exit 1
fi
log_message INFO "Machine Name: $machine"

# Read configuration values like number of processes, nodes, etc., from another YAML file
read -r nproc nodes walltime memory lumi_version account partition run_on_sunday < <(python -c "
try:
    import yaml
    with open('$AQUA/cli/weights/weights_config.yml') as f:
        config = yaml.safe_load(f)['compute_resources']
        print(config['nproc'], config['nodes'], config['walltime'], config['memory'], config['lumi_version'], \
        config['account']['$machine'], config['partition']['$machine'], config['run_on_sunday'])
except Exception as e:
    print('Error:', e)
")
if [[ $nproc == Error:* ]]; then
    log_message ERROR "Failed to read compute resources from weights_config.yml: ${nproc#Error: }"
    exit 1
fi

# Set the job's start time based on 'run_on_sunday' flag
if [ "$run_on_sunday" == "True" ]; then
    log_message INFO "Scheduling job for next Sunday"
    begin_time=$(date -d "next Sunday 21:00" +"%Y-%m-%dT%H:%M:%S")
else
    log_message INFO "Scheduling job for immediate execution"
    begin_time=$(date +"%Y-%m-%dT%H:%M:%S")
fi

log_message INFO "Begin run time: $begin_time"

# Environment setup for different machines
if [ $machine == "levante" ]; then
    # find mamba/conda (to be refined)
    whereconda=$(which mamba | rev | cut -f 3-10 -d"/" | rev)
    source $whereconda/etc/profile.d/conda.sh
    # activate conda environment
    conda activate aqua
fi
# Function to load environment on LUMI
function load_environment_AQUA() {
        # Load env modules on LUMI
    module purge
    module use LUMI/$lumi_version
}

# Submitting the SLURM job
log_message INFO "Submitting the SLURM job"
if [ "$machine" == 'levante' ] || [ "$machine" == 'lumi' ]; then
    # Submit the SLURM job with submission_time variable
    sbatch --begin="$begin_time" <<EOL
#!/bin/bash
#SBATCH --account=$account
#SBATCH --partition=$partition
#SBATCH --job-name=weights
#SBATCH --output=weights_%j.out
#SBATCH --error=weights_%j.log
#SBATCH --nodes=$nodes
#SBATCH --ntasks-per-node=$nproc
#SBATCH --time=$walltime
#SBATCH --mem=$memory

# Load function to log messages with colored output
source "$AQUA/cli/util/logger.sh"

log_message INFO 'Hello from SLURM job!'
log_message INFO "Number of processes (nproc): $nproc"
log_message INFO "Number of nodes: $nodes"
log_message INFO "Walltime: $walltime"
log_message INFO "Memory: $memory"
log_message INFO "Account: $account"
log_message INFO "Partition: $partition"

# if machine is lumi use modules
if [ $machine == "lumi" ]; then
    load_environment_AQUA
    # get username
    username=$USER
    export PATH="/users/$username/mambaforge/aqua/bin:$PATH"
fi
/usr/bin/env python3 "$AQUA/cli/weights/generate_weights.py" --nproc=$nproc
EOL

else
    /usr/bin/env python3 "$AQUA/cli/weights/generate_weights.py" --nproc=$nproc
fi