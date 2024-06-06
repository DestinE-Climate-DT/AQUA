#!/bin/bash
# This script sets up and submits a SLURM job based on configurations from YAML files.

set -e # Exit immediately if a command exits with a non-zero status.

# Check if AQUA is set and the file exists
if [[ -z "$AQUA" ]]; then
    echo -e "\033[0;31mError: The AQUA environment variable is not defined."
    echo -e "\x1b[38;2;255;165;0mPlease define the AQUA environment variable with the path to your 'AQUA' directory."
    echo -e "For example: export AQUA=/path/to/aqua\033[0m"
    exit 1  # Exit with status 1 to indicate an error
else
    source "$AQUA/cli/util/logger.sh"
    log_message INFO "Sourcing logger.sh from: $AQUA/cli/util/logger.sh"
    # Your subsequent commands here
fi
setup_log_level 2 # 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL
aqua=$AQUA

# Read configuration values from the YAML file using a single Python script
config_json=$(python - <<EOF
import yaml
import json

try:
    with open('$AQUA/cli/weights/weights_config.yml') as f:
        config = yaml.safe_load(f)
        machine = config.get('machine', 'Error: Machine not found')
        compute_resources = config.get('compute_resources', {})
        nproc = compute_resources.get('nproc', 'Error: nproc not found')
        nodes = compute_resources.get('nodes', 'Error: nodes not found')
        walltime = compute_resources.get('walltime', 'Error: walltime not found')
        memory = compute_resources.get('memory', 'Error: memory not found')
        lumi_version = compute_resources.get('lumi_version', 'Error: lumi_version not found')
        account = compute_resources.get('account', {}).get(machine, f'Error: account not found for {machine}')
        partition = compute_resources.get('partition', {}).get(machine, f'Error: partition not found for {machine}')
        run_on_sunday = compute_resources.get('run_on_sunday', 'Error: run_on_sunday not found')

        result = {
            'machine': machine,
            'nproc': nproc,
            'nodes': nodes,
            'walltime': walltime,
            'memory': memory,
            'lumi_version': lumi_version,
            'account': account,
            'partition': partition,
            'run_on_sunday': run_on_sunday
        }

        print(json.dumps(result))

except Exception as e:
    print(json.dumps({'error': str(e)}))
EOF
)

# Log the raw JSON output for debugging
log_message DEBUG "Raw JSON output: $config_json"

# Parse the JSON output using jq
machine=$(echo $config_json | jq -r '.machine')
nproc=$(echo $config_json | jq -r '.nproc')
nodes=$(echo $config_json | jq -r '.nodes')
walltime=$(echo $config_json | jq -r '.walltime')
memory=$(echo $config_json | jq -r '.memory')
lumi_version=$(echo $config_json | jq -r '.lumi_version')
account=$(echo $config_json | jq -r '.account')
partition=$(echo $config_json | jq -r '.partition')
run_on_sunday=$(echo $config_json | jq -r '.run_on_sunday')
error=$(echo $config_json | jq -r '.error')

# Check for errors
if [[ -n $error && $error != "null" ]]; then
    log_message ERROR "Failed to read from weights_config.yml: $error"
    exit 1
fi

# Log the machine name
log_message INFO "Machine Name: $machine"

# Check for errors in other configuration values
for var in nproc nodes walltime memory lumi_version account partition run_on_sunday; do
    if [[ ${!var} == Error:* ]]; then
        log_message ERROR "Failed to read compute resources from weights_config.yml: ${!var#Error: }"
        exit 1
    fi
done

# Log the configuration values
log_message INFO "nproc: $nproc, nodes: $nodes, walltime: $walltime, memory: $memory, lumi_version: $lumi_version, account: $account, partition: $partition, run_on_sunday: $run_on_sunday"

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