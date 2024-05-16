#!/bin/bash

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
AQUA_container="/work/bb1153/b382289/container/AQUA/aqua_v0.8.1.sif"

log_message $next_level_msg_type "Do you want to use your local AQUA (y/n): "
read user_defined_aqua

if [[ "$user_defined_aqua" = "yes" || "$user_defined_aqua" = "y" || "$user_defined_aqua" = "Y" ]]; then
    # Check if AQUA is set and the file exists 
    log_message INFO "Selecting this AQUA path for the container: $AQUA"
    branch_name=$(git -C "$AQUA" rev-parse --abbrev-ref HEAD)
    log_message INFO "Current branch: $branch_name"
    last_commit=$(git -C "$AQUA" log -1 --pretty=format:"%h %an: %s")
    log_message INFO "Last commit: $last_commit"
elif [[ "$user_defined_aqua" = "no" || "$user_defined_aqua" = "n" || "$user_defined_aqua" = "N" ]]; then
    export AQUA="/app/AQUA"
    log_message INFO "Selecting the AQUA of the container."
else 
    log_message CRITICAL "Enter 'yes' or 'no' for user_defined_aqua"
    exit 1
fi

log_message INFO "Perfect! Now it's time to ride with AQUA ⛵"

module load singularity

singularity shell \
    --cleanenv \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA \
    --env AQUA=$AQUA \
    --bind /work/bb1153 \
    $AQUA_container
