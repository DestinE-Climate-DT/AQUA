#!/bin/bash

AQUA=$(aqua --path)/../..

if [ ! -d $AQUA ]; then
    echo -e "\033[0;31mError: AQUA is not installed."
    echo -e "\x1b[38;2;255;165;0mPlease install AQUA with aqua install command"
    exit 1  # Exit with status 1 to indicate an error
else
    source "$AQUA/cli/util/logger.sh"
    log_message INFO "Sourcing logger.sh from: $AQUA/cli/util/logger.sh"
fi

setup_log_level 2 # 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL
AQUA_container="/work/bb1153/b382289/container/aqua/aqua_0.12.1.sif"
GRID_DEFINITION_PATH="/work/bb1153/b382321/grid_definitions"

# Simple command line parsing
user_defined_aqua="ask"
script=""
cmd="shell"
help=0

while [ $# -gt 0 ] ; do
        case $1 in
                -y) user_defined_aqua="y" ; shift 1 ;;
                -n) user_defined_aqua="n" ; shift 1 ;;
                -e) cmd="exec"; script="bash $2" ; shift 2 ;;
                -c) cmd="exec"; script=$2 ; shift 2 ;;
                -h) help=1 ; shift 1 ;;
                *) shift 1 ;;
        esac
done

if [ "$help" -eq 1 ]; then
    cat << END
Loads the AQUA container in LUMI or runs a script in the container

Options:
    -y          use the AQUA variable from your current machine
    -n          use the AQUA variable from the container
    -e SCRIPT   execute a script in the container
    -c CMD      run a command in the container
    -h          show this help message 
END
    exit 0
fi

if [ "$user_defined_aqua" == "ask" ] ; then
    log_message $next_level_msg_type "Do you want to use your local AQUA (y/n): "
    read user_defined_aqua
fi

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

singularity $cmd \
    --cleanenv \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA \
    --env AQUA=$AQUA \
    --env PYTHONUSERBASE=1 \
    --bind /work/bb1153 \
    --bind /pool/data/ICDC/atmosphere/ceres_ebaf/ \
    $AQUA_container $script

