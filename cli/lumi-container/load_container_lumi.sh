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
AQUA_container="/project/project_465000454/containers/aqua/aqua_0.11.3.sif"
GSV_WEIGHTS_PATH="/scratch/project_465000454/igonzalez/gsv_weights/"
GRID_DEFINITION_PATH="/scratch/project_465000454/igonzalez/grid_definitions"

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

singularity $cmd \
    --cleanenv \
    --env GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH \
    --env GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA \
    --env AQUA=$AQUA \
    --bind /pfs/lustrep1/ \
    --bind /pfs/lustrep2/ \
    --bind /pfs/lustrep3/ \
    --bind /pfs/lustrep4/ \
    --bind /pfs/lustrep3/scratch/ \
    --bind /appl/local/climatedt/ \
    --bind /appl/local/destine/ \
    --bind /flash/project_465000454 \
    --bind /projappl/ \
    --bind /project \
    --bind /scratch/ \
    $AQUA_container $script

# Run this script in LUMI in VSCode 

# For different FDB config files, export specific FDB5_CONFIG_FILE.

##### To update any python package e.g. gsv interface, opa, aqua ######
# Do "pip install /path/to/repo/package_name" inside the singularity container.
# Remember, when you close the container, your changes get lost.
# You need to do it everytime you load the container.

######## Jupyter-Notebook Run in VSCode ##########
# Now, to run the Jupyter-notebooks with the AQUA environemnt
# Run "jupyter-lab"

# You will get a jupyter-server like this: "http://localhost:port/lab?token=random_token"

# If you are using VS-Code, open a notebook.
# On top right corner of the notebook, select for "select kernel" option.
# Next "Select another kernel" and then "Existing Jupyter Server".
# Paste the jupyter server url there and keep the password blank and Enter.
# Then you can use "Python 3(ipykernel)" kernel for AQUA env. 


# If you want to open jupyer-lab on your browser:
# run this in your system terminal "ssh -L port:localhost:port lumi_user@lumi.csc.fi", port is localhost channel.
# Then paste the jupyter url on your web browser.

# Detailed instructions can be found here: https://github.com/oloapinivad/AQUA/issues/420
