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
AQUA_container="/project/project_465000454/containers/aqua/aqua-v0.7.1.sif"
FDB5_CONFIG_FILE="/scratch/project_465000454/igonzalez/fdb-long/config.yaml"
GSV_WEIGHTS_PATH="/scratch/project_465000454/igonzalez/gsv_weights/"
GRID_DEFINITION_PATH="/scratch/project_465000454/igonzalez/grid_definitions"

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

singularity shell \
    --cleanenv \
    --env FDB5_CONFIG_FILE=$FDB5_CONFIG_FILE \
    --env GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH \
    --env GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA \
    --env AQUA=$AQUA \
    --bind /pfs/lustrep3/ \
    --bind /projappl/ \
    --bind /project \
    --bind /scratch/ \
    $AQUA_container
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
