#!/bin/bash

AQUA_container="/project/project_465000454/containers/aqua/aqua-v0.5.1.sif"
FDB5_CONFIG_FILE="/scratch/project_465000454/igonzalez/fdb-long/config.yaml"
GSV_WEIGHTS_PATH="/scratch/project_465000454/igonzalez/gsv_weights/"
GRID_DEFINITION_PATH="/scratch/project_465000454/igonzalez/grid_definitions"

read -p "Do you want to use your local AQUA (y/n): " user_defined_aqua

# Define colors for echo output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

colored_echo() {
  local color=$1
  shift
  echo -e "${color}$@${NC}"
}

if [[ "$user_defined_aqua" = "yes" || "$user_defined_aqua" = "y" || "$user_defined_aqua" = "Y" ]]; then
    script_dir=$(cd "$(dirname "$0")" && pwd)
    # AQUA_path=$(echo "$script_dir" | grep -oP '.*?AQUA' | head -n 1)
    AQUA_path=$(echo "$script_dir" | awk -F'/AQUA' '{print ($2 ? $1 "/AQUA" : "")}')
    if [ -z "$AQUA_path" ]; then
        colored_echo $RED "Not able to find the local AQUA Repository"
        read -p "Please provide the AQUA path: " AQUA_path
        branch_name=$(git -C "$AQUA_path" rev-parse --abbrev-ref HEAD)
        colored_echo $GREEN "Current branch: $branch_name"
        last_commit=$(git -C "$AQUA_path" log -1 --pretty=format:"%h %an: %s")
        colored_echo $GREEN "Last commit: $last_commit"
    else
        colored_echo $GREEN "Selecting this AQUA path for the container: $AQUA_path"
        branch_name=$(git -C "$AQUA_path" rev-parse --abbrev-ref HEAD)
        colored_echo $GREEN "Current branch: $branch_name"
        last_commit=$(git -C "$AQUA_path" log -1 --pretty=format:"%h %an: %s")
        colored_echo $GREEN "Last commit: $last_commit"
    fi
elif [[ "$user_defined_aqua" = "no" || "$user_defined_aqua" = "n" || "$user_defined_aqua" = "N" ]]; then
    colored_echo $GREEN "Selecting the AQUA of the container"
    AQUA_path="/app/AQUA"
else 
    colored_echo $RED "Enter 'yes' or 'no' for user_defined_aqua"
    exit 1
fi

colored_echo $GREEN "\033[1mPerfect! Now it's time to ride with AQUA ⛵\033[0m"



singularity shell \
    --cleanenv \
    --env FDB5_CONFIG_FILE=$FDB5_CONFIG_FILE \
    --env GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH \
    --env GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA_path \
    --env AQUA=$AQUA_path \
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