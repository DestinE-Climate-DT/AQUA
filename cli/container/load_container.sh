#!/bin/bash

machine="lumi" # lumi, levante, MN5
version="0.12"

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

if [ "$user_defined_aqua" = "ask" ] ; then
    echo "Do you want to use your local AQUA (y/n): "
    read user_defined_aqua
fi

if [[ "$user_defined_aqua" = "yes" || "$user_defined_aqua" = "y" || "$user_defined_aqua" = "Y" ]]; then
    # Check if AQUA is set and the file exists 
    echo "Selecting this AQUA path for the container: $AQUA"
    if [ ! -d "$AQUA" ]; then
        echo "ERROR: The AQUA directory does not exist at: $AQUA"
        exit 1
    fi
    branch_name=$(git -C "$AQUA" rev-parse --abbrev-ref HEAD)
    echo "Current branch: $branch_name"
    last_commit=$(git -C "$AQUA" log -1 --pretty=format:"%h %an: %s")
    echo "Last commit: $last_commit"
elif [[ "$user_defined_aqua" = "no" || "$user_defined_aqua" = "n" || "$user_defined_aqua" = "N" ]]; then
    export AQUA="/app/AQUA"
    echo "Selecting the AQUA of the container."
else 
    echo "ERROR: Enter 'yes' or 'no' for user_defined_aqua"
    exit 1
fi

echo "Perfect! Now it's time to ride with AQUA ⛵"

function setup_container_path(){
    machine=$1
    if [ "$machine" == "lumi" ] ; then
        AQUA_folder="/project/project_465000454/containers/aqua"
    elif [ "$machine" == "levante" ] ; then
        AQUA_folder="/work/bb1153/b382289/container/aqua"
    elif [ "$machine" == "MN5" ] ; then
        AQUA_folder="/gpfs/projects/ehpc01/containers/aqua"
    else
        echo "ERROR: The machine $machine is not supported"
        exit 1
    fi
    AQUA_container="$AQUA_folder/aqua_${version}.sif"
    if [ ! -f "$AQUA_container" ]; then
        echo "ERROR: The AQUA container does not exist at: $AQUA_container"
        exit 1
    fi
    echo $AQUA_container
}

function setup_envs(){
    machine=$1

    if [ "$machine" = "lumi" ] ; then
        GSV_WEIGHTS_PATH="/scratch/project_465000454/igonzalez/gsv_weights/"
        GRID_DEFINITION_PATH="/scratch/project_465000454/igonzalez/grid_definitions"
        PYTHONPATH=$AQUA
        ESMFMKFILE="/opt/conda/lib/esmf.mk"
    elif [ "$machine" = "levante" ] ; then
        GSV_WEIGHTS_PATH=""
        GRID_DEFINITION_PATH="/work/bb1153/b382321/grid_definitions"
        PYTHONPATH=$AQUA
        ESMFMKFILE="/opt/conda/lib/esmf.mk"
    elif [ "$machine" = "MN5" ] ; then
        GSV_WEIGHTS_PATH=""
        GRID_DEFINITION_PATH=""
        PYTHONPATH=$AQUA
        ESMFMKFILE="/opt/conda/lib/esmf.mk"
    else
        echo "ERROR: The machine $machine is not supported"
        exit 1
    fi

    envs=("GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH"
          "GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH"
          "PYTHONPATH=$PYTHONPATH"
          "AQUA=$AQUA"
          "ESMFMKFILE=$ESMFMKFILE")

    echo "${envs[@]}"
}

function setup_binds(){
    machine=$1
    if [ "$machine" = "lumi" ] ; then
        binds=("/pfs/lustrep1/"
               "/pfs/lustrep2/"
               "/pfs/lustrep3/"
               "/pfs/lustrep4/"
               "/pfs/lustrep3/scratch/"
               "/appl/local/climatedt/"
               "/appl/local/destine/"
               "/flash/project_465000454/"
               "/projappl/"
               "/project/"
               "/scratch/")
    elif [ "$machine" = "levante" ] ; then
        binds=("/work/bb1153"
               "/pool/data/ICDC/atmosphere/ceres_ebaf/")
    elif [ "$machine" = "MN5" ] ; then
        binds=("/gpfs/projects/ehpc01/")
    else
        echo "ERROR: The machine $machine is not supported"
        exit 1
    fi

    echo "${binds[@]}"
}

# Call the function and assign its output to a variable
AQUA_container=$(setup_container_path $machine)

echo $AQUA_container

ENVS=$(setup_envs $machine)
if [ $? -ne 0 ]; then
    exit 1
fi

BINDS=$(setup_binds $machine)
if [ $? -ne 0 ]; then
    exit 1
fi

env_args=""
for env in ${ENVS[@]}; do
    env_args+="--env $env "
done

bind_args=""
for bind in ${BINDS[@]}; do
    bind_args+="--bind $bind "
done

echo "Perfect! Now it's time to ride with AQUA ⛵"

singularity $cmd --cleanenv $env_args $bind_args $AQUA_container $script

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