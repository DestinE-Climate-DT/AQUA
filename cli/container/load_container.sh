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

function setup_lumi_envs(){
    AQUA_folder="/project/project_465000454/containers/aqua"
    AQUA_container="$AQUA_folder/aqua_${version}.sif"
    # if [ ! -f "$AQUA_container" ]; then
    #     echo "Error: The AQUA container does not exist at: $AQUA_container"
    #     exit 1
    # fi
    GSV_WEIGHTS_PATH="/scratch/project_465000454/igonzalez/gsv_weights/"
    GRID_DEFINITION_PATH="/scratch/project_465000454/igonzalez/grid_definitions"
    PYTHONPATH=$AQUA

    envs=("GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH"
          "GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH"
          "PYTHONPATH=$PYTHONPATH"
          "AQUA=$AQUA")

    echo "${envs[@]}"
}

function setup_lumi_binds(){
    # Bindings for the container
    binds=("/pfs/lustrep1/"
           "/pfs/lustrep2/"
           "/pfs/lustrep3/"
           "/pfs/lustrep4/"
           "/pfs/lustrep3/scratch/"
           "appl/local/climatedt/"
           "appl/local/destine/"
           "flash/project_465000454"
           "projappl/"
           "project"
           "scratch/")

    echo "${binds[@]}"
}

if [ "$machine" = "lumi" ]; then
    echo "Setting up LUMI specifics"
    ENVS="$(setup_lumi_envs)"
    echo "Environment variables: $ENVS"
    if [ $? -ne 0 ]; then
        exit 1
    fi

    BINDS="$(setup_lumi_binds)"
    echo "Bindings: $BINDS"
    if [ $? -ne 0 ]; then
        exit 1
    fi
fi

# Build the singularity command

singularity $cmd \
    --cleanenv \
    --env $ENVS \
    --bind $BINDS \
    $AQUA_container $script