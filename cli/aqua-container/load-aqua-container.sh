#!/bin/bash
set -e

#--------------------------------------------------------------#
#----- Script to load AQUA container on multiple machines -----#
#------------ Support for levante, lumi and MN5 ---------------#
#--------------------------------------------------------------#

# Set default AQUA container version
version="latest"

#--------------------------------------------------------------#
#--------------------------Parsing block-----------------------#
#--------------------------------------------------------------#

usage() {
    cat << EOF
    Usage: $0 <machine_name> [OPTIONS]

    Mandatory Argument:
        machine_name             The name of the machine to set.
                                 Machine supported are lumi, levante and MN5

    Options:
        --native                 Enable native mode: AQUA will read from native env variable.
        -s <script>              Execute an executable bash or python script.
        -c <command>             Execute a shell command.
        -v <version>             Specify the AQUA container version (default: "latest").
        -h, --help               Display this help message.

    Examples:
        $0 lumi --native
        $0 lumi -s my_script.py
        $0 lumi -c "echo 'I love AQUA so much!'"
        $0 lumi -v 0.12.1
EOF
    exit 1
}

# Parse the mandatory machine argument and options
parse_machine() {
    # IMPORTANT: this will set the $cmd and $script
    #global variables for container execution

    # Check if the machine argument is provided
    if [ -z "$1" ]; then
        echo "ERROR: A machine name is required."
        usage
    fi
    machine="$1"
    shift  # Shift the first argument to process options

    # Default values for options
    native_mode=0  # AQUA reads from container 
    script=""     # Script to be read as argument
    cmd="shell"   # Standard container init
    mode=""    # Container mode: none, script or bash
 
    # Use getopt to parse options
    OPTIONS=$(getopt -o hnc:s:v: --long help,native,version:,command:,script: -n "$0" -- "$@")
    if [ $? -ne 0 ]; then
        usage
    fi
    eval set -- "$OPTIONS"

    # Process each option
    while true; do
        case "$1" in
            -n|--native)
                native_mode=1; shift ;;
            -c|--command)
                mode="bash"; cmd="exec"; script=$2; shift 2 ;;
            -s|--script)
                mode="script"; cmd="exec"; script=$2; shift 2 ;;
            -v|--version)
                version="$2"; shift 2 ;;
            -h | --help)
                usage ;;
            --)
                shift
                break   
                ;;
            *)
                echo "ERROR: Invalid option '$1'"
                usage ;;
        esac
    done

    # Set up global variable for native mode
    echo "Machine is set to: $machine"

    # Local model configuration
    [ "$native_mode" -eq 1 ] && echo "Local mode is enabled: AQUA will use native installation instead of container one."
    if [[ "$native_mode" -eq 0 ]]; then
        export AQUA="/app/AQUA"
        echo "Selecting the AQUA path $AQUA from the container."
        echo "AQUA version is set to: $version"
    else
        if [ -z $AQUA ]; then
            echo "ERROR: AQUA directory is not set!"
            exit 1
        fi
        # Check if AQUA is set and the file exists 
        echo "Selecting native AQUA path: $AQUA"
        echo "Please use this with caution since it is not how the container is meant to be used!"
        echo "Remember to run: pip install -e `$AQUA` once you are in the container"
        if [ ! -d "$AQUA" ]; then
            echo "ERROR: The AQUA directory does not exist at: $AQUA"
            exit 1
        fi
        branch_name=$(git -C "$AQUA" rev-parse --abbrev-ref HEAD)
        echo "Current branch: $branch_name"
        last_commit=$(git -C "$AQUA" log -1 --pretty=format:"%h %an: %s")
        echo "Last commit: $last_commit"
    fi

    # Container mode configuration
    if [[ "$mode" == "script" ]]; then
        if [[ -f $script ]]; then
            echo "Running script: $script"
            script="./$script"
        else
            echo "ERROR: Cannot find script $script to run"
            exit 1
        fi 
    elif [[ "$mode" == "bash" ]]; then
        echo "Executing command line: $script"
    fi


}

#--------------------------------------------------------------#
#------------------Machine-dependent setup---------------------#
#--------------------------------------------------------------#

function setup_container_path(){
    machine=$1
    case "$machine" in
        "lumi")
            AQUA_folder="/project/project_465000454/containers/aqua"
            ;;
        
        "levante")
            AQUA_folder="/work/bb1153/b382289/container/aqua"
            ;;
        
        "MN5")
            AQUA_folder="/gpfs/projects/ehpc01/containers/aqua"
            ;;
        
        *)
            echo "ERROR: The machine $machine is not supported" >&2
            return 1
            ;;
    esac

    
    if [ ${version} == "latest" ] ; then
        echo "Asking for latest AQUA version, detecting the more recent available in ${AQUA_folder}" >&2
        available_versions=$(find ${AQUA_folder} -type f -name 'aqua_*.sif' -exec basename {} .sif \; | sed 's/^aqua_//')
        version=$(printf "%s\n" "${available_versions[@]}" | sort -V -r | head -n 1 )
        echo "AQUA v${version} selected! If you are not happy, please specify your version with -v flag" >&2
    fi 

    AQUA_container="$AQUA_folder/aqua_${version}.sif"

    if [ ! -f "$AQUA_container" ]; then
        echo "ERROR: The AQUA container does not exist at: $AQUA_container" >&2
        return 1
    fi

    # if [ ${version} == "latest" ] ; then
    #     resolved_path=$(readlink "$AQUA_container")

    #     if [ -n "$resolved_path" ]; then
    #         AQUA_container="$AQUA_folder/$resolved_path"
    #     else
    #         echo "Warning: Unable to resolve the symlink for $AQUA_container. Using the specified path instead." >&2
    #     fi
    # fi

    echo "${AQUA_container}"
}

function setup_envs(){
    machine=$1

    case "$machine" in
        "lumi")
            GSV_WEIGHTS_PATH="/scratch/project_465000454/igonzalez/gsv_weights/"
            GRID_DEFINITION_PATH="/scratch/project_465000454/igonzalez/grid_definitions"
            ESMFMKFILE="/opt/conda/lib/esmf.mk"
            ;;
        "levante")
            GSV_WEIGHTS_PATH=""
            GRID_DEFINITION_PATH="/work/bb1153/b382321/grid_definitions"
            ESMFMKFILE="/opt/conda/lib/esmf.mk"
            ;;
        "MN5")
            GSV_WEIGHTS_PATH=""
            GRID_DEFINITION_PATH=""
            ESMFMKFILE="/opt/conda/lib/esmf.mk"
            ;;
        *)
            echo "ERROR: The machine $machine is not supported" >&2
            return 1
            ;;
    esac

    envs=("GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH"
          "GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH"
          "PYTHONPATH=$PYTHONPATH"
          "PYTHONUSERBASE=1" #this is used to remove reference to .local
          "AQUA=$AQUA" #this is common to all machines
          "PYTHONPATH=$AQUA" #this is common to all machines
          "ESMFMKFILE=$ESMFMKFILE")

    echo "${envs[@]}"
}

function setup_binds(){

    machine=$1
    case "$machine" in
        "lumi")
            binds=(
                "/pfs/lustrep1/"
                "/pfs/lustrep2/"
                "/pfs/lustrep3/"
                "/pfs/lustrep4/"
                "/pfs/lustrep3/scratch/"
                "/appl/local/climatedt/"
                "/appl/local/destine/"
                #"/flash/project_465000454/" #REMOVED SINCE IT LEADS TO COINTAINER HANG
                "/projappl/"
                "/project/"
                "/scratch/"
            )
            ;;

        "levante")
            binds=(
                "/work/bb1153"
                "/pool/data/ICDC/atmosphere/ceres_ebaf/"
            )
            ;;

        "MN5")
            binds=(
                "/gpfs/projects/ehpc01/"
            )
            ;;

        *)
            echo "ERROR: The machine $machine is not supported" >&2
            return 1
            ;;
    esac

    echo "${binds[@]}"
}

#--------------------------------------------------------------#
#-----------------------Real running---------------------------#
#--------------------------------------------------------------#

# parse the command line
parse_machine "$@"

# Call the function and assign its output to a variable
AQUA_container="$(setup_container_path $machine)"
if [ $? -ne 0 ]; then
    echo "Cannot find the container!"
    exit 1
fi

echo "Container to be loaded is: $AQUA_container"

ENVS=$(setup_envs $machine)
if [ $? -ne 0 ]; then
    echo "Problems with the env variables!"
    exit 1
fi

BINDS=$(setup_binds $machine)
if [ $? -ne 0 ]; then
    echo "Problems with the the bindings!"
    exit 1
fi

# extend the binding and the environment
env_args=""
for env in ${ENVS[@]}; do
    env_args+="--env $env "
done

bind_args=""
for bind in ${BINDS[@]}; do
    bind_args+="--bind $bind "
done

echo "Perfect! Now it's time to ride with AQUA ⛵"

#echo "singularity $cmd --cleanenv $env_args $bind_args $AQUA_container $script"
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
