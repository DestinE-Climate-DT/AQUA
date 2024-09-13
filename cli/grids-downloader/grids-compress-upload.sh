#!/bin/bash
set -e

# define the aqua installation path
AQUA=$(aqua --path)/..

echo $AQUA
if [ ! -d $AQUA ]; then
    echo -e "\033[0;31mError: AQUA is not installed."
    echo -e "\x1b[38;2;255;165;0mPlease install AQUA with aqua install command"
    exit 1  # Exit with status 1 to indicate an error
fi

source "$AQUA/cli/util/logger.sh"
log_message DEBUG "Sourcing logger.sh from: $AQUA/cli/util/logger.sh"
setup_log_level 2 # 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL

# This script compresses the grids as tar.gz files and uploads them to the SWIFT website
# You need to have a working token and be connected to the correct project if you want
# to upload the files to the SWIFT website and on the correct container
# The user can specify the models to compress and upload by removing objects from the
# models array.
model=$1

usage()
{
   echo "To compress and upload a single dataset:"
   echo "       ./grids-compress-upload.sh dataset-name"
   echo "To compress and upload all datasets:"
   echo "       ./grids-compress-upload.sh all"
   echo
}

log_message INFO "Loading the swift client module and creating a token"
module load py-python-swiftclient
# Check if the token exists: execute swift-token and capture the output
token_output=$(swift-token)

# Check if the output contains the token expiration message
if [[ $token_output == *"Your swift token will expire"* ]]; then
    log_message INFO "Token already exists, no action needed"
else
    log_message WARNING "Token doesn't exist or has expired, creating new token..."
    swift-token new
fi
module switch py-python-swiftclient/3.12.0-gcc-11.2.0

# Check if no arguments are provided
if [ $# -eq 0 ]; then
    usage
    exit 1
fi

if [[ $model == "all" ]] ; then
    models=("EN4" "ERA5" "FESOM" "HealPix" "ICON" "IFS" "lonlat" "NEMO" "OSI-SAF" "PSC" "WOA18")
else
    models=( $model ) 	
fi

griddir="/work/bb1153/b382075/aqua/grids"

# Compress and upload grids
log_message INFO "Compressing and uploading grids"
# If I don't move to the grid directory, SWIFT will upload the full path as subcontainers
cd $griddir

for model in "${models[@]}"
do
    log_message INFO "Compressing and uploading grid for $model"

    # Check if the grid directory exists
    if [ ! -d "$griddir/$model" ]; then
        log_message ERROR "The grid directory $griddir/$model does not exist."
        exit 1
    fi

    # Compress the grid directory
    log_message INFO "Compressing the grid directory $model"
    tar -cvzf $model.tar.gz $model

    # Check the size of the compressed file
    log_message DEBUG "Checking the size of the compressed file $model.tar.gz"
    size=$(wc -c < $model.tar.gz)
    log_message DEBUG "The size of the compressed file is $size"

    # if size >= 5 GB set split flag to true
    split=false
    if [ $size -ge 5000000000 ]; then
        log_message INFO "The size of the compressed file is greater than or equal to 5 GB. Splitting the file into smaller parts."
        split=true
    fi

    # Upload the compressed grid directory to the SWIFT website
    log_message INFO "Uploading the compressed grid directory $model.tar.gz to the SWIFT website"
    if [ "$split" = true ]; then
        swift upload --segment-size 1000000000 AQUA/grids $model.tar.gz
    else # size < 5 GB
        swift upload AQUA/grids $model.tar.gz
    fi
done

log_message INFO "Finished compressing and uploading grids"