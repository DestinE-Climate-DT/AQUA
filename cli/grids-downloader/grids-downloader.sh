#!/bin/bash
set -e

# Check if AQUA is set and the file exists
if [[ -z "$AQUA" ]]; then
    echo -e "\033[0;31mError: The AQUA environment variable is not defined."
    echo -e "\x1b[38;2;255;165;0mPlease define the AQUA environment variable with the path to your 'AQUA' directory."
    echo -e "For example: export AQUA=/path/to/aqua\033[0m"
    exit 1  # Exit with status 1 to indicate an error
else
    source "$AQUA/cli/util/logger.sh"
    log_message INFO "Sourcing logger.sh from: $AQUA/cli/util/logger.sh"
fi
setup_log_level 2 # 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL

# This script downloads the grids from the Swift server of DKRZ
# and stores them in the specified output directory.
# The user can specify the models to download by removing objects from the
# models array.
model=$1

usage()
{
   echo "To download a single dataset:"
   echo "       ./grids-downloader.sh dataset-name"
   echo "To download all datasets:"
   echo "       ./grids-downloader.sh all"
   echo
}

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

# User defined variables

# for LUMI
outputdir="/pfs/lustrep3/projappl/project_465000454/data/AQUA/grids"
# for Levante
#outputdir="/work/bb1153/b382075/aqua/grids"

log_message INFO "Creating output directory $outputdir"
mkdir -p $outputdir

# Download grids
for model in "${models[@]}"
do
    log_message INFO "Downloading grid for $model"

    # Hardcoded path to the grids on the Swift server

    # EN4 link
    if [ "$model" == "EN4" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/EN4.tar.gz?temp_url_sig=8bdf6b39f88d2d894b8715dea113d909bcdd8b21&temp_url_expires=2026-08-31T13:45:59Z"
    fi 

    # ERA5 link
    if [ "$model" == "ERA5" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/ERA5.tar.gz?temp_url_sig=b7927eee57dd74abb5e83705356b7b7b0ebccc95&temp_url_expires=2026-08-31T13:49:15Z"
    fi

    # FESOM link
    if [ "$model" == "FESOM" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/FESOM.tar.gz?temp_url_sig=2a01ec890c2a1c7192d32e8bcd3a2a42d93ff8a2&temp_url_expires=2026-08-31T13:49:33Z"
    fi

    # HealPix link
    if [ "$model" == "HealPix" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/FESOM.tar.gz?temp_url_sig=2a01ec890c2a1c7192d32e8bcd3a2a42d93ff8a2&temp_url_expires=2026-08-31T13:49:33Z"
    fi

    # ICON link
    if [ "$model" == "ICON" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/ICON.tar.gz?temp_url_sig=0eed3205b5f2737437093b4f162f77b7e9cc6ecc&temp_url_expires=2026-08-31T13:50:11Z"
    fi

    # IFS link
    if [ "$model" == "IFS" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/IFS.tar.gz?temp_url_sig=b26b4ed95a2396736c65c7daeb7e113113bd4d5a&temp_url_expires=2026-08-31T13:50:27Z"
    fi

    # lonlat link
    if [ "$model" == "lonlat" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/lonlat.tar.gz?temp_url_sig=8e3d0874543c634c6f86c9e24516310bcb5a87f7&temp_url_expires=2026-08-31T13:50:53Z"
    fi

    # NEMO link
    if [ "$model" == "NEMO" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/NEMO.tar.gz?temp_url_sig=061122b80c26f3a26ff7a3a9e5bcc552a100a454&temp_url_expires=2026-08-31T13:50:40Z"
    fi

    # OSI-SAF link
    if [ "$model" == "OSI-SAF" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/OSI-SAF.tar.gz?temp_url_sig=2da0b849ed973beb395553140b0f831adc4f36fd&temp_url_expires=2026-08-31T13:51:39Z"
    fi

    # PSC link
    if [ "$model" == "PSC" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/PSC.tar.gz?temp_url_sig=2307f716e8f1cbae2b62f303ee1f001e40277778&temp_url_expires=2026-08-31T13:52:00Z"
    fi

    # WOA18 link
    if [ "$model" == "WOA18" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/WOA18.tar.gz?temp_url_sig=93c533739ad539e280629b95fc771db019d138bb&temp_url_expires=2026-08-31T13:52:12Z"
    fi

    # Download the grid
    wget -O $outputdir/$model.tar.gz $path

    # Unpack the grid
    log_message INFO "Unpacking grid for $model"
    tar -xzf $outputdir/$model.tar.gz -C $outputdir

    # Remove the tar.gz file
    log_message INFO "Removing tar.gz file for $model"
    rm -f $outputdir/$model.tar.gz
done

log_message INFO "Download complete"
