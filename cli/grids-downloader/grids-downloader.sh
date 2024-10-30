#!/bin/bash
set -e

# define the aqua installation path
AQUA=$(aqua --path)/../..

echo $AQUA
if [ ! -d $AQUA ]; then
    echo -e "\033[0;31mError: AQUA is not installed."
    echo -e "\x1b[38;2;255;165;0mPlease install AQUA with aqua install command"
    exit 1  # Exit with status 1 to indicate an error
fi

source "$AQUA/cli/util/logger.sh"
log_message DEBUG "Sourcing logger.sh from: $AQUA/cli/util/logger.sh"
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
# outputdir="/appl/local/climatedt/data/AQUA/grids"
# for Levante
# outputdir="/work/bb1153/b382075/aqua/grids"
# for Leonardo
# outputdir="/leonardo_work/DestE_330_24/AQUA/grids"

log_message INFO "Creating output directory $outputdir"
mkdir -p $outputdir

# Download grids
for model in "${models[@]}"
do
    log_message INFO "Downloading grid for $model"

    # Hardcoded path to the grids on the Swift server

    # EN4 link
    if [ "$model" == "EN4" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/EN4.tar.gz?temp_url_sig=a62db3040e0d39e7d26d276842844c8fb47cd0d7&temp_url_expires=2027-02-04T14:28:04Z"
    fi 

    # ERA5 link
    if [ "$model" == "ERA5" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/ERA5.tar.gz?temp_url_sig=939efca1165c89b19f3a56350216a838431b759a&temp_url_expires=2027-02-04T14:28:21Z"
    fi

    # FESOM link
    if [ "$model" == "FESOM" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/FESOM.tar.gz?temp_url_sig=46d55291dc9fba710137e8a5f0c11c723822be50&temp_url_expires=2027-02-04T14:28:41Z"
    fi

    # HealPix link
    if [ "$model" == "HealPix" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/HealPix.tar.gz?temp_url_sig=612581fe9aa409a3a454ca24db98a96eadf629a8&temp_url_expires=2027-02-04T14:29:02Z"
    fi

    # ICON link
    if [ "$model" == "ICON" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/ICON.tar.gz?temp_url_sig=07353422388cdec113463665d2bb2fde6a75dbec&temp_url_expires=2027-02-04T14:29:17Z"
    fi

    # IFS link
    if [ "$model" == "IFS" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/IFS.tar.gz?temp_url_sig=01eaac3202f63f451ef465a7dd1a8ee25167f27f&temp_url_expires=2027-02-04T14:29:36Z"
    fi

    # lonlat link
    if [ "$model" == "lonlat" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/lonlat.tar.gz?temp_url_sig=707ca48208a4a5eab53f5c277333577835f0ae81&temp_url_expires=2027-02-04T14:30:04Z"
    fi

    # NEMO link
    if [ "$model" == "NEMO" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/NEMO.tar.gz?temp_url_sig=d7693ea294de56a90c98c7e967b8188ce3ec9b34&temp_url_expires=2027-02-04T14:30:23Z"
    fi

    # OSI-SAF link
    if [ "$model" == "OSI-SAF" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/OSI-SAF.tar.gz?temp_url_sig=b138f6a1a8cac962969e22f72253ae03c8e98d01&temp_url_expires=2027-02-04T14:30:42Z"
    fi

    # PSC link
    if [ "$model" == "PSC" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/PSC.tar.gz?temp_url_sig=dee0a029000a9fe7328f4f43bc5162541e319366&temp_url_expires=2027-02-04T14:30:58Z"
    fi

    # WOA18 link
    if [ "$model" == "WOA18" ]; then
        path="https://swift.dkrz.de/v1/dkrz_a973e394-5f24-4f4d-8bbf-1a83bd387ccb/AQUA/grids/WOA18.tar.gz?temp_url_sig=a1a11c27c123c0dd7f6c23cceb8e22d55fdfc1e1&temp_url_expires=2027-02-04T14:31:14Z"
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
