#!/bin/bash
set -e
# This script synchronizes the grids from the Levante server to the LUMI server
# It may be generalized to synchronize grids between any two servers
# and for other folders (e.g. LRA created from an high-res experiment
# available only on one cluster)

# Usage: ./grids-sync.sh [levante_to_lumi | lumi_to_levante]
# Note: The script assumes that the user has passwordless ssh access to the servers
# and you're connected to the machine from which you're copying the grids

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

# Set the path to the grids on the servers
LEVANTE_GRID_PATH="/work/bb1153/b382075/aqua/grids"
LUMI_GRID_PATH="/appl/local/climatedt/data/AQUA/grids"

# Set the paths for the grids on levante and lumi
LEVANTE_SSH="levante:$LEVANTE_GRID_PATH"
LUMI_SSH="lumi:$LUMI_GRID_PATH"
# Function to synchronize grids from levante to lumi
sync_levante_to_lumi() {
    log_message INFO "Syncing grids from levante to lumi..."
    log_message INFO "Source path: $LEVANTE_GRID_PATH"
    log_message INFO "Destination path: $LUMI_GRID_PATH"
    rsync -avrh --progress $LEVANTE_GRID_PATH/ $LUMI_SSH/
}

# Function to synchronize grids from lumi to levante
sync_lumi_to_levante() {
    log_message INFO "Syncing grids from lumi to levante..."
    log_message INFO "Source path: $LUMI_GRID_PATH"
    log_message INFO "Destination path: $LEVANTE_GRID_PATH"
    rsync -avrh --progress $LUMI_GRID_PATH/ $LEVANTE_SSH/
}

# Main script
if [ "$1" == "levante_to_lumi" ]; then
    sync_levante_to_lumi
elif [ "$1" == "lumi_to_levante" ]; then
    sync_lumi_to_levante
else
    echo "Usage: $0 [levante_to_lumi | lumi_to_levante]"
    exit 1
fi

log_message INFO "Done."
