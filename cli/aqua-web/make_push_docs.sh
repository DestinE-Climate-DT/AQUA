#!/bin/bash

# Compiles AQUA html documentation and pushes to aqua-web
#
# Check if AQUA is set
if [[ -z "$AQUA" ]]; then
    echo -e "\033[0;31mError: The AQUA environment variable is not defined."
    echo -e "\x1b[38;2;255;165;0mPlease define the AQUA environment variable with the path to your 'AQUA' directory."
    echo -e "For example: export AQUA=/path/to/aqua\033[0m"
    exit 1  # Exit with status 1 to indicate an error
fi

source "$AQUA/cli/util/logger.sh"
setup_log_level 2 # 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL
log_message DEBUG "Sourcing logger.sh from: $AQUA/cli/util/logger.sh"

cd $AQUA/docs/sphinx

# build docs
log_message INFO "Building html"
make html

# setup a fresh local aqua-web copy
log_message INFO "Clone aqua-web"

git clone git@github.com:DestinE-Climate-DT/aqua-web.git aqua-web$$

# erase content and copy all new html files to content
log_message INFO "Update docs"
cd aqua-web$$
git rm -r content/documentation
mkdir -p content/documentation
cp -R ../build/html/* content/documentation/

# commit and push
log_message INFO "Commit and push"
git add content/documentation
commit_message="update docs $(date)"
git commit -m "$commit_message"
git push

## cleanup
log_message INFO "Clean up"
cd ..
rm -rf aqua-web$$
#
log_message INFO "Pushed new documentation to aqua-web"
