#!/bin/bash

# Compiles AQUA html documentation and pushes to aqua-web

# define the aqua installation path
AQUA=$(aqua --path)/..

echo $AQUA
if [ ! -d $AQUA ]; then
    echo -e "\033[0;31mError: AQUA is not installed."
    echo -e "\x1b[38;2;255;165;0mPlease install AQUA with aqua install command"
    exit 1  # Exit with status 1 to indicate an error
fi

source "$AQUA/cli/util/logger.sh"
log_message INFO "Sourcing logger.sh from: $AQUA/cli/util/logger.sh"

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
