#!/bin/bash

# Push analysis results to aqua-web
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

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <indir> <modelexp>"
    echo
    echo "<indir>:    the directory containing the output, e.g. output"
    echo "<modelexp>: the subfolder to push, e.g IFS-NEMO/historical-1990" 
    exit 1
fi

# setup a fresh local aqua-web copy
log_message INFO "Clone aqua-web"

git clone git@github.com:DestinE-Climate-DT/aqua-web.git aqua-web$$
indir="$1/$2"
dstdir="./aqua-web$$/content/pdf/$2"

# erase content and copy all files to content
log_message INFO "Collect and update figures in content/pdf"
cd aqua-web$$
git rm -r content/pdf/$2
cd ..
mkdir -p $dstdir

find $indir -name "*.pdf"  -exec cp {} $dstdir/ \;

# Remove dates from EC-mean filenames
for file in $dstdir/PI4*_????_????.pdf
do
    mv -- "$file" "${file%_*_*}.pdf"
done

echo $(date) > $dstdir/last_update.txt

# commit and push
log_message INFO "Commit and push"

cd aqua-web$$
git add content/pdf/$2
commit_message="update pdfs $(date)"
git commit -m "$commit_message"

git push

## cleanup
log_message INFO "Clean up"
cd ..
rm -rf aqua-web$$
#
log_message INFO "Pushed new figures to aqua-web"
