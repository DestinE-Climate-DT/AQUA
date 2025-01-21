#!/bin/bash

# set -x

# CLI tool to push analysis results to aqua-web

push_lumio() {
    # This assumes that we are inside the aqua-web repository

    log_message INFO "Pushing to LUMI-O"
    cd content/png
    python $SCRIPT_DIR/push_s3.py $1 $2
    cd ../..
}

make_contents() {
    # This assumes that we are inside the aqua-web repository

    if [ "$content" -eq 1 ]; then
        log_message INFO "Making all content files"
        python $SCRIPT_DIR/make_contents.py -f
        dstdir="./content/png"
        git add $dstdir
    else
        log_message INFO "Making content files for $1"
        python $SCRIPT_DIR/make_contents.py -f -e $1
        dstdir="./content/png/$1"
        git add $dstdir
    fi
}

collect_figures() {
    # This assumes that we are inside the aqua-web repository

    log_message INFO "Collecting figures for $2"

    indir="$1/$2"
    dstdir="./content/pdf/$2"
    wipe=$3

    # erase content and copy all files to content
    if [ "$wipe" -eq 1 ]; then
        log_message INFO "Wiping destination directory $dstdir"
        git rm -r $dstdir
    fi

    mkdir -p $dstdir

    find $indir -name "*.pdf"  -exec cp {} $dstdir/ \;

    # Remove dates from EC-mean filenames
    for file in $dstdir/PI4*_????_????.pdf $dstdir/global_mean*_????_????.pdf
    do
        if [ -e "$file" ]; then
            mv -- "$file" "${file%_*_*}.pdf"
        fi
    done

    echo $(date) > $dstdir/last_update.txt
    git add $dstdir
}

convert_pdf_to_png() {
    # This assumes that we are inside the aqua-web repository

    if [ "$convert" -eq 1 ]; then
        log_message INFO "Converting PDFs to PNGs for $1"

        dstdir="./content/png/$1"

        git rm -r $dstdir
        mkdir -p $dstdir
    
        IFS='/' read -r catalog model experiment <<< "$1"
        ./pdf_to_png.sh "$catalog" "$model" "$experiment"
        git add $dstdir
    fi
}

# Note: the -r|--repository option is implemented but deactivated at the moment since 
# it may create issues when the repository history is purged. To be evaluated.

print_help() {
    echo "Usage: $0 [OPTIONS] INDIR EXPS"
    echo "Arguments:"
    echo "  INDIR                  the directory containing the output, e.g. ~/work/aqua-analysis/output"
    echo "  EXPS                   the subfolder to push, e.g climatedt-phase1/IFS-NEMO/historical-1990"
    echo "                         or the name of a text file containing a list of catalog, model, experiment (space separated)"
    echo
    echo "Options:"
    echo "  -b, --branch BRANCH    branch to push to (optional, default is "main")"
    echo "  -c, --content          flag to refresh all content.yaml files (default is only specific experiment)"
    echo "  -d, --dry-run          do not push to the repository"
    echo "  -h, --help             display this help and exit"
    echo "  -l, --loglevel LEVEL   set the log level (1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL). Default is 2."
    echo "  -m, --message MESSAGE  message for the automatic PR (optional)"
    echo "  -n, --no-convert       do not convert PDFs to PNGs"
    echo "  -o, --lumio            push also to LUMI-O (optional)"
    echo "  -t, --title TITLE      title for the automatic PR (optional)"
    echo "  -u, --user USER:PAT    credentials (in the format "username:PAT") to create an automatic PR for the branch (optional)"
    echo "  -w, --wipe             wipe the destination directory before copying the images"
    echo "  --bucket BUCKET        push to the specified bucket (optional, dafaults to 'aqua')"
}

if [ -z "$1" ] || [ -z "$2" ]; then
    print_help
    exit 0
fi

# Parse command line arguments
branch=""
repository=""
user=""
message=""
wipe=0
dry=0
content=0
convert=1
loglevel=2
bucket="aqua"
lumio=0

while [[ $# -gt 2 ]]; do
  case "$1" in
    -h|--help)
      print_help
      exit 0
      ;;
    -b|--branch)
        branch="$2"
        shift 2
        ;;
    -u|--user)
        user="$2"
        shift 2
        ;;
    -m|--message)
        message="$2"
        shift 2
        ;;
    -t|--title)
        title="$2"
        shift 2
        ;;
    -w|--wipe)
        wipe=1
        shift
        ;;
    -n|--no-convert)
        convert=0
        shift
        ;;
    -l|--loglevel)
        loglevel="$2"
        shift 2
        ;;
    -c|--content)
        content=1
        shift
        ;;
    -d|--dry-run)
        dry=1
        shift
        ;;
    --bucket)
        bucket="$2"
        shift 2
        ;;
    -o|--lumio)
        lumio=1
        shift
        ;;
    -*|--*)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

indir=$1
exps=$2

# define the location of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ ! -f "$SCRIPT_DIR/../util/logger.sh" ]; then
    echo "Warning: $SCRIPT_DIR/../util/logger.sh not found, using dummy logger"
    # Define a dummy log_message function
    function log_message() {
        echo "$2"
    }
else
    source "$SCRIPT_DIR/../util/logger.sh"
    setup_log_level $loglevel # 1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL
    log_message DEBUG "Sourcing logger.sh from: $SCRIPT_DIR/../util/logger.sh"
fi

log_message INFO "Processing $indir"

if [ "$convert" -eq 0 ]; then
    log_message INFO "Conversion of PDFs to PNGs suppressed"
fi

if [ -n "$repository" ]; then
    log_message INFO "Using local aqua-web repository: $repository"
    repo=$repository
    cd $repo
    git checkout main
    git pull
else
    log_message INFO "Clone aqua-web"
    git clone git@github.com:DestinE-Climate-DT/aqua-web.git aqua-web$$
    repo=aqua-web$$
    cd $repo
fi

if [ -n "$branch" ]; then
    log_message INFO "Creating and switching to branch $branch"
    git checkout -B $branch
fi

autopr=false
if [ -n "$branch" ]; then
    if [ -n "$user" ]; then
            log_message INFO "Will create automatic PR"
            autopr=true
            if [ -z "$title" ]; then
                title="Automatic PR for branch $branch"
            fi
    fi
fi

# erase content and copy all files to content
log_message INFO "Collect and update figures in content/pdf"

description="This is an automatic PR to update the figures in the aqua-web repository.\n\nThe following experiments were updated on $(date):\n\n|Catalog|Experiment|Model|\n|--------|-----------|------|\n"

# Check if the second argument is an actual file and use it as a list of experiments
if [ -f "$exps" ]; then
    log_message INFO "Reading list of experiments from $exps"

    # Loop over each line in the file
    while IFS= read -r line; do
        # Skip empty lines and lines starting with #
        if [[ -z "$line" || "$line" == "#"* ]]; then
            continue
        fi

        # Extract model, experiment, and source from the line
        catalog=$(echo "$line" | awk '{print $1}')
        model=$(echo "$line" | awk '{print $2}')
        experiment=$(echo "$line" | awk '{print $3}')

        log_message INFO "Collect figures for $catalog/$model/$experiment and converting to png"
        collect_figures "$1" "$catalog/$model/$experiment" $wipe
        convert_pdf_to_png "$catalog/$model/$experiment"
        make_contents "$catalog/$model/$experiment"  # create catalog.yaml and catalog.json
        if [ "$lumio" -eq 1 ]; then
            push_lumio $bucket "$catalog/$model/$experiment"
        fi
        description="$description|$catalog|$experiment|$model|\n"

    done < "$exps"
else  # Otherwise, use the second argument as the experiment folder
    log_message INFO "Collect figures for $exps and converting to png"
    collect_figures "$indir" "$exps" $wipe
    convert_pdf_to_png "$exps"
    make_contents "$exps"  # create catalog.yaml and catalog.json
    if [ "$lumio" -eq 1 ]; then
        push_lumio $bucket "$exps"
    fi
    description="$description|${exps//\//|}|\n"
fi

# commit and push
log_message INFO "Commit and push"

commit_message="update pdfs $(date)"
git commit -m "$commit_message"

if [ "$dry" -eq 1 ]; then
    log_message INFO "Dry run, not pushing to the repository"
    cd ..
    exit 0
fi

if [ -n "$branch" ]; then
    git push --set-upstream origin $branch
else
    git push
fi

cd ..

# Erase the temporary repository (only if the repository option was not specified)
log_message INFO "Clean up"
if [ -z "$repository" ]; then
    rm -rf $repo
fi
#
log_message INFO "Pushed new figures to aqua-web"

if [ "$autopr" = true ]; then
    if [ -z "$message" ]; then
        message=$description
    fi
    log_message INFO "Creating automatic PR for branch $branch"
    log_message INFO "    Title: $title"
    log_message INFO "    Description: $message"
    curl -u $user -X POST  -H "Accept: application/vnd.github.v3+json" \
         https://api.github.com/repos/DestinE-Climate-DT/aqua-web/pulls \
         -d "{\"title\":\"$title\",\"head\":\"$branch\",\"base\":\"main\",\"body\":\"$message\"}"
fi
