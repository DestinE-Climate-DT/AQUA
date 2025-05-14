#!/bin/bash

#set -x

# CLI tool to push analysis results to aqua-web

rsync_with_mkdir() {
    local rsync_target="$2"
    local local_path="$1"

    # Extract remote host and remote path
    local remote_host="${rsync_target%%:*}"
    local remote_path="${rsync_target#*:}"

    # Ensure the remote directory exists
    ssh "$remote_host" "mkdir -p \"$remote_path\""

    # Run rsync
    rsync -avz "$local_path/" "$rsync_target/"
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log_message ERROR "Rsync failed with exit code $exit_code"
        exit $exit_code
    fi
}

push_lumio() {
    # This assumes that we are inside the aqua-web repository
    if [[ -n "$3" ]]; then
        log_message INFO "Rsyncing figures to $rsync: $2"
        rsync_with_mkdir content/png/$2/ $3/content/png/$2/
        rsync_with_mkdir content/pdf/$2/ $3/content/pdf/$2/
        return
    else
        log_message INFO "Pushing figures to LUMI-O: $2"
        python $SCRIPT_DIR/push_s3.py $1 content/png/$2
        exit_code=$?
        if [ $exit_code -ne 0 ]; then
            log_message ERROR "Pushing PNGs failed with exit code $exit_code"
            exit $exit_code
        fi
        python $SCRIPT_DIR/push_s3.py $1 content/pdf/$2
        exit_code=$?
        if [ $exit_code -ne 0 ]; then
            log_message ERROR "Pushing PDFs failed with exit code $exit_code"
            exit $exit_code
        fi
    fi
}

make_contents() {
    # This assumes that we are inside the aqua-web repository

    log_message INFO "Making content files for $1 with config $2"
    python $SCRIPT_DIR/make_contents.py -f -e $1 -c $2
}

collect_figures() {
    # This assumes that we are inside the aqua-web repository

    log_message INFO "Collecting figures for $2"

    indir="$1/$2"
    dstdir="./content/pdf/$2"

    mkdir -p $dstdir
    find $indir -name "*.pdf"  -exec cp {} $dstdir/ \;

    # Remove dates from EC-mean filenames
    for file in $dstdir/PI4*_????_????.pdf $dstdir/global_mean*_????_????.pdf
    do
        if [ -e "$file" ]; then
            mv -- "$file" "${file%_*_*}.pdf"
        fi
    done

    # Copy experiment.yaml if it exists
    log_message INFO "Trying to collect $indir/experiment.yaml"
    if [ -f "$indir/experiment.yaml" ]; then
        log_message INFO "Collecting also experiment.yaml"
        mkdir -p ./content/png/$2
        cp "$indir/experiment.yaml" "./content/png/$2/"
    fi

    echo $(date) > $dstdir/last_update.txt
}

convert_pdf_to_png() {
    # This assumes that we are inside the aqua-web repository

    if [ "$convert" -eq 1 ]; then
        log_message INFO "Converting PDFs to PNGs for $1"

        dstdir="./content/png/$1"

        mkdir -p $dstdir
    
        IFS='/' read -r catalog model experiment <<< "$1"
        $SCRIPT_DIR/pdf_to_png.sh "$catalog" "$model" "$experiment"
    fi
}

print_help() {
    echo "Usage: $0 [OPTIONS] INDIR EXPS"
    echo "Arguments:"
    echo "  INDIR                  the directory containing the output, e.g. ~/work/aqua-analysis/output"
    echo "  EXPS                   the subfolder to push, e.g climatedt-phase1/IFS-NEMO/historical-1990"
    echo "                         or the name of a text file containing a list of catalog, model, experiment (space separated)"
    echo
    echo "Options:"
    echo "  -b, --bucket BUCKET    push to the specified bucket (defaults to 'aqua-web')"
    echo "  -c, --config FILE      alternate config file to determine diagnostic groupings for make_contents (defaults to config.grouping.yaml)"
    echo "  -d, --no-update        do not update the remote github repository"  
    echo "  -h, --help             display this help and exit"
    echo "  -l, --loglevel LEVEL   set the log level (1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL). Default is 2."
    echo "  -n, --no-convert       do not convert PDFs to PNGs (use only if all PNGs are already available)"  
    echo "  -r, --repository       remote aqua-web repository (default 'DestinE-Climate-DT/aqua-web'). If it starts with 'local:' a local directory is used."
    echo "  -s, --rsync URL        remote rsync target (takes priority over s3 bucket if specified)"
}

if [ -z "$1" ] || [ -z "$2" ]; then
    print_help
    exit 0
fi

# define the location of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse command line arguments

loglevel=2
convert=1
bucket="aqua-web"
repository="DestinE-Climate-DT/aqua-web"
update=1
rsync=""
config="$SCRIPT_DIR/config.grouping.yaml"

while [[ $# -gt 2 ]]; do
  case "$1" in
    -h|--help)
      print_help
      exit 0
      ;;
    -n|--no-convert)
        convert=0
        shift
        ;;
    -l|--loglevel)
        loglevel="$2"
        shift 2
        ;;
    -d|--no-update)
        update=0
        shift
        ;;
    -c|--config)
        config="$2"
        shift 2
        ;;
    -b|--bucket)
        bucket="$2"
        shift 2
        ;;
    -s|--rsync)
        rsync="$2"
        update=0  # if rsync is used, we will not update aqua-web
        shift 2
        ;;
    -r|--repository)
        repository="$2"
        shift 2
        ;;
    -*|--*)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

localrepo=0
if [[ $repository == local:* ]]; then
    repository=${repository#local:}
    localrepo=1
fi

indir=$1
exps=$2

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

if [ $localrepo -eq 1 ]; then
    log_message INFO "Using local repository $repository"
    repo=$repository
else
    log_message INFO "Clone aqua-web from $repository"
    repo=aqua-web$$
    if [ $update -eq 1 ]; then
        git clone git@github.com:$repository.git $repo
    else
        mkdir -p $repo
    fi
fi

cd $repo
if [ $update -eq 1 ]; then
    git checkout main
    git pull
fi

echo "Updated figures in bucket $bucket"  > updated.txt
echo "on $(date) for the following experiments:" >> updated.txt

# erase content and copy all files to content
log_message INFO "Collect and update figures in content/pdf"

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
        collect_figures "$1" "$catalog/$model/$experiment"
        convert_pdf_to_png "$catalog/$model/$experiment"
        make_contents "$catalog/$model/$experiment" "$config" # create catalog.yaml and catalog.json
        push_lumio $bucket "$catalog/$model/$experiment" "$rsync"
        echo "$catalog/$model/$experiment" >> updated.txt
    done < "$exps"
else  # Otherwise, use the second argument as the experiment folder
    log_message INFO "Collect figures for $exps and converting to png"
    collect_figures "$indir" "$exps"
    convert_pdf_to_png "$exps"
    make_contents "$exps" "$config" # create catalog.yaml and catalog.json
    push_lumio $bucket "$exps" "$rsync"
    echo "$exps" >> updated.txt
fi

if [ $update -eq 1 ]; then
    git add updated.txt

    # commit and push
    log_message INFO "Commit and push ..."
    git commit -m "update figures"

    git push
    log_message INFO "Updated repository $repository"
fi

cd ..

if [ $localrepo -eq 0 ]; then
    log_message DEBUG "Removing $repo"
    rm -rf $repo
fi

