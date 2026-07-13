#!/bin/bash

set -e

# CLI tool to push analysis results to aqua-web

rsync_with_mkdir() {
    local rsync_target="$2"
    local local_path="$1"

    # Extract remote host and remote path
    local remote_host="${rsync_target%%:*}"
    local remote_path="${rsync_target#*:}"

    # Run rsync

    if [ -e "$local_path" ]; then
        rsync -avz "$local_path/" "$rsync_target/" --relative --chmod=D775,F664
        exit_code=$?
        if [ $exit_code -ne 0 ]; then
            log_message ERROR "Rsync failed with exit code $exit_code"
            exit $exit_code
        fi
    else
        log_message ERROR "Directory $local_path does not exist, skipping rsync"
    fi
}

get_file() {
    # get_file <bucket> <remote_file> <local_file> <rsync>
    # This assumes that we are inside the aqua-web repository
    # We do not exit if an error occurs because indeed the remote file may not exist

    if [[ -n "$4" ]]; then
        log_message INFO "Getting file $2 from rsync target $4 writing to $3"
        if rsync "$4/$2" > /dev/null 2>&1; then
            rsync -avz $4/$2 $3
        else
            log_message WARNING "File $2 not found in rsync target $4."
        fi
    else
        log_message INFO "Getting file $2 from bucket $1 on LUMI-O"
        if ! python $SCRIPT_DIR/push_s3.py -g $1 $3 -d $2; then
            log_message WARNING "File $2 not found in bucket $1."
        fi
    fi
}

push_s3() {
    # push_s3 <bucket> <file>

    if [ -e "$2" ]; then
        log_message INFO "Pushing file $2 to bucket $1 on LUMI-O"
        python $SCRIPT_DIR/push_s3.py $1 $2 -d $2
        exit_code=$?
        if [ $exit_code -ne 0 ]; then
            log_message ERROR "Pushing $2 to bucket $1 failed with exit code $exit_code"
            exit $exit_code
        fi
    else
        log_message ERROR "File/directory $2 does not exist, skipping push_s3"
    fi
}

push_lumio() {
    # push_lumio <bucket> <experiment> <rsync>
    # This function pushes the figures to the specified S3 bucket and updates the experiments.yaml file.
    # This assumes that we are inside the aqua-web repository
    if [[ -n "$3" ]]; then
        log_message INFO "Rsyncing figures to $rsync: $2"
        if [ -f "content/experiments.yaml" ]; then
            rsync -avz content/experiments.yaml $3/content/experiments.yaml
        fi
        for fmt in $format; do
            rsync_with_mkdir "content/$fmt/$2/" "$3"
        done
        return
    else
        log_message INFO "Pushing figures to bucket $1 on LUMI-O, experiment: $2"
        if [ -f "content/experiments.yaml" ]; then
            push_s3 $1 content/experiments.yaml
        fi
        for fmt in $format; do
            push_s3 $1 "content/$fmt/$2"
        done
    fi
}

make_contents() {
    # This assumes that we are inside the aqua-web repository

    log_message INFO "Making content files for $1 with config $2 and ensemble $3"
    if [ $ensemble -eq 1 ]; then
        # If ensemble structure, we need to pass the ensemble flag
        for fmt in $format; do
            python $SCRIPT_DIR/make_contents.py -f -e $1 -c $2 --format $fmt
        done
    else
        # Otherwise, we use the old structure
        for fmt in $format; do
            python $SCRIPT_DIR/make_contents.py -f -e $1 -c $2 --no-ensemble --format $fmt
        done
    fi
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log_message ERROR "Creating content files for $1 failed with exit code $exit_code"
        exit $exit_code
    fi
}

collect_figures() {
    # This assumes that we are inside the aqua-web repository

    log_message INFO "Collecting figures for $2"

    indir="$1/$2"

    for fmt in $format; do
        dstdir="./content/$fmt/$2"
        mkdir -p $dstdir
        find $indir -name "*.$fmt"  -exec cp {} $dstdir/ \;
        echo $(date) $(whoami) @ $(uname -n) $(hostname -I) > $dstdir/last_update.txt
    done

    # Copy experiment.yaml if it exists
    log_message INFO "Trying to collect $indir/experiment.yaml"
    if [ -f "$indir/experiment.yaml" ]; then
        log_message INFO "Collecting also experiment.yaml"
        for fmt in $format; do
            mkdir -p "./content/$fmt/$2"
            cp "$indir/experiment.yaml" "./content/$fmt/$2/"
        done
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
    echo "  -c, --config FILE      alternate config file to determine diagnostic groupings for make_contents (defaults to config.grouping.yaml in \$AQUA/config/analysis)"
    echo "  -d, --no-update        do not update the remote github repository"
    echo "  --no-ensemble          use old ensemble structure with only 3 levels catalog/model/exp"
    echo "  -f, --format FORMAT    specify image formats to transfer (default is 'pdf,png,svg')"
    echo "  -h, --help             display this help and exit"
    echo "  -l, --loglevel LEVEL   set the log level (1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL). Default is 2."
    echo "  -r, --repository       remote aqua-web repository (default 'DestinE-Climate-DT/aqua-web'). If it starts with 'local:' a local directory is used."
    echo "  -s, --rsync URL        remote rsync target (takes priority over s3 bucket if specified)"
}

if [ "$#" -lt 2 ]; then
    print_help
    exit 0
fi

# define the location of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse command line arguments

loglevel=2
bucket="aqua-web"
repository="DestinE-Climate-DT/aqua-web"
update=1
rsync=""
config=""
ensemble=1  # Default to new ensemble structure with 4 levels (catalog/model/experiment/realization)
format="pdf png svg"

# Parse all options first
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print_help
      exit 0
      ;;
    -l|--loglevel)
        loglevel="$2"
        shift 2
        ;;
    --no-ensemble)
        ensemble=0
        shift
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
    -f|--format)
        format=$(echo "$2" | tr ',' ' ')
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
    *)
      # Stop parsing options, the rest are positional arguments
      break
      ;;
  esac
done

# Check for the two required positional arguments
if [ "$#" -ne 2 ]; then
    echo "Error: Missing required arguments INDIR and EXPS."
    print_help
    exit 1
fi

indir=$1
exps=$2

localrepo=0
if [[ $repository == local:* ]]; then
    repository=${repository#local:}
    localrepo=1
fi

if [ -z "$1" ] || [ -z "$2" ]; then
    print_help
    exit 0
fi

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

if [ -z "$config" ]; then
    if [ -n "$AQUA_CONFIG" ]; then
        config="$AQUA_CONFIG/analysis/config.grouping.yaml"
    elif [ -d "$HOME/.aqua" ]; then
        config="$HOME/.aqua/analysis/config.grouping.yaml"
    fi
fi

if [ -f "$config" ]; then
    log_message DEBUG "Using configuration file: $config"
else
    log_message ERROR "Configuration file not found. Please specify with -c or set AQUA_CONFIG environment variable."
    exit 1
fi

log_message INFO "Processing $indir"

if [ $localrepo -eq 1 ]; then
    log_message INFO "Using local repository $repository"
    repo=$repository
else
    log_message INFO "Clone aqua-web from $repository"
    # Create a truly random temporary directory
    repo=$(mktemp -d -p $PWD aqua-webXXXXXX)
    trap "rm -rf $repo" EXIT
    if [ $update -eq 1 ]; then
        git clone git@github.com:$repository.git $repo
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
log_message INFO "Collect and update figures in content/pdf, content/png and content/svg"

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
        if [ $ensemble -eq 1 ]; then
            realization=$(echo "$line" | awk '{print $4}')
            realization=${realization:-r1}  # Default to r1 if not specified
        fi

        if [ $ensemble -eq 1 ]; then
            expstr="$catalog/$model/$experiment/$realization"
        else
            expstr="$catalog/$model/$experiment"
        fi
        log_message INFO "Collect figures for $expstr"
        collect_figures "$1" "$expstr"
        get_file $bucket content/experiments.yaml content/experiments.yaml "$rsync"  # recover experiments.yaml file
        make_contents "$expstr" "$config" # create catalog.yaml and catalog.json and update experiments.yaml
        push_lumio $bucket "$expstr" "$rsync"  # push figures including experiments.yaml
        echo "$expstr" >> updated.txt
    done < "$exps"
else  # Otherwise, use the second argument as the experiment folder
    log_message INFO "Collect figures for $exps"
    collect_figures "$indir" "$exps"
    get_file $bucket content/experiments.yaml content/experiments.yaml "$rsync"  # recover experiments.yaml file
    make_contents "$exps" "$config" # create catalog.yaml and catalog.json and update experiments.yaml
    push_lumio $bucket "$exps" "$rsync"  # push figures to LUMI-O including experiments.yaml
    echo "$exps" >> updated.txt
fi

if [ $update -eq 1 ]; then
    git pull
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

log_message INFO "push_analysis job completed."
