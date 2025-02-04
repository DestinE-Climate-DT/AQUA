#!/bin/bash

# Compiles AQUA html documentation and pushes to aqua-web

print_help() {
    echo "Usage: $0"
    echo
    echo "Options:"
    echo "  -b, --bucket BUCKET    push to the specified bucket (optional, dafaults to 'aqua-web')"
    echo "  -d, --dry-run          do not push to the repository"
    echo "  -h, --help             display this help and exit"
    echo "  -l, --loglevel LEVEL   set the log level (1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL). Default is 2."
    echo "  -r, --repository       remote aqua-web repository (default 'DestinE-Climate-DT/aqua-web'). If it starts with 'local:' a local directory is used."
}

dry=0
loglevel=2
bucket="aqua-web"
repository="DestinE-Climate-DT/aqua-web"

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
    -d|--dry-run)
        dry=1
        shift
        ;;
    -b|--bucket)
        bucket="$2"
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

# define the location of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AQUA="$SCRIPT_DIR"/../..

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

cd $AQUA/docs/sphinx

# build docs
log_message INFO "Building html"
make html

# push to s3

log_message INFO "Pushing new docs to LUMI-O"
python $SCRIPT_DIR/push_s3.py $bucket build/html -d content/documentation

# Update aqua-web to trigger rebuild

log_message INFO "Update aqua-web to trigger rebuild"

if [ $localrepo -eq 1 ]; then
    log_message INFO "Using local repository $repository"
    repo=$repository
else
    log_message INFO "Clone aqua-web from $repository"
    repo=aqua-web$$
    git clone git@github.com:$repository.git $repo
fi

cd $repo
git checkout main
git pull

echo "Updated documentation on $(date)" > updated.txt
git add updated.txt
commit_message="update docs $(date)"
git commit -m "$commit_message"

if [ "$dry" -eq 1 ]; then
    log_message INFO "Dry run, not pushing to the repository"
else
    git push
    log_message INFO "Pushed to aqua-web"
fi

cd ..

if [ $localrepo -eq 0 ]; then
    log_message DEBUG "Removing $repo"
#    rm -rf $repo
fi

log_message INFO "Pushed new documentation to lumi-o and triggered rebuild"
