#!/bin/bash
#SBATCH --partition=compute
#SBATCH --job-name=jupyter
#SBATCH --output=output_%j.out
#SBATCH --error=output_%j.err
#SBATCH --account=bb1153
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=08:00:00
#SBATCH --mem=0 
set -e

# export AQUA = PATH_TO_AQUA_repo
AQUA_container="/work/bb1153/b382289/container/aqua/aqua_0.12.1.sif"
GRID_DEFINITION_PATH="/work/bb1153/b382321/grid_definitions"

module load singularity

singularity exec \
    --cleanenv \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA \
    --env AQUA=$AQUA \
    --env PYTHONUSERBASE=1 \
    --bind /pool/data/ICDC/atmosphere/ceres_ebaf/ \
    --bind /work/bb1153 \
    $AQUA_container \
    bash -c \
    ' 
    node=$(hostname -s)
    port=$(shuf -i8000-9999 -n1)
    jupyter-lab --no-browser --port=${port} --ip=${node}
    '

