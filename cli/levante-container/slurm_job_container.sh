#!/bin/bash
#SBATCH --partition=compute
#SBATCH --job-name=jupyter
#SBATCH --output=output_%j.out
#SBATCH --error=output_%j.err
#SBATCH --account=bb1153
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=16
#SBATCH --time=08:00:00
#SBATCH --mem=0 
set -e

# export AQUA = PATH_TO_AQUA_repo
AQUA_container="/work/bb1153/b382289/container/AQUA/aqua_v0.8.1.sif"

module load singularity
cd $AQUA
singularity exec \
    --cleanenv \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA \
    --env AQUA=$AQUA \
    --bind /pool/data/ICDC/atmosphere/ceres_ebaf/ \
    --bind /work/uc0928/DATA/ocean/ \
    --bind /work/bb1153 \
    $AQUA_container \
    bash -c \
    ' 
    node=$(hostname -s)
    port=$(shuf -i8000-9999 -n1)
    jupyter-lab --no-browser --port=${port} --ip=${node}
    '

