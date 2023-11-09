#!/bin/bash

#SBATCH -A project_465000454
#SBATCH --cpus-per-task=1
#SBATCH -n 1
#SBATCH -t 00:25:00 #change the wallclock
#SBATCH -J aqua_jupyter
#SBATCH --output=/users/pghinass/AQUA/diagnostics/tropical_cyclones/slurm_jobs/prova_%j.out
#SBATCH --error=/users/pghinass/AQUA/diagnostics/tropical_cyclones/slurm_jobs/prova_%j.err
#SBATCH -p debug    #change the partition

AQUA_container=/project/project_465000454/igonzalez/aqua/aqua-v0.2.sif
FDB5_CONFIG_FILE=/scratch/project_465000454/igonzalez/fdb-long/config.yaml #Change it to your simulation
GSV_WEIGHTS_PATH=/scratch/project_465000454/igonzalez/gsv_weights/
GRID_DEFINITION_PATH=/scratch/project_465000454/igonzalez/grid_definitions

# directory of python script to execute
DIR=/users/pghinass/AQUA/diagnostics/tropical_cyclones

singularity exec  \
    --cleanenv \
    --env FDB5_CONFIG_FILE=$FDB5_CONFIG_FILE \
    --env GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH \
    --env GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk  \
    --bind /pfs/lustrep3/scratch/project_465000454  \
    --bind /scratch/project_465000454  \
    /project/project_465000454/containers/aqua/aqua-v0.2.sif \
    bash -c \
    python $DIR/prova.py


