#!/bin/bash

#SBATCH -A project_465000454
#SBATCH --cpus-per-task=1
#SBATCH -n 10
#SBATCH -t 20:30:00 #change the wallclock
#SBATCH -J a0eo_LRA1
#SBATCH --output=oce_diagslurm1.out
#SBATCH --error=oce_diagslurm1.err
#SBATCH -p small    #change the partition

AQUA_path=/scratch/project_465000454/sughosh/AQUA/
AQUA_container=/project/project_465000454/containers/aqua/aqua-v0.4.sif
FDB5_CONFIG_FILE=/scratch/project_465000454/igonzalez/fdb-long/config.yaml #Change it to your simulation
GSV_WEIGHTS_PATH=/scratch/project_465000454/igonzalez/gsv_weights/
GRID_DEFINITION_PATH=/scratch/project_465000454/igonzalez/grid_definitions

singularity exec  \
    --cleanenv \
    --env FDB5_CONFIG_FILE=$FDB5_CONFIG_FILE \
    --env GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH \
    --env GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA_path \
    --env AQUA=$AQUA_path \
    --bind /pfs/lustrep3/ \
    --bind /projappl/ \
    --bind /scratch/ \
    $AQUA_container
    bash -c \
    '
    python /scratch/project_465000454/sughosh/AQUA/diagnostics/ocean3d/cli/cli_ocean3d.py
    '


###### Jupyter-lab in compute node ######
# open aqua_slurm.err
# You will find an url like this: http://node_number:port_number/lab?token=random_value
# e.g. http://nid007521:8839/lab?token=random_value

# In a separate terminal run this :
# ssh -L port_number:node_number:port_number lumi_user@@lumi.csc.fi (e.g.: ssh -L 8839:nid007521:8839 lumi_user@@lumi.csc.fi)
# and open the URL in your browser, it will open jupyter-lab.

# If you face any issue, ask in the mattermost AQUA channel.
