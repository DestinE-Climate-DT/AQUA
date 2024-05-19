#!/bin/bash

#SBATCH -A project_465000454
#SBATCH --cpus-per-task=1
#SBATCH -n 1
#SBATCH -t 00:25:00 #change the wallclock
#SBATCH -J aqua_jupyter
#SBATCH --output=aqua_slurm.out
#SBATCH --error=aqua_slurm.err
#SBATCH -p debug    #change the partition

AQUA_path=/path_to/AQUA 
AQUA_container=/project/project_465000454/containers/aqua/aqua-v0.8.1.sif
FDB5_CONFIG_FILE=/scratch/project_465000454/igonzalez/fdb-long/config.yaml
GSV_WEIGHTS_PATH=/scratch/project_465000454/igonzalez/gsv_weights/
GRID_DEFINITION_PATH=/scratch/project_465000454/igonzalez/grid_definitions

# singularity shell can be an option depending on the requirement
singularity exec \
    --cleanenv \
    --env FDB5_CONFIG_FILE=$FDB5_CONFIG_FILE \
    --env GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH \
    --env GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA_path \
    --env AQUA=$AQUA_path \
    --bind /pfs/lustrep3/ \
    --bind /flash/project_465000454 \
    --bind /projappl/ \
    --bind /project \
    --bind /scratch/ \
    $AQUA_container \
    bash -c \
    ' 
    # You can edit below code for your required script.
    # This is just an example to run jupyter-lab in compute node.
    # You can run your own script here.
    
    # To run jupyter-lab in compute node
    node=$(hostname -s)
    port=$(shuf -i8000-9999 -n1)
    jupyter-lab --no-browser --port=${port} --ip=${node}
    '


###### Jupyter-lab in compute node ######
# open aqua_slurm.err
# You will find an url like this: http://node_number:port_number/lab?token=random_value
# e.g. http://nid007521:8839/lab?token=random_value

# In a separate terminal run this :
# ssh -L port_number:node_number:port_number lumi_user@lumi.csc.fi (e.g.: ssh -L 8839:nid007521:8839 lumi_user@lumi.csc.fi)
# and open the URL in your browser, it will open jupyter-lab.

# If you face any issue, ask in the mattermost AQUA channel.
