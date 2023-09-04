#!/bin/bash

#SBATCH -A project_465000454
#SBATCH --cpus-per-task=1
#SBATCH -n 1
#SBATCH -t 00:25:00 #change the wallclock
#SBATCH -J aqua_jupyter
#SBATCH --output=aqua_slurm.out
#SBATCH --error=aqua_slurm.err
#SBATCH -p dev-g    #change the partition

AQUA_container=/project/project_465000454/containers/aqua/aqua-v0.2.sif
FDB5_CONFIG_FILE=/scratch/project_465000454/igonzalez/fdb-long/config.yaml #Change it to your simulation
GSV_WEIGHTS_PATH=/scratch/project_465000454/igonzalez/gsv_weights/
GRID_DEFINITION_PATH=/scratch/project_465000454/igonzalez/grid_definitions

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
    
    ' 
    #You can edit below code for your required script.
    
    pip install /scratch/project_465000454/softwares/gsv_interface
    export FDB5_CONFIG_FILE=/scratch/project_465000454/sughosh/config.yaml
    
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
# ssh -L port_number:node_number:port_number lumi_user@@lumi.csc.fi (e.g.: ssh -L 8839:nid007521:8839 lumi_user@@lumi.csc.fi)
# and open the URL in your browser, it will open jupyter-lab.

# If you face any issue, ask in the mattermost AQUA channel.
