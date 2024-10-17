#!/bin/bash
#SBATCH --exclusive
#SBATCH --time 10:00:00
#SBATCH --job-name graphcast_nb
#SBATCH --output jupyter-notebook-%J.out
#SBATCH --error jupyter-notebook-%J.err
#SBATCH --gres=gpu:1
#SBATCH --ntasks=40
#SBATCH --account=bsc32
#SBATCH --qos acc_bsces

AQUA_path=$AQUA
# If you don't have access to ehpc01, use the below bsc32 path
# AQUA_container="/gpfs/projects/bsc32/DestinE/containers/aqua/aqua_0.9.2.sif"
AQUA_container="/gpfs/projects/ehpc01/containers/AQUA/aqua_0.12.sif"
# singularity shell can be an option depending on the requirement
singularity exec \
    --cleanenv \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --env PYTHONPATH=$AQUA_path \
    --env AQUA=$AQUA_path \
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
