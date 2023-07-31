#!/bin/bash

# This container /scratch/project_465000454/sughosh/containers/aqua0.1.sif
# is based on this https://github.com/oloapinivad/AQUA/blob/main/environment-common.yml 

# To load AQUA-common environment
singularity shell  \
 --env FDB5_CONFIG_FILE=/scratch/project_465000454/sughosh/config.yaml \
 --env GSV_WEIGHTS_PATH=/scratch/project_465000454/igonzalez/gsv_weights/ \
 --env GRID_DEFINITION_PATH=/scratch/project_465000454/igonzalez/grid_definitions \
 --bind /pfs/lustrep3/scratch/project_465000454 \
 --bind /scratch/project_465000454 \
 /scratch/project_465000454/sughosh/containers/aqua0.1.sif

#for different FDB config files export specific FDB5_CONFIG_FILE.

######## Jupyter-Notebook Run ##########
# Now, to run the Jupyter-notebooks with the AQUA environemnt
# just run "jupyter-lab"

# you will get a jupyter-server like this: "http://localhost:8888/lab?token="

# If you are using VS-Code, just open a notebook and add this jupyter server url.
# password is "docker". It should connect.
# Then you can use "Python 3(ipykernel)" kernel for AQUA env. 

# If you want to open jupyer-lab on your browser:
# run this in your system terminal "ssh -L 8888:localhost:8888 lumi_user@lumi.csc.fi"
# rest are same as above

# If you face issue, ask in the mattermost AQUA channel.