#!/bin/bash

# This container /project/project_465000454/containers/aqua/aqua-v0.2.sif
# is based on this https://github.com/oloapinivad/AQUA/blob/main/environment-common.yml env and AQUA v0.2.

# To load AQUA-common environment
singularity shell  \
 --cleanenv \
 --env FDB5_CONFIG_FILE=/scratch/project_465000454/igonzalez/fdb-long/config.yaml \
 --env GSV_WEIGHTS_PATH=/scratch/project_465000454/igonzalez/gsv_weights/ \
 --env GRID_DEFINITION_PATH=/scratch/project_465000454/igonzalez/grid_definitions \
 --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
 --bind /pfs/lustrep3/scratch/project_465000454 \
 --bind /scratch/project_465000454 \
 /project/project_465000454/containers/aqua/aqua-v0.2.sif

# For different FDB config files, export specific FDB5_CONFIG_FILE.

######## Jupyter-Notebook Run ##########
# Now, to run the Jupyter-notebooks with the AQUA environemnt
# just run "jupyter-lab"

# You will get a jupyter-server like this: "http://localhost:8888/lab?token=********"

# If you are using VS-Code, just open a notebook.
# On top right corner of the notebook, select for "select kernel" option.
# Then "Select another kernel" and then "Existing Jupyter Server".
# Paste the jupyter server url there and keep the password blank and Enter.
# Then you can use "Python 3(ipykernel)" kernel for AQUA env. 


# If you want to open jupyer-lab on your browser:
# run this in your system terminal "ssh -L port:localhost:port lumi_user@lumi.csc.fi", port is localhost channel.
# Then paste the jupyter url on your web browser.

# If you face any issue, ask in the mattermost AQUA channel.