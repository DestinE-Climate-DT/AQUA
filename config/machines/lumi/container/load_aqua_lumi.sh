#!/bin/bash

AQUA_container="/project/project_465000454/containers/aqua/aqua-v0.4.sif"
FDB5_CONFIG_FILE="/scratch/project_465000454/igonzalez/fdb-long/config.yaml"
GSV_WEIGHTS_PATH="/scratch/project_465000454/igonzalez/gsv_weights/"
GRID_DEFINITION_PATH="/scratch/project_465000454/igonzalez/grid_definitions"

singularity shell \
    --cleanenv \
    --env FDB5_CONFIG_FILE=$FDB5_CONFIG_FILE \
    --env GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH \
    --env GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH \
    --env PYTHONPATH=/opt/conda/lib/python3.10/site-packages \
    --env ESMFMKFILE=/opt/conda/lib/esmf.mk \
    --bind /pfs/lustrep3/ \
    --bind /projappl/ \
    --bind /scratch/ \
    $AQUA_container
# Run this script in LUMI in VSCode 

# For different FDB config files, export specific FDB5_CONFIG_FILE.

##### To update any python package e.g. gsv interface, opa, aqua ######
# Do "pip install /path/to/repo/package_name" inside the singularity container.
# Remember, when you close the container, your changes get lost.
# You need to do it everytime you load the container.

######## Jupyter-Notebook Run in VSCode ##########
# Now, to run the Jupyter-notebooks with the AQUA environemnt
# Run "jupyter-lab"

# You will get a jupyter-server like this: "http://localhost:port/lab?token=random_token"

# If you are using VS-Code, open a notebook.
# On top right corner of the notebook, select for "select kernel" option.
# Next "Select another kernel" and then "Existing Jupyter Server".
# Paste the jupyter server url there and keep the password blank and Enter.
# Then you can use "Python 3(ipykernel)" kernel for AQUA env. 


# If you want to open jupyer-lab on your browser:
# run this in your system terminal "ssh -L port:localhost:port lumi_user@lumi.csc.fi", port is localhost channel.
# Then paste the jupyter url on your web browser.

# Detailed instructions can be found here: https://github.com/oloapinivad/AQUA/issues/420
