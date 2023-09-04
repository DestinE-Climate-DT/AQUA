AQUA Container
==============

Using a Singularity container, you can quickly load the AQUA environment on platforms like LUMI, Marenostrum, or Levante. This guide explains how to use the AQUA environment using the container, with a focus on LUMI. However, the same instructions apply to Marenostrum 4 and Levante.

Load AQUA Environment into the Shell
-------------------------------------

1. Create a shell script named `load_aqua.sh` using the following code:

.. code-block:: bash

   cat << 'EOF' > load_aqua.sh
   #!/bin/bash

   AQUA_container="/project/project_465000454/containers/aqua/aqua-v0.2.sif"
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
       --bind /pfs/lustrep3/scratch/project_465000454 \
       --bind /scratch/project_465000454 \
       $AQUA_container
   EOF

   chmod +x load_aqua.sh

2. Run the following code to load the AQUA environment into the shell:

.. code-block:: bash

   ./load_aqua.sh

In this way, you will have your shell with AQUA environment active.

Running Jupyter Notebook
------------------------

To run a Jupyter Notebook using the AQUA environment, follow these steps. 

1. Run the previously saved script in the terminal to load the AQUA Singularity container into the shell:

.. code-block:: bash

   ./load_aqua.sh

2. Start Jupyter Lab, which will provide a server URL like: http://localhost:8888/lab?token=random_token.

.. code-block:: bash

   jupyter-lab --port=8888 --no-browser

3. If you wish to open Jupyter Lab in your browser, execute the following command in a separate terminal, replacing "lumi" with your SSH hostname:

.. code-block:: bash

   ssh -L 8888:localhost:8888 lumi

4. Open the Jupyter Lab URL in your browser. It will launch Jupyter Lab. Choose the "Python 3 (ipykernel)" kernel for the AQUA environment.

Running Jupyter Notebook within VSCode
--------------------------------------


If you want to open notebooks in VSCode, follow the same steps as above, but then: 

5. Copy the Jupyter server URL.

6. Open a notebook in VS Code and in the top-right corner of the notebook, click on "Select kernel" >> "Select another kernel" >> "Existing Jupyter server" >> "Enter the URL" and paste the copied Jupyter server URL, then press enter.

7. Select "Python 3 (ipykernel)" as the kernel for the AQUA environment.

That's it! You can now work within the AQUA environment using Jupyter Notebook also within VSCode

Temporary Upgrade of Any Package
---------------------------------

If you want to upgrade any Python package in the container environment, it is possible by using pip install. If it is a Git repo, then clone it. Note that this upgrade will be temporary. Every time you open the container, it will start from its base environment.

.. code-block:: bash

   ./load_aqua.sh
   pip install any_package/or/path/to/the/repo/

Pointing to a Specific FDB
--------------------------

1. If you want to access a specific FDB, export your config file after loading the AQUA container:

.. code-block:: bash

   export FDB5_CONFIG_FILE=/path/to/config.yaml

Submitting Slurm Job Using the Container
-----------------------------------------

It might be required to use the container within a batch job. 
Below is a template for a Slurm script on Lumi. Customize it according to your needs.

.. code-block:: bash

   #!/bin/bash

   #SBATCH -A project_465000454
   #SBATCH --cpus-per-task=1
   #SBATCH -n 1
   #SBATCH -t 00:25:00  # Change the wallclock
   #SBATCH -J aqua_jupyter
   #SBATCH --output=aqua_slurm.out
   #SBATCH --error=aqua_slurm.err
   #SBATCH -p dev-g    # Change the partition

   AQUA_container=/project/project_465000454/containers/aqua/aqua-v0.2.sif
   FDB5_CONFIG_FILE=/scratch/project_465000454/igonzalez/fdb-long/config.yaml  # Change it to your simulation
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
       # You can edit the code below for your required script.
        
       pip install /scratch/project_465000454/softwares/gsv_interface
       export FDB5_CONFIG_FILE=/scratch/project_465000454/sughosh/config.yaml
        
       # To run Jupyter Lab on the compute node
       node=$(hostname -s)
       port=$(shuf -i8000-9999 -n1)
       jupyter-lab --no-browser --port=${port} --ip=${node}

       # Jupyter-lab in compute node:
       # Open aqua_slurm.err
       # Find a URL like this: http://node_number:port_number/lab?token=random_value
       # e.g. http://nid007521:8839/lab?token=random_value

       # In a separate terminal, run this:
       # ssh -L port_number:node_number:port_number lumi_user@@lumi.csc.fi
       # (e.g.: ssh -L 8839:nid007521:8839 lumi_user@@lumi.csc.fi)
       # Open the URL in your browser, and it will open Jupyter Lab.
       '
