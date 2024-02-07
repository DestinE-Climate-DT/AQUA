AQUA Container
==============

Using a Singularity container, you can quickly load the AQUA environment on platforms like LUMI, Marenostrum, or Levante. 

This guide explains how to use the AQUA environment using the container, with a focus on LUMI.
However, the same instructions apply to Marenostrum 4 and Levante.


Generate a Personal Access Token (PAT)
--------------------------------------

You need to generate a Personal Access Token from GitHub to authenticate your access to the GitHub Container Registry.
Follow these steps:

1. Go to your GitHub account settings.
2. Click on "Developer settings" in the left sidebar at the bottom of the list.
3. Under "Personal access tokens," click on the "Token (classic)" tab and then "Generate new token" on the top right.
4. Give the token a name, and make sure to select the appropriate scopes. You'll need at least ``read:packages`` and ``write:packages`` for the GitHub Container Registry.
5. Click "Generate token" at the bottom of the page.

Download the docker image
--------------------------

Pull the docker image from the docker hub using the generated token:

.. code-block:: bash

   singularity pull --docker-login docker://ghcr.io/destine-climate-dt/aqua:0.5.2

This will require you to enter your username and token generated above.
The above command will create a file called ``aqua_0.5.2.sif`` in the current directory.

.. note::
   If you want to use a different version of AQUA, you can change the tag in the above command.
   For example, to use version 0.4, you can use ``aqua:0.4``.

.. note::
   If in your machine is installed docker instead of singularity, you can pull the image with docker,
   just by replacing ``singularity pull`` with ``docker pull``.

Store the token
---------------

You can store the token as an environment variable:

.. code-block:: bash

   export SINGULARITY_DOCKER_USERNAME=mygithubusername
   export SINGULARITY_DOCKER_PASSWORD=generatedtoken

This will allow you to pull the image without having to enter your username and token every time.
It can be particularly useful if you want to use the image in a batch job.

Load AQUA Environment into the Shell
-------------------------------------

The AQUA repository contains a script to load the AQUA container (updated to the last release) with singularity on LUMI.
This contains also bindings to the commonly used folders on LUMI but it can be easily adapted to other platforms.

1. Navigate to the AQUA repository, in the ``config/machines/lumi/container`` folder:

.. code-block:: bash
   
   cd AQUA/config/machines/lumi/container


2. Run the `load_aqua_lumi.sh <https://github.com/DestinE-Climate-DT/AQUA/blob/main/config/machines/lumi/container/load_aqua_lumi.sh>`_ script  to load the AQUA environment into the shell:

.. code-block:: bash

   ./config/machines/lumi/container/load_aqua_lumi.sh

Specify the path to the AQUA repository
+++++++++++++++++++++++++++++++++++++++

The actual version of the script will also ask if you want to use your local AQUA or the one in the container.

This allows to use the container with the latest version of the environment, but with the local version of your AQUA repository,
pointing to a specific branch or commit you may want to test.

After loading the container you can be sure that the code is loaded from the container in ``/app/AQUA`` or from your local repository by running:

.. code-block:: bash

   echo $AQUA

In this way, you will have your AQUA environment activated on the shell.

Running Jupyter Notebook
------------------------

To run a Jupyter Notebook using the AQUA environment, follow these steps. 

1. Run the previously saved script in the terminal to load the AQUA Singularity container into the shell:

.. code-block:: bash

   ./config/machines/lumi/container/load_aqua_lumi.sh

2. Start Jupyter Lab, which will provide a server URL like: ``http://localhost:<port>/lab?token=random_token``.

.. code-block:: bash

   jupyter-lab --no-browser

3. If you wish to open Jupyter Lab in your browser, execute the following command in a separate terminal, replacing "lumi" with your SSH hostname:

.. code-block:: bash

   ssh -L <port>:localhost:<port> lumi

4. Open the Jupyter Lab URL in your browser. It will launch Jupyter Lab. Choose the "Python 3 (ipykernel)" kernel for the AQUA environment.

.. warning::
   When running the script, be sure to modify the paths in the script with your own paths.

Running Jupyter Notebook within VSCode
--------------------------------------

If you want to open notebooks in VSCode, follow the same steps as above, but then: 

5. Copy the Jupyter server URL.

6. Open a notebook in VS Code and in the top-right corner of the notebook, click on "Select kernel" >> "Select another kernel" >> "Existing Jupyter server" >> "Enter the URL" and paste the copied Jupyter server URL, then press enter.

7. Select "Python 3 (ipykernel)" as the kernel for the AQUA environment.

.. warning::
   When running the script, be sure to modify the paths in the script with your own paths.

Temporary Upgrade of Any Package
---------------------------------

If you want to upgrade any Python package in the container environment, it is possible by using pip install.
If it is a Git repository, then clone it.
Note that this upgrade will be temporary.
Every time you open the container, it will start from its base environment.

.. code-block:: bash

   ./load_aqua.sh
   pip install any_package/or/path/to/the/repo/

Pointing to a Specific FDB
--------------------------

1. If you want to access a specific FDB, export your config file after loading the AQUA container:

.. code-block:: bash

   export FDB5_CONFIG_FILE=/path/to/config.yaml

.. note::
   If you want to access different FDB sources with the AQUA reader, the reader itself can take care of
   different FDB configuration files.

Submitting Slurm Job Using the Container
-----------------------------------------

It might be required to use the container within a batch job. 
Below you can find a template for a Slurm script on Lumi.
You can customize it according to your needs.

.. note::
   A copy of this script is available in the AQUA repository in the ``config/machines/lumi/container`` folder.

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

   AQUA_container=/project/project_465000454/containers/aqua/aqua-v0.5.2.sif # Change it to your container
   FDB5_CONFIG_FILE=/scratch/project_465000454/igonzalez/fdb-long/config.yaml  # Change it to your simulation
   GSV_WEIGHTS_PATH=/scratch/project_465000454/igonzalez/gsv_weights/
   GRID_DEFINITION_PATH=/scratch/project_465000454/igonzalez/grid_definitions

   singularity exec  \
       --cleanenv \
       --env FDB5_CONFIG_FILE=$FDB5_CONFIG_FILE \
       --env GSV_WEIGHTS_PATH=$GSV_WEIGHTS_PATH \
       --env GRID_DEFINITION_PATH=$GRID_DEFINITION_PATH \
       --env PYTHONPATH=/opt/conda/lib/python3.11/site-packages \
       --env ESMFMKFILE=/opt/conda/lib/esmf.mk  \
       --bind /pfs/lustrep3/scratch/project_465000454  \
       --bind /scratch/project_465000454  \
       /project/project_465000454/containers/aqua/aqua-v0.5.2.sif \
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
