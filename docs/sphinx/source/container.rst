.. _container:

Container
=========

Every new version of AQUA generates a new container, which is available on the GitHub Container Registry.

Download the container image
----------------------------

The container is available
`here <https://github.com/DestinE-Climate-DT/AQUA/pkgs/container/aqua>`_.

Using `Singularity <https://docs.sylabs.io/guides/latest/user-guide/>`_ or
`Docker <https://docs.docker.com/>`_, you can quickly download the container and
load the AQUA environment on any platform.

Pull the container image from the docker hub using a personal access token (PAT) generated from GitHub.
If you don't have a PAT, :ref:`pat`.

.. parsed-literal::

    singularity pull --docker-login docker://ghcr.io/destine-climate-dt/aqua:|version|

or

.. parsed-literal::

    docker pull ghcr.io/destine-climate-dt/aqua:|version|

This will require you to enter your username and PAT.
The above command will create a file called aqua\_\ |version|\.sif in the current directory.

.. note::
   If you want to use a different version of AQUA, you can change the tag in the above command.
   For example, to use version 0.7, you can use ``aqua:0.7``.

Load the container
------------------

The container can be loaded using the following command:

.. parsed-literal:: 

   singularity shell --cleanenv aqua\_\ |version|\.sif

or analogue for Docker.

Anyway, you may want to bind some folders to the container to access your data and scripts or
to define some environment variables.

Load container script
^^^^^^^^^^^^^^^^^^^^^

AQUA provides scripts to use the AQUA container (updated to the last release) with Singularity on LUMI and Levante.
These contain also bindings to the commonly used folders on the machine but they can be easily adapted to other platforms.
The scripts are located in the ``cli/lumi-container`` and ``cli/levante-container`` folders.
The scripts are (for LUMI, but same in the Levante folder):

- **load_container_lumi.sh**: Load the AQUA container with the most common bindings and environment variables.
  This is the most common script to use the AQUA container on LUMI on production environment.
- **slurm_job_container.sh**: A template for a Slurm script to use the AQUA container on LUMI.
  This is useful to run batch jobs on LUMI using the AQUA container, but it can be easily adapted to
  any platform using Slurm. By default it opens a Jupyter Lab server on the computational node.

The script can be called and will guide the user to load the container in an interactive way.
Otherwise some options can be passed to the script to avoid the interactive mode, for example in a batch job.

.. option:: -y

   Load the container with the local version of AQUA found in the ``$AQUA`` environment variable.

.. option:: -n

   Load the container with the container version of AQUA.

.. option:: -e <script>

   Execute a script in the container after loading it.

.. option:: -c <command>

   Execute a command in the container after loading it.

.. option:: -h
   
   Show the help message.

.. _pat:

Generate a Personal Access Token (PAT)
--------------------------------------

You need to generate a Personal Access Token from GitHub to authenticate your access to the GitHub Container Registry.

Follow these steps:

1. Go to your GitHub account settings.
2. Click on "Developer settings" in the left sidebar at the bottom of the list.
3. Under "Personal access tokens," click on the "Token (classic)" tab and then "Generate new token" on the top right.
4. Give the token a name, and make sure to select the appropriate scopes. You'll need at least ``read:packages`` and
   ``write:packages`` for the GitHub Container Registry.
5. Click "Generate token" at the bottom of the page.

You can store the token as an environment variable:

.. code-block:: bash

   export SINGULARITY_DOCKER_USERNAME=mygithubusername
   export SINGULARITY_DOCKER_PASSWORD=generatedtoken

This will allow you to pull the image without having to enter your username and token every time.
It can be particularly useful if you want to use the image in a batch job.

.. _advanced-container:

Advanced Topics
---------------

Running Jupyter Notebook
^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::
    If you want to use a notebook with the AQUA container, maybe you should consider using the
    LUMI installation procedure, see :ref:`installation-lumi`.
    The container is mainly designed for workflow and production environment, not for interactive use.

To run a Jupyter Notebook using the container, follow these steps. 

- Run the previously saved script in the terminal to load the AQUA Singularity container into the shell:

.. code-block:: bash

   $AQUA/cli/lumi-container/load_container_lumi.sh

- Start Jupyter Lab:

.. code-block:: bash

   node=$(hostname -s)
   port=$(shuf -i8000-9999 -n1)
   jupyter-lab --no-browser --port=${port} --ip=${node}

This will provide a server URL like: ``http://nodeurl:<port>/lab?token=random_token`` (e.g. ``http://nid007521:8839/lab?token=random_value``)

- If you wish to open Jupyter Lab in your browser, execute the following command in a separate terminal,
  replacing "lumi" with your SSH hostname:

.. code-block:: bash

   ssh -L <port>:nodeurl:<port> lumi

(e.g. ``ssh -L 8839:nid007521:8839 lumi``)

- Open the Jupyter Lab URL in your browser. It will launch Jupyter Lab. Choose the **Python 3 (ipykernel)** kernel for the AQUA environment.

.. note::
    Using the ``load_container_lumi.sh`` script will launch the Jupyter Lab server on the node where the script is executed.
    You may want to use a computational node to run the Jupyter Lab server, especially if you are running a large notebook.
    This can be achieved by requiring a computational node and then running the Jupyter Lab server on that node or 
    by using the Slurm script to run the Jupyter Lab server (you can find an example in the Slurm script itself).

Running Jupyter Notebook within VSCode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you want to open notebooks in VSCode, follow the same steps as above, but then: 

- Copy the Jupyter server URL.
- Open a notebook in VS Code and in the top-right corner of the notebook,
  click on *Select kernel* >> *Select another kernel* >> *Existing Jupyter server* >> *Enter the URL*
  and paste the copied Jupyter server URL, then press enter.
- Select "Python 3 (ipykernel)" as the kernel for the AQUA environment.

Temporary Upgrade of Any Package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you want to upgrade any Python package in the container environment, it is possible by using pip install.
If it is a Git repository, then clone it.

.. note::
    Note that this upgrade will be temporary.
    Every time you open the container, it will start from its base environment.