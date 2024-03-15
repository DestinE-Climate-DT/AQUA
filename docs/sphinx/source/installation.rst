.. _installation:

Installation
============

In this section we will provide a step-by-step guide to install the Python package AQUA.
AQUA is developed and tested with Python 3.11 and it supports Python 3.9 and later.

We recommend using Mamba, a package manager for conda-forge, for the installation process.
However, you can also use Conda, the default package manager for Anaconda.

Prerequisites
-------------

Before installing AQUA, ensure that you have the following software installed:

- `Git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_: AQUA is hosted on GitHub, and you will need Git to clone the repository.
- `Mamba <https://github.com/mamba-org/mamba>`_ or `Conda <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`_: Mamba is a package manager for conda-forge, and it is recommended for the installation process. 

.. note ::
    Currently, AQUA is a private repository, so you need to be registered as a user to access it.
    If you need access, please contact the AQUA team.

Installation with Mamba
-----------------------

First, clone the AQUA repository from GitHub:

.. code-block:: bash

    git clone https://github.com/DestinE-Climate-DT/AQUA.git

Then, navigate to the AQUA directory:

.. code-block:: bash

    cd AQUA

Create a new environment with Mamba.
An environment file is provided in the repository, so you can create the environment with the following command:

.. code-block:: bash

    mamba env create -f environment.yml

This will create a new environment called ``aqua`` with all the required dependencies.

Finally, activate the environment:

.. code-block:: bash

    conda activate aqua

At this point, you should have successfully installed the AQUA package and its dependencies 
in the newly created aqua environment.

.. note ::
    Together with the environment file, a ``pyproject.toml`` file is provided in the repository.
    This file contains the required dependencies for the AQUA package and allows you to install the package with the pip package manager.
    However, we recommend using Mamba to install the package and its dependencies, since some dependencies are not available in the PyPI repository.
    If you want to install the package with pip, please be aware that you may need to install some dependencies manually.

Update of the environment
-------------------------

If you want to install AQUA in an existing environment, or if you want to update the environment with the latest version of the package,
you can install the dependencies with the following command:

.. code-block:: bash

    mamba env update -n <environment_name> -f environment.yml

Replace ``<environment_name>`` with the name of the existing environment if this is different from ``aqua``.

.. _installation-lumi:

Installation on LUMI HPC
------------------------

LUMI is currently the main HPC of the DestinE-Climate-DT project, and it is the main platform for the development of AQUA.
The Lustre filesystem does not support the use of conda environments, so another approach has been developed to install on LUMI,
based on `container-wrapper <https://docs.lumi-supercomputer.eu/software/installing/container-wrapper/>`_.

First, clone the AQUA repository from GitHub as described in the previous section.

For simpler installation, it is recommended to define a ``$AQUA`` environment variable that points to the AQUA directory:

.. code-block:: bash

    export AQUA=/path/to/AQUA

Then, navigate to the AQUA directory and specifically in the ``cli/lumi-install`` directory:

.. code-block:: bash

    cd $AQUA/cli/lumi-install

Run the installation script:

.. code-block:: bash

    ./lumi-install.sh

This installs the AQUA environment into a container, and then set up the correct modules
via a ``load_aqua.sh`` script that is generated and then called from the ``.bash_profile``.
The script will actually ask the user if they wish to include ``load_aqua.sh`` in ``.bash_profile`` at the end of the installation.
If you do not agree, you will need to call ``load_aqua.sh`` manually every time you want to use AQUA.

.. note ::
    The installation script is designed to be run on the LUMI cluster, and it may require some adjustments to be run on other systems
    that use the container-wrapper tool. Please refer to the documentation of the container-wrapper tool for more information.

.. warning ::
    This installation script, despite the name, does not install the AQUA package in the traditional sense nor in a pure container.
    It wraps the conda installation in a container, allowing to load LUMI modules and run from command line or batch jobs the AQUA code.
    Different LUMI module loading or setups may lead to different results, but it's the most flexible way to develop AQUA on LUMI.

.. note ::
    If you encounter any issues with the installation script, please refer to the :ref:`faq` section.

Installation and use of the AQUA container
------------------------------------------

In order to use AQUA in complicate workflows or in a production environment, it is recommended to use the AQUA container.
The AQUA container is a Docker container that contains the AQUA package and all its dependencies.

Please refer to the :ref:`container` section for more information on how to deploy and how to use the AQUA container.

.. note ::
    If you're working on LUMI HPC, a compact script is available to load the AQUA container,
    mounting the necessary folders and creating the necessary environment variables.
    Please refer to the :ref:`container` section.
