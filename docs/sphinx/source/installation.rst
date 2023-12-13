Installation
============

This document provides detailed installation instructions for the Python package AQUA, which is currently developed on Python 3.11 (Python>= 3.9 is supported)

We recommend using Mamba, a package manager for conda-forge, for the installation process.

.. contents::
   :local:
   :depth: 1

Prerequisites
-------------

Before installing AQUA, ensure that you have the following prerequisites installed:

- `Git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_
- `Mamba <https://github.com/mamba-org/mamba>`_ or `Conda <https://docs.conda.io/projects/conda/en/latest/user-guide/install/>`_

.. note ::
    Currently, AQUA is a private repository, so you need to be registered as a user to access it.

Creating a Conda/Mamba Environment and Installing Packages
----------------------------------------------------------

Clone the AQUA repository from GitHub:

.. code-block:: bash
   
   git clone https://github.com/DestinE-Climate-DT/AQUA.git

Change to the AQUA directory:

.. code-block:: bash
   
   cd AQUA

Create a new Mamba environment and install the required packages using the provided ``environment-common.yml`` file:

.. code-block:: bash
   
   mamba env create -f environment-common.yml

This will install the packages required for the AQUA framework and all the available diagnostics.

Activate the newly created aqua environment:

.. code-block:: bash
   
   conda activate aqua_common

At this point, you should have successfully installed the AQUA package and its dependencies 
in the newly created aqua environment.

If you are not working on Levante, remember to change the machine name in the ``config/config-aqua.yaml`` file:

.. code-block:: markdown
   
   machine: levante

Installation on Lumi
--------------------

If you are using LUMI, you cannot use pure conda environments due to the Lustre filesystem.
A solution based on containers can be used, as described on `Lumi user-guide <https://docs.lumi-supercomputer.eu/software/installing/container-wrapper/>`_.
The script ``/config/machines/lumi/installation/lumi_install.sh`` provides an installation of the correct environment.

.. code-block:: bash

   ./config/machines/lumi/installation/lumi_install.sh

This installs the AQUA environment into a container, and then set up the correct modules via a ``load_aqua.sh`` script that is generated and then called from the ``.bash_profile``.

.. note ::

   Having multiple conda environments on Lumi is not straightforward, but is possible modifying your own ``$PATH`` pointing to the different conda binaries.
   Please check the Lumi user-guide mentioned above.

.. warning ::
   
   It is possible that, if you're recreating the environment, the code breaks while removing the folder ``~/mambaforge/aqua_common/bin``, complaining the resource is busy.
   In this case you may have some processes running in the background. 
   You can check them with ``ps -ef | grep aqua_common`` and kill them manually if needed.

.. note ::

   It is also possible to work using a container and singularity.
   Please check the :doc:`aqua_container` instructions.

Working with personal kernel in Jupyter Hub 
-------------------------------------------

You need to register the kernel for the aqua environment to work with the AQUA package in Jupyter Hub on HPC systems (like JUWELS or Levante).
In essence, the process comes down to the following steps:

1. Activate the aqua environment

.. code-block:: bash
   
   conda activate aqua_common

2. Install the ipykernel package

.. code-block:: bash
   
   mamba install ipykernel

3. Register the kernel

.. code-block:: bash
   
   python -m ipykernel install --user --name aqua --display-name "Python (aqua)"


Please follow the documentation on the process for the machine you are working, for example, on:

DKRZ: `how to Use your own kernel <https://docs.dkrz.de/doc/software%26services/jupyterhub/kernels.html#use-your-own-kernel>`_.

JUWELS: `presentation with instructions <https://juser.fz-juelich.de/record/890058/files/14_Jupyter.pdf>`_.


