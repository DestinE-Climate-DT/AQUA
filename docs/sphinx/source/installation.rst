Installation
============

This document provides detailed installation instructions for the Python package AQUA, tested on Python 3.9 and 3.10. 
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
   
   git clone https://github.com/oloapinivad/AQUA.git

Change to the AQUA directory:

.. code-block:: bash
   
   cd AQUA

Create a new Mamba environment and install the required packages using the provided environment.yml file:

.. code-block:: bash
   
   mamba env create -f environment.yml

Activate the newly created aqua environment:

.. code-block:: bash
   
   conda activate aqua

At this point, you should have successfully installed the AQUA package and its dependencies 
in the newly created aqua environment.


If you are not working on Levante, remember to change the machine name in the `config/config.yaml` file:

.. code-block:: markdown
   
   machine: levante

If you are using LUMI, the script `/config/machines/lumi/installation/lumi_install.sh` provides an installation of the correct environment.
After putting your LUMI username under 'user' at the top of the script, you have to run the following:

.. code-block:: bash

   ./config/machines/lumi/installation/lumi_install.sh



Working with personal kernel in Jupyter Hub 
-------------------------------------------

You need to register the kernel for the aqua environment to work with the AQUA package in Jupyter Hub on HPC systems (like JUWELS or Levante).
In essence, the process comes down to the following steps:

1. Activate the aqua environment

.. code-block:: bash
   
   conda activate aqua

2. Install the ipykernel package

.. code-block:: bash
   
   mamba install ipykernel

3. Register the kernel

.. code-block:: bash
   
   python -m ipykernel install --user --name aqua --display-name "Python (aqua)"


Please follow the documentation on the process for the machine you are working, for example, on:

DKRZ: `how to Use your own kernel <https://docs.dkrz.de/doc/software%26services/jupyterhub/kernels.html#use-your-own-kernel>`_.

JUWELS: `presentation with instructions <https://juser.fz-juelich.de/record/890058/files/14_Jupyter.pdf>`_.


