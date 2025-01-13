.. _getting_started:

Getting Started
===============

Basic Concepts
--------------

AQUA is designed to simplify the diagnostics process on high-resolution climate models. 
This is done by creating a series of tools that simplifies data access and basic data operations so that the 
users - diagnostics developers or climate researchers interested in accessing model data - can focus only on scientific analysis.
For this reason, whatever object accessed by AQUA is delivered as a `xarray <https://docs.xarray.dev/en/stable/>`_ object.
The package is built around a few core concepts:

- **Data reading and preprocessing**: Data are exposed through `intake <https://intake.readthedocs.io/en/stable/>`_ catalogs 
  and represented as `xarray <https://docs.xarray.dev/en/stable/>`_ objects. 
  This allows us to easily read and preprocess data from various sources, including local files, remote servers, 
  and cloud storage, from climate models and observational datasets.
- **Data fixing**: AQUA provides capabilities to change metadata (e.g., variable names) and data themselves
  (e.g., convert to different units or homogeneize coordinate names) so that model data reach the users with a common data format.
- **Regridding and interpolation**: AQUA offers robust regridding and interpolation functionalities 
  to align datasets with different grids and spatial resolutions.
- **Averaging and aggregation**: AQUA provides tools to perform temporal and spatial averaging and aggregation of climate data.
- **Parallel processing**: AQUA supports parallel processing through `dask <https://examples.dask.org/xarray.html>`_ to 
  speed up the execution of diagnostics.
- **Lazy evaluation**: AQUA uses `xarray <https://docs.xarray.dev/en/stable/>`_ to represent data, 
  which allows for lazy evaluation of operations, meaning that the data are not loaded into memory until they are needed.
- **Diagnostics**: most importantly, AQUA includes a set of built-in diagnostic tools,
  and it allows users to create custom diagnostics as well.

Python installation
-------------------

Installation can be done using `Miniforge <https://github.com/conda-forge/miniforge>`_.
Containers and tools specific to the machines used in the project are available.
Please refer to the :ref:`installation` and :ref:`container` sections for more information.

.. _initialization:

Catalog installation
--------------------

After the package has been installed, or the container has been loaded, the AQUA catalog needs to be set up.
This means to set up the configuration file and the catalog, with a copy or a link to the necessary files.
This needs to be done only once, unless catalogs or fix and grid files are added or need to be updated/removed.

.. note::
  A more complete description of the available commands can be found in the :ref:`aqua-console` section.

The following paragraphs cover the fundamental steps.

Set up the configuration folder
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The configuration folder contains configuration files and the catalogs added to the AQUA package.
To set up the configuration folder, run the following shell command:

.. code-block:: bash

    aqua install <machine>

This command will create the configuration folder in the ``$HOME/.aqua`` directory and it will copy there the essential files.
You will need to specify the machine name on which you are installing (e.g. "lumi", "levante" or "MN5")
Check the :ref:`aqua-install` section for more information.

.. warning::

    If you are using a supported machine please be sure to specify the correct machine name.
    Supported machines are ``lumi``, ``levante`` and ``MN5``.
    You may need to check to have the correct permissions to access the HPC specific directories.

Add a catalog
^^^^^^^^^^^^^^^

A catalog is a folder containing the YAML files that inform AQUA on the data available.
The catalog are derived from `intake <https://intake.readthedocs.io/en/stable/>`_ .

To add a catalog, run the following command:

.. code-block:: bash

    aqua add <catalog>

For example, to add the catalog for ``climatedt-phase1``, run:

.. code-block:: bash

    aqua add climatedt-phase1

This command will copy the catalog folder to the configuration folder. Please notice that will operate fetching the catalog.

.. caution:: 

  You will need an internet connection available since the catalog are fetched from the `Climate-DT GitHub repository <https://github.com/DestinE-Climate-DT/Climate-DT-catalog>`_ .


Set up Jupyter kernel
---------------------

You need to register the kernel for the aqua environment to work with the AQUA 
package in Jupyter Hub on HPC systems.

Activate the environment and register the kernel with the following command:

.. code-block:: bash

    conda activate aqua
    python -m ipykernel install --user --name=aqua

.. note::

    It is possible that it will be necessary to install the ipykernel package in the environment.

.. warning::

    On Lumi you cannot use Miniforge to install the environment, so that this step is not possible.
    Please refer to the :ref:`container` section if you are working from a container
    or the :ref:`installation-lumi` section for more information on how to install AQUA
    specifically on Lumi.