Configuration
=============

AQUA configuration is based on a configuration folder ``config``, which includes a few different files, both local and machine-dependent. 
The ``config-aqua.yml`` includes the most crucial path to be stored.

Then, machine-dependent folders are used to specify the data access properties and the AQUA functions as the interpolation features.

The ``config`` folder is structured as follows:


.. code-block:: text

    ├── config
    │   ├── data_models
    │   ├── fixes
    │   └── machines
    │       ├── levante
    !       │   ├── catalog 
    │       │   ├── catalog.yaml
    │       │   └── regrid.yaml
    │       ├── lumi
    │       └── ...
    ├── aqua-grids.yaml
    ├── config-aqua.yaml
    
In the ``config-aqua.yaml`` file, the following paths are specified:

.. code-block:: yaml

    machine: levante # machine name, AQUA will look for a folder with the same name in the machines folder

    reader: # path to the reader configuration file
      catalog: '{configdir}/machines/{machine}/catalog.yaml'
      fixer: '{configdir}/fixes'

    cdo: # CDO path is found automatically with which if not specified here
      levante: /home/m/m214003/local/bin/cdo # CDO 2.2.0 by Uwe Schulzweida (HEALPix support)

In the ``aqua-grids.yaml`` file, the available grids are described.
This file is used by the AQUA ``Reader`` to identify the grid of the dataset and to interpolate the data to the target grid.
The grid name is used to identify the grids in the metadata of each available source (see :doc:`adding_data`).

The ``data_models`` folder contains the data model configuration files, which are used to specify the data model of the datasets.
The ``fixes`` folder contains the fixer configuration files used to fix the data in terms of variable names and units.

Each machine folder contains a ``catalog.yaml`` file specifying the data access properties. 
The ``catalog`` folder inside each ``machine`` folder contains intake catalogues for individual datasets.

For a more detailed description of the content in the machine folder, see the section on :doc:`adding_data`.

By default the AQUA ``Reader`` looks for its configuration files in a ``config`` folder in a series of predefined directory, with the following order:

- An environment variable defined as ``$AQUA``
- The current directory
- An absolute path based on the location of the ``aqua`` package
- A ``.aqua/config`` folder

This gives the user the freedom to run scripts and notebooks from different locations without worrying about the location of configuration files. 
