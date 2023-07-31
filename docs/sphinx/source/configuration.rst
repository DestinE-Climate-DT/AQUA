Configuration
=============

AQUA configuration is based on a configuration folder `config`, which includes a few different files, both local and machine-dependent. 
The `config.yml` includes the most crucial path to be stored.

Then, machine-dependent folders are used to specify the data access properties and the AQUA functions as the interpolation features.

The `config` folder is structured as follows:


.. code-block:: text

    ├── config
    │   ├── data_models
    │   ├── fixes
    │   └── machines
    ├── config.yaml
    
In the `config.yaml` file, the following paths are specified:

.. code-block:: yaml

    machine: levante # machine name, AQUA will look for a folder with the same name in the machines folder

    reader: # path to the reader configuration file
      catalog: '{configdir}/machines/{machine}/catalog.yaml'
      regrid: '{configdir}/machines/{machine}/regrid.yaml'
      fixer: '{configdir}/fixes'

    cdo: # path to the cdo executable, that is necessary for generation interpolation weights
      levante: /sw/spack-levante/cdo-2.0.6-jkuh4i/bin/cdo
      lumi: /scratch/project_465000454/devaraju/SW/LUMI-22.08/C/python-xarray/bin/cdo



The `data_models` folder contains the data model configuration files, which are used to specify the data model of the datasets.
The `fixes` folder contains the fixer configuration files used to fix the data in terms of variable names and units.

Each machine folder contains a `catalog.yaml` file specifying the data access properties
and a `regrid.yaml` file specifying the interpolation properties. 
The `catalog` folder inside each `machine` folder contains intake catalogues for individual datasets.

For a more detailed description of the content in the machine folder, see the section on :doc:`adding_data`.

By default the AQUA `Reader` looks for its configuration files in a `config` folder in a series of predefined directory, with the following order:

- An environment variable defined as `$AQUA`
- The current directory
- A 3-level parent directory hierarchy (.., ../.., ../../..)
- A `.aqua/config` folder

This gives the user the freedom to run scripts and notebooks from different locations without worrying about the location of configuration files. 
In any case it is also possible to pass explicitly a `configdir` keyword when you instantiate an AQUA `Reader` object.
