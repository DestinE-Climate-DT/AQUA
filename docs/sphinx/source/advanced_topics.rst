.. _advanced-topics:

Advanced Topics
===============

.. _config-file:

Set up the configuration file
-----------------------------

A configuration file is available to specify the parameters for the AQUA package.
This is a YAML file called ``config-aqua.yaml`` and is located in the installation folder, which usually is ``$HOME/.aqua``.

.. warning::
  All the details of the configuration file are now handled during the installation process
  described in the :ref:`initialization` section. This is why this section is in the advanced topics.
  As a normal user you should not need to modify the configuration file manually.

The configuration file is used to specify the following parameters:

- **machine**: the machine on which the AQUA will run. This is used to specify the
  location of the grids, weights and areas produced by AQUA.
  By default no machine is set. You will need to specify the machine you want to use while installing AQUA (see :ref:`aqua-install`).
- **catalog**: the catalog on which the AQUA will run. This is used to specify the
  location of the AQUA catalog and the location of the data.
  By default no catalog is set. You will need to specify the catalog you want to use (see :ref:`aqua-add`).
- **reader**: this block contains catalog, fixes and grids location.
  These paths are required to be inside the AQUA repository,
  so these paths should not be changed unless strictly necessary.
  Refer to :ref:`add-data` for more information.
- **paths**: this is an optional block, which can be used to specify and override the paths specified in the catalog where grids, areas and weights are stored.
  This is useful if you want to store the grids, areas and weights in a different location than the default one, you don't have access to the default location or
  you are setting up a new machine.

  The paths block should have the following structure:

.. code-block:: yaml

    paths:
        grids: /path/to/aqua/data/grids
        weights: /path/to/aqua/data/weights
        areas: /path/to/aqua/data/areas

One or all the paths can be specified in the configuration file.
If a path is not specified in the configuration file, the default path specified in the catalog will be used.

The configuration folder has this structure:

.. code-block:: text

    ├── config
    │   ├── data_models
    │   ├── fixes
    │   ├── grids
    │   └── catalogs
    │       ├── climatedt-phase1
    │       │   ├── catalog 
    │       │   └── catalog.yaml
    │       │   └── machine.yaml
    │       ├── obs
    │       └── ...
    ├── config-aqua.yaml


.. _new-catalog:

Adding a new catalog
----------------------


Creation of the catalog folder
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add a new catalog to the AQUA catalog we need to create a
new folder that will contain the configuration files.

You can create the folder where you prefer and then add it to the
available catalogs with the ``aqua add`` command (see :ref:`aqua-add`).
This will copy or link the required files, allowing to have your custom catalog
folder under version control if needed.

.. code-block:: bash

    cd /path/of/your/catalog
    mkdir new_catalog

A machine specific file ``machine.yaml`` need to be created as a first step. This will include the path 
where the grids, weights and areas produced by AQUA will be stored. A default can be provided to be used in 
whatsoever machine where AQUA is installed, but also machine specific paths can be defined

.. code-block:: yaml

    default: 
        paths:
            grids: /path/to/aqua/data/grids
            weights: /path/to/aqua/data/weights
            areas: /path/to/aqua/data/areas
    myhpc: 
        paths:
            grids: /path/to/aqua/data/grids
            weights: /path/to/aqua/data/weights
            areas: /path/to/aqua/data/areas


Then, you will need to create the the ``catalog.yaml`` file, which is the main file for the catalog configuration.

.. code-block:: yaml

    sources:
        my-model:
            description: New model for a new catalog
            driver: yaml_file_cat
            args:
                path: "{{CATALOG_DIR}}/catalog/my-model/main.yaml"

In this example we're adding just one model, called ``my-model``.

Populating the catalog
^^^^^^^^^^^^^^^^^^^^^^^^

Let's assume that the new catalog has a new model called ``my-model`` defined before.
Let's create a new experiment with a new source for this model.

The file ``main.yaml`` should be created in the ``catalog/my-model`` directory.
This file will contain the informations about the experiments for the new model.

.. code-block:: yaml

    sources:
        my-exp:
            description: my first experiment for my-model
            driver: yaml_file_cat
            args:
                path: "{{CATALOG_DIR}}/my-exp.yaml"

Finally we can create the file ``my-exp.yaml`` in the same directory.
This is the file that will describe all the sources for the new experiment.
More informations about how to add them can be found in the :ref:`add-data` section.

Adding the catalog to the AQUA package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since ``v0.9`` the AQUA package has an entry point script that will allow to add a new catalog to the AQUA package.
This is done with the ``aqua add`` command.

.. code-block:: bash

    aqua add new_catalog -e /path/to/your/catalog/new_catalog

.. note::
    This command will create a symbolic link to the new catalog in the ``$AQUA/config/catalogs`` directory.
    See the :ref:`aqua-add` section for more information.

Download of grids
^^^^^^^^^^^^^^^^^

Grids used in AQUA are stored and available on Swift storage, powered by DKRZ.
A command line tool is available to download the grids from Swift on your machine.

Please refer to the section :ref:`grids-downloader` for more details.

.. _FDB_dask:

Dask access to FDB or GSV
--------------------------

If an appropriate entry has been created in the catalog, the reader can also read data from a FDB/GSV source. 
The request is transparent to the user (no apparent difference to other data sources) in the call.

.. code-block:: python

    reader = Reader(model="IFS", exp="control-1950-devcon", source="hourly-1deg")
    data = reader.retrieve(var='2t')

The default is that this call returns a regular dask-enabled (lazy) ``xarray.Dataset``,
like all other data sources.
This is performed by an intake driver for FDB which has been specifically developed from scratch inside AQUA.

In the case of FDB access specifying the variable is compulsory,
but a list can be provided and it is done for the FDB sources available in the catalog.
If not specified, the default variable defined in the catalog is used.

.. warning::

    The FDB access can be significantly fasten by selecting variables and time range.

An optional keyword, which in general we do **not** recommend to specify for dask access, is ``chunks``,
which specifies the chunk size for dask access.
Values could be ``D``, ``M``, ``Y`` etc. (in pandas notation) to specify daily, monthly and yearly aggregation.
It is best to use the default, which is already specified in the catalog for each data source.
This default is based on the memory footprint of single grib message, so for example for IFS-NEMO dative data
we use ``D`` for Tco2559 (native) and "1deg" streams, ``Y`` for monthly 2D data and ``M`` for 3D monthly data.
In any case, if you use multiprocessing and run into memory troubles for your workers, you may wish to decrease
the aggregation (i.e. chunk size).
It is also possible to specify vertical chunking by passing a dictionary with the keys ``time`` and ``vertical``.
In this case ``time`` will follow the notation discussed above, while ``vertical`` specifies the number of vertical
levels to use for each chunk.

.. _lev-selection-regrid:

Polytope access to Destination Earth data
-----------------------------------------

It is possible to access ClimateDT data available on the 'Databridge' for the DestinE ClimateDT also remotely, from other machines,
using the 'polytope' access. to this end you will need to specify ``engine="polytope"`` when instantiating the `Reader` or permanently, adding
the argument ``engine: polytope`` as an additional argument in the intake catalog source entry in the corresponding yaml file, under `args:`.

.. code-block:: python

    reader = Reader(model="IFS-NEMO", exp="ssp370", source="hourly-hpz7-atm2d", engine="polytope")
    data = reader.retrieve(var='2t')

This allows accessing ClimateDT data on the Databridge also remotely from other machines.

To access Destination Earth ClimateDT data you will need to be registered on the `Destine Service Platform  <https://platform.destine.eu/>`_
and have requested "upgraded access" to the data (follow the link "Access policy upgrade" under your username at the top left corner of the page).

In order for his to work you will need to store an access token in the file ``~/.polytopeapirc`` in your home directory.
You can create this file following two proceures:

1. **Using DestinE Service Platform credentials**: 

Follow the instructions in the `Polytope documentation <https://github.com/destination-earth-digital-twins/polytope-examples>`_
and the username and password which you defined for the Destine Service Platform to download the credentials into this file. 

2. **Using ECMWF credentials**:

You can also use your ECMWF credentials to access the data. You will find the email and key which you need by logging in to your `ECMWF account <https://www.ecmwf.int/>`_.
After logging in you will find your key at `https://api.ecmwf.int/v1/key/ <https://api.ecmwf.int/v1/key/>`_.

A sample ``~/.polytopeapirc`` file will look like this:

.. code-block:: text

    {
        "user_email" : "<your.email>",
        "user_key" : "<your.token>"
    }

.. note::
    Please notice that the two procedures use different tokens and that in the first case there will be no `"user_email"` in the polytope credentials file.

Level selection and regridding
------------------------------

Here there are a few notes of caution about regrid 3D data with level selection.
Please check the section :ref:`lev-selection` to first understand how to select levels
while instantiating the Reader.

When reading 3D data the reader also adds an additional coordinates with prefix ``idx_``
and suffix the names of vertical dimensions to the Dataset.
These represent the indices of the (possibly selected) levels in the original archive.
This hidden index helps the regridder to choose the appropriate weights for each level even if a level
selection has been performed.

This means that when regridding 3D data the regridding can be performed first on a full dataset and then
levels are selected or vice versa.
In both cases the regridding will be performed using the correct weights.
By default in xarray when a single vertical level is selected the vertical dimension is dropped, but
the regridder is still able to deal with this situation using the information in the hidden index.

.. warning::
    Please avoid performing regridding on datasets in which single levels have been selected for multiple
    3D variables using different vertical dimensions or on datasets containing also 2D data,
    because in such cases it may not be possible to reconstruct which vertical dimension
    each variable was supposed to be using. 
    In these cases it is better to first select a variable, then select levels and finally regrid. 
    The regridder will issue a warning if it detects such a situation.
    An alternative is to maintain the vertical dimension when selecting a single level by specifying a list with one element,
    for example using ``isel(nz1=[40])`` instead of ``isel(nz1=40)``.
    If level selection was performed at the ``retrieve()`` stage this is not a problem,
    since in that case the vertical level information is preserved by producing 3D variables
    with a single vertical level.

Reader prepocessing option
--------------------------

The reader has a preprocessing option that can be used to apply a function to the data before it is retrieved.
This can be useful to apply a function to the data before it is read, to quickly apply some function that is not available in the Reader class.

In order to use this option, the user must pass a function as ``preproc`` keyword while instantiating the Reader.

.. code-block:: python

    def my_preproc(data):
        return data * 2

    reader = Reader(model="IFS", exp="control-1990", source="lra-r100-monthly", preproc=my_preproc)
    data = reader.retrieve(var='2t')

.. note::
    There is not yet a way to define a preproc function in the catalog, so it must be passed as a keyword argument.
    This is a feature that will be added in the future, if needed.

.. _new-machine-regrid:

Enable regrid capabilities in a new machine
-------------------------------------------

If AQUA has been installed in a machine where the grids are not available yet, some extra step may be needed to enable the regrid capabilities.

Set the machine in the catalog machine file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Every catalog has a machine file that specifies the paths where the grids, weights and areas produced by AQUA will be stored for each machine.
If you want to use such a catalog in a new machine, you need to add the machine to the catalog machine file.
This is contained in the ``config/catalogs/<catalog-name>/machine.yaml`` file.
The block to add should look like this:

.. code-block:: yaml

    myhpc: 
        paths:
            grids: /path/to/aqua/data/grids
            weights: /path/to/aqua/data/weights
            areas: /path/to/aqua/data/areas

Where ``myhpc`` is the name of the machine used during the ``aqua install <myhpc>`` command.

Download the grids
^^^^^^^^^^^^^^^^^^

The grids used in AQUA are stored and available on Swift storage, powered by DKRZ.
See the :ref:`grids-downloader` section for more details.

You can then check the completeness of the grids with the tool described in the :ref:`grids-checker` section.

.. _dev-notes:

Developer notes
---------------

The standard setup of AQUA is thought to be used in a conda environment by users who are not going to modify under version control the downloaded catalogs.
For this reason we suggest to install the AQUA configuration files in the ``$HOME/.aqua``. 
Anyway, this configuration could be not ideal if you're creating a new catalog or modifying an existing one and you want to keep it under version control.
For this reason the following steps are suggested to set up the AQUA package in a developer environment.

Set up environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since ``v0.9`` the AQUA package has an entry point script that can be used to copy the configuration files
and the catalog to an external directory (see :ref:`aqua-install` and :ref:`aqua-console`).

By default the configuration files are stored in the ``$HOME/.aqua`` directory.
Same for the catalog, which is stored in the ``$HOME/.aqua/catalogs`` directory.
This has been done to make the package more user-friendly, expecially when installing the package
from a conda environment or from a pip package.

A developer may want to keep the configuration files and the catalogs in a different directory,
for this reason the ``aqua init`` command can be used to copy the configuration files and the catalog
to a different directory. For more information see the :ref:`aqua-install` section.

If you're using a custom directory to store the configuration files and the catalog it is recommended
to set up an environment variable to specify the path to the AQUA package.
This can be done by adding the following line to your `.bashrc` or `.bash_profile` file:

.. code-block:: bash

    export AQUA_CONFIG=/path/to/config_files

This will make clear for the code where to find the AQUA catalog and the configuration files.

.. note::
    It is temporalily possible to set the environment variable ``AQUA`` to specify the path of the source code,
    so that the entire new aqua entry point can be superseeded by the old method.
    This will be removed in the next release.

Add new catalogs as developer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you're adding a new catalog or modifying an existing one it is recommended to use the old method to set up the AQUA package
or to add the catalog with the editable option.
Please refer to the :ref:`aqua-add` section for more information.

.. _eccodes:

ecCodes
-------

ecCodes is a package developed by ECMWF to handle GRIB and BUFR files.
AQUA uses ecCodes to interpret the GRIB files coming from the FDB sources.
This is handled by the intake driver for FDB sources developed inside AQUA and making use of ecCodes definitions and the GSVRetriever class.

Since v0.13 AQUA uses always the ecCodes definitions selected in the environment file. In particular we're currently using ecCodes 2.39.0.
There is the possibility to switch ecCodes version while opening a source written with an older ecCodes but this is not recommended.
As a consequence of this default behaviour, the shortnames deduced from a paramid will be always referred to the ecCodes definitions used by AQUA
and not to the definitions used by the source. If the fixer is used, the shortnames will be anyway converted to the standard variable names used in AQUA.

ecCodes fixer
^^^^^^^^^^^^^

.. warning::

    Deprecated starting from AQUA v0.13

In order to be able to read data written with recent versions of ecCodes,
AQUA needs to use a very recent version of the binary and of the definition files.
Data written with earlier versions of ecCodes should instead be read using previous definition files.
AQUA solves this problem by switching on the fly the definition path for ecCodes, as specified in the source catalog entry. 
Starting from version 2.34.0 of ecCodes older definitions are not compatible anymore.
As a fix we create copies of the original older definion files with the addition/change of 5 files (``stepUnits.def`` and 4 files including it).
A CLI script (``eccodes/fix_eccodes.sh``) is available to create such 'fixed' definition files.

.. warning::

    This change is necessary since AQUA v0.11.1 and it is going to be not necessary anymore starting from AQUA v0.13.
    Please notice that this also means that earlier versions of the ecCodes binary will not work using these 'fixed' definition files.
    If you are planning to use older versions of AQUA (with older versions of ecCodes) you should not use these 'fixed' definition files
    and you may need to modify the ecCodes path in the catalog entries.