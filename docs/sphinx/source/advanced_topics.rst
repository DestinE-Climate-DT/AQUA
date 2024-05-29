.. _advanced-topics:

Advanced Topics
===============

Set up the configuration file
-----------------------------

A configuration file is available to specify the parameters for the AQUA package.
This is a YAML file called ``config-aqua.yaml`` and is located in the configuration folder.

.. warning::
  All the details of the configuration file are now handled during the installation process
  described in the :ref:`initialization` section. This is why this section is in the advanced topics.
  As a normal user you should not need to modify the configuration file manually.

The configuration file is used to specify the following parameters:

- **machine**: the machine on which the code is running. This is used to specify the
  location of the AQUA catalogue and the location of the data. Default is ``lumi``.
  Other options are ``ci`` and ``levante``. Custom machines can be defined (see :ref:`_new-catalogue`).
- **reader**: this block contains catalogue, fixes and grids location.
  These paths are required to be inside the AQUA repository,
  so these paths should not be changed unless strictly necessary.
  Refer to :ref:`add-data` for more information.
- **cdo**: location of the CDO executable. By default this option is not needed, since CDO is required in the ``environment.yml`` file
  and provided by conda.

The configuration folder has this structure:

.. code-block:: text

    ├── config
    │   ├── data_models
    │   ├── fixes
    │   ├── grids
    │   └── machines
    │       ├── lumi
    │       │   ├── catalog 
    │       │   └── catalog.yaml
    │       ├── levante
    │       └── ...
    ├── config-aqua.yaml

.. note::
  The machine depencency in files and folders will be removed in future versions of AQUA.

.. _new-catalogue:

Adding a new catalogue
----------------------

.. warning::

    The machine depencency in configuration files will be removed in the next release.
    If you want to add a new machine, please follow the instructions below, but be aware
    of the changes we are introducing in the section :ref:`aqua-console`.

.. Change the machine name
.. ^^^^^^^^^^^^^^^^^^^^^^^

.. Let's assume that the new machine to configure is called ``new_machine``.
.. The first step is to change the machine name in the ``config-aqua.yaml`` file,
.. which is located in the ``$AQUA/config`` directory.

.. .. code-block:: yaml

..     machine: new_machine

Creation of the catalogue folder
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To add a new machine to the AQUA catalogue we need to create a
new folder that will contain the configuration files for the new machine.

You can create the folder where you prefer and then add it to the
available catalogues with the ``aqua add`` command (see :ref:`aqua-add`).
This will copy or link the required files, allowing to have your custom catalogue
folder under version control if needed.

.. code-block:: bash

    cd /path/of/your/catalogue
    mkdir new_catalogue

This will contain the ``catalog.yaml`` file, which is the main file for the machine configuration.

.. code-block:: yaml

    paths:
        grids: /path/to/aqua/data/grids
        weights: /path/to/aqua/data/weights
        areas: /path/to/aqua/data/areas

    sources:
        my-model:
            description: New model for a new machine
            driver: yaml_file_cat
            args:
                path: "{{CATALOG_DIR}}/catalog/my-model/main.yaml"

In this example we're adding just one model, called ``my-model``.

Populating the catalogue
^^^^^^^^^^^^^^^^^^^^^^^^

Let's assume that the new catalogue has a new model called ``my-model`` defined before.
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

Adding the catalogue to the AQUA package
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since ``v0.8.2`` the AQUA package has an entry point script that will allow to add a new catalogue to the AQUA package.
This is done with the ``aqua add`` command.

.. code-block:: bash

    aqua add new_catalogue -e /path/to/your/catalogue/new_catalogue

.. note::
    This command will create a symbolic link to the new catalogue in the ``$AQUA/config/machines`` directory.
    See the :ref:`aqua-add` section for more information.

Download of grids
^^^^^^^^^^^^^^^^^

Grids used in AQUA are stored and available on Swift storage, powered by DKRZ.
A command line tool is available to download the grids from Swift on your machine.

Please refer to the section :ref:`grids-downloader` for more details.

.. _FDB_dask:

Dask access to FDB or GSV
--------------------------

If an appropriate entry has been created in the catalogue, the reader can also read data from a FDB/GSV source. 
The request is transparent to the user (no apparent difference to other data sources) in the call.

.. code-block:: python

    reader = Reader(model="IFS", exp="control-1950-devcon", source="hourly-1deg")
    data = reader.retrieve(var='2t')

The default is that this call returns a regular dask-enabled (lazy) ``xarray.Dataset``,
like all other data sources.
This is performed by an intake driver for FDB which has been specifically developed from scratch inside AQUA.

In the case of FDB access specifying the variable is compulsory,
but a list can be provided and it is done for the FDB sources available in the catalogue.
If not specified, the default variable defined in the catalogue is used.

.. warning::

    The FDB access can be significantly fasten by selecting variables and time range.

An optional keyword, which in general we do **not** recommend to specify for dask access, is ``chunks``,
which specifies the chunk size for dask access.
Values could be ``D``, ``M``, ``Y`` etc. (in pandas notation) to specify daily, monthly and yearly aggregation.
It is best to use the default, which is already specified in the catalogue for each data source.
This default is based on the memory footprint of single grib message, so for example for IFS-NEMO dative data
we use ``D`` for Tco2559 (native) and "1deg" streams, ``Y`` for monthly 2D data and ``M`` for 3D monthly data.
In any case, if you use multiprocessing and run into memory troubles for your workers, you may wish to decrease
the aggregation (i.e. chunk size).
It is also possible to specify vertical chunking by passing a dictionary with the keys ``time`` and ``vertical``.
In this case ``time`` will follow the notation discussed above, while ``vertical`` specifies the number of vertical
levels to use for each chunk.

.. _iterators:

Iterator access to data
-----------------------

In alternative to the lazy access it is also possible to ask the reader to return an *iterator/generator* object passing the ``stream_generator=True`` 
keyword to the ``retrieve()`` method.
In that case the next block of data can be read from the iterator with ``next()`` as follows:

.. code-block:: python

    reader = Reader(model="IFS", exp="fdb-tco399", source="fdb-long", aggregation="D",
                    regrid="r025")
    data = reader.retrieve(startdate='20200120', enddate='20200413', var='ci',
                           stream_generator=True)
    dd = next(data)

or with a loop iterating over ``data``. The result of these operations is in turn a regular xarray.Dataset containg the data.
Since this is a data stream the user should also specify the desired initial time and the final time (the latter can be omitted and will default to the end of the dataset).
When using an iterator it is possible to specify the size of the data blocks read at each iteration with the ``aggregation`` keyword
(``M`` is month, ``D``is day etc.). 
The default is ``S`` (step), i.e. single saved timesteps are read at each iteration.

Please notice that the resulting object obtained at each iteration is not a lazy dask array, but is instead entirely loaded into memory.
Please consider memory usage in choosing an appropriate value for the ``aggregation`` keyword.

In the special case where the source is FDB/GSV and iterator access is requested, ``aggregation`` takes precedence over ``chunks`` and chunking is set to the value specified by it.

.. _lev-selection-regrid:

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

.. _slurm:

Slurm utilities
---------------

The ``aqua.slurm`` module is based on the ``dask_jobqueue`` `package <https://jobqueue.dask.org/en/latest/>`_.
This package makes easy to run Dask on job-queuing systems in HPC environments.
It has a simple interface accessible from interactive systems like Jupyter Notebooks or batch Jobs.

The Slurm Class
^^^^^^^^^^^^^^^

The ``aqua.slurm`` module contains several functions that allow us to create and operate Dask jobs:

- ``squeue``: Allows us to check the status of created jobs in the queue.
- ``job``: Allows the creation and submission of a job to a selected queue.
- ``scancel``: Allows the cancellation of all submitted jobs or only a job with a specified Job_ID.


Dask-Job Initialization
^^^^^^^^^^^^^^^^^^^^^^^

The ``job()`` function can be used to launch a job to the queue directly from a notebook cell.
This function leverages the ``dask_jobqueue.SLURMCluster`` for initializing and managing Dask jobs on SLURM-managed clusters.

.. code-block:: python

    slurm.job(machine_name='lumi')


Submitting Jobs on Different Machines
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``job()`` function provides a flexible and efficient way to submit jobs to SLURM-managed clusters on different machines.
Users can specify machine-specific configurations through a YAML configuration file (``.aqua/aqua/slurm/config-slurm.yml``) or provide parameters directly through the function call.

The ``job()`` function allows users to either use predefined settings from a YAML file for known machines or manually input job parameters for machines without predefined settings. 
Here's how to use the function for different scenarios:


Submit a Job Using Predefined Configurations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the machine has predefined settings in the YAML configuration file, simply specify the machine's name:

.. code-block:: python

    slurm.job(machine_name='lumi')

This method pulls all necessary parameters like memory, cores, and walltime from the YAML file associated with the specified machine name.

.. note::

    The available machines are Lumi, Levante, Atos (HPC2020) and Mafalda.
    Please be aware that the user or project fields may be specific of the Destination Earth project and may need to be changed.

Submit a Job with Maximum Available Resources per Node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To utilize the maximum available resources per node for the selected queue, set ``max_resources_per_node=True``:

.. code-block:: python

    slurm.job(machine_name='lumi', max_resources_per_node=True)


Change default attributes
^^^^^^^^^^^^^^^^^^^^^^^^^

If a machine is defined then default arguments are used for a simple call of the ``slurm.job()`` function.
On Lumi for example they are as follows:

.. code-block:: yaml

    machines:
      lumi:
        queue: 'small'
        account: 'project_465000454'
        walltime: '02:30:00'
        memory: '10 GB'
        cores: 1
        jobs: 1
        loglevel: 'WARNING'
        path_to_output: '.'

.. note::

    The ``slurm.job()`` function has an argument ``exclusive=False`` by default.
    Setting ``exclusive=True`` reserves an entire node for the job.

If you would like to reserve a node on a different queue, specify the queue's name as an argument of the function:

.. code-block:: python

    slurm.job(machine_name='lumi', queue="small")

.. warning::

    The `exclusive` argument **does not** automatically provide the maximum available memory, number of cores, and walltime.
    Anyway for some machines you will be billed for the entire node every time you ask exclusive access to it.

The ``slurm.job()`` function has an argument ``max_resources_per_node``, which is ``False`` by default.
Setting ``max_resources_per_node=True`` will allocate the maximum number of cores, memory, and walltime available for the chosen node.


Path to the Output
^^^^^^^^^^^^^^^^^^

The ``slurm.job()`` function creates folders for the job output.
By default, the path is ``"."``.
Therefore, the paths for log and output are:

- ``./slurm/logs`` for errors,
- ``./slurm/output/`` for output.

Users can specify different paths for the SLURM output:

.. code-block:: python

    slurm.job(machine_name='lumi', path_to_output="/any/other/folder/")


Canceling the Dask Job
^^^^^^^^^^^^^^^^^^^^^^

The user can cancel all submitted jobs by:

.. code-block:: python

    slurm.scancel()

If the user would like to cancel a specific job, they need to know the Job_ID of that job.
The Job_ID can be found using the ``slurm.squeue()`` function, which returns information about all user SLURM jobs on the machine.
Then, the user can cancel the particular job as:

.. code-block:: python

    slurm.scancel(all=False, Job_ID=5000000)

.. warning::

It is potentially dangerous to cancel all your jobs. Always prefer to cancel jobs with the Job_ID.


Modifying and Adding Machine Configurations in YAML
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To modify existing configurations or add new machines, edit the ``.aqua/aqua/slurm/config-slurm.yml`` file:

1. Open the YAML file and locate the machines section.
2. Modify or add entries for machines. For example, to add a new machine configuration:

.. code-block:: yaml

    machines:
      mafalda:
        queue: 'batch'
        account: null
        walltime: '02:30:00'
        memory: '10 GB'
        cores: 1
        jobs: 1
        loglevel: 'WARNING'
        path_to_output: '.'

.. note::

    Currently, the pip installation does not copy the YAML configuration file to a user-accessible directory.
    This functionality will be updated in the future to ensure easier modification of configurations by users.

.. _dev-notes:

Developer notes
---------------

The standard setup of AQUA is thought to be used in a conda environment by users who are not going to modify under version control the downloaded catalogues.
For this reason we suggest to install the AQUA configuration files in the ``$HOME/.aqua``. 
Anyway, this configuration could be not ideal if you're creating a new catalogue or modifying an existing one and you want to keep it under version control.
For this reason the following steps are suggested to set up the AQUA package in a developer environment.

Set up environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since ``v0.8.2`` the AQUA package has an entry point script that can be used to copy the configuration files
and the catalogue to an external directory (see :ref:`aqua-init` and :ref:`aqua-console`).

By default the configuration files are stored in the ``$HOME/.aqua`` directory.
Same for the catalogue, which is stored in the ``$HOME/.aqua/machines`` directory.
This has been done to make the package more user-friendly, expecially when installing the package
from a conda environment or from a pip package.

A developer may want to keep the configuration files and the catalogues in a different directory,
for this reason the ``aqua init`` command can be used to copy the configuration files and the catalogue
to a different directory. For more information see the :ref:`aqua-init` section.

If you're using a custom directory to store the configuration files and the catalogue it is recommended
to set up an environment variable to specify the path to the AQUA package.
This can be done by adding the following line to your `.bashrc` or `.bash_profile` file:

.. code-block:: bash

    export AQUA_CONFIG=/path/to/config_files

This will make clear for the code where to find the AQUA catalogue and the configuration files.

.. note::
    It is temporalily possible to set the environment variable ``AQUA`` to specify the path of the source code,
    so that the entire new aqua entry point can be superseeded by the old method.
    This will be removed in the next release.

Add new catalogues as developer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you're adding a new catalogue or modifying an existing one it is recommended to use the old method to set up the AQUA package
or to add the catalogue with the editable option.
Please refer to the :ref:`aqua-add` section for more information.
