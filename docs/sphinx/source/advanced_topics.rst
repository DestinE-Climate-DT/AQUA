.. _advanced-topics:
Advanced Topics
===============

.. _new-machine:
Adding a new machine
--------------------

Change the machine name
^^^^^^^^^^^^^^^^^^^^^^^

Let's assume that the new machine to configure is called ``new_machine``.
The first step is to change the machine name in the ``config-aqua.yaml`` file,
which is located in the ``$AQUA/config`` directory.

.. code-block:: yaml

    machine: new_machine

Creation of the catalogue folder
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Then to add a new machine to the AQUA catalogue we need to create a
new folder that will contain the configuration files for the new machine.

The folder should be created in the ``config`` directory.

.. code-block:: bash

    cd aqua/config
    mkdir new_machine

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

Let's assume that the new machine has a new model called ``my-model`` defined before.
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

An optional keyword, which in general we do **not** recommend to specify for dask access, is ``aggregation``,
which specifies the chunk size for dask access.
Values could be ``D``, ``M``, ``Y`` etc. (in pandas notation) to specify daily, monthly and yearly aggregation.
It is best to use the default, which is already specified in the catalogue for each data source.
This default is based on the memory footprint of single grib message, so for example for IFS-NEMO dative data
we use ``D`` for Tco2559 (native) and "1deg" streams, ``Y`` for monthly 2D data and ``M`` for 3D monthly data.
In any case, if you use multiprocessing and run into memory troubles for your workers, you may wish to decrease
the aggregation (i.e. chunk size).

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

The aqua.slurm module is based on the `dask_jobqueue package <https://jobqueue.dask.org/en/latest/>`_.
The Dask-jobqueue makes it easy to run Dask on job-queuing systems in high-performance supercomputers.
It has a simple interface accessible from interactive systems like Jupyter Notebooks or batch Jobs.

The Slurm Class
^^^^^^^^^^^^^^^^^

The aqua.slurm module contains the ``slurm`` class, which allows us to create and operate the dask-jobs.
The ``slurm`` class has the following main functions:

- ``squeue``: allows us to check the status of created Jobs in the queue,
- ``job``: allows the creation and submission of the Job to a selected queue,
- ``scancel``: allows to cancel of all submitted Jobs or only Job with specified Job_ID.


The dask-job initialization 
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The job can be launched to the queue with the following command in a Notebook cell:

.. code-block:: python

	slurm.job()
 

The default arguments of ``slurm.job()`` function on Levante
(``machine=Levante`` in configdir) are the followings:

.. code-block:: python

	account = "bb1153"
	queue = "compute"
	cores=1
	memory="10 GB"
	path_to_output="."
	exclusive=False

The default arguments of ``slurm.job()`` function,
i.e., account and queue names, are different for Lumi (``machine=Lumi`` in configdir):

.. code-block:: python

	account = "project_465000454"
	queue = "small"

The function ``slurm.job()`` has an argument ``exclusive=False`` by default.
Exclusive argument ``exclusive=True`` is reserving an entire node for the Job.

If you would like to reserve a node on a different queue,
specify the queue's name as an argument of the function:

.. code-block:: python

	slurm.job(queue="gpu")


.. warning::
	The exclusive argument **does not** automatically provide us the maximum available memory,
    number of cores, and walltime.

The function ``slurm.job()`` has an argument ``max_resources_per_node``, False by default.
If we set the argument to ``max_resources_per_node=True``, the number of cores, memory,
and walltime will equal the maximum available for the choosen node.

Path to the output
^^^^^^^^^^^^^^^^^^

The function slurm.job() creates the folders for the job output.
By default, the path is ``".""``. 
Therefore, the paths for log and output are: 

- ``./slurm/logs`` for the errors,
- ``./slurm/output/`` for the output.

Users can specify the different paths for the SLURM output:

.. code-block:: python

	slurm.job(path_to_output="/any/other/folder/")


Cancel the dask-job
^^^^^^^^^^^^^^^^^^^

The user can cancel all submitted Jobs by

.. code-block:: python
	
	slurm.scancel()

If the user would like to cancel the specific Job, he needs to know the Job_ID of that Job. 
The Job_ID can be found with the function slurm.squeue(),
which returns the information about all user Slurm Jobs on the machine. 
Then the user can cancel the particular Job as:

.. code-block:: python

	slurm.scancel(all=False, Job_ID=5000000)

.. warning::
    It is potentially dangerous to cancel all your jobs,
    always prefer to cancel jobs with the Job_ID