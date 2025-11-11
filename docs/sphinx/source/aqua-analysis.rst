.. _aqua_analysis:

AQUA analysis wrapper
=====================

A wrapper containing calls to all the state-of-the-art diagnostic available in AQUA.

.. note::

    Since ``v0.18.0``, the AQUA analysis tool is available as part of the AQUA console (see :ref:`aqua-console`).
    If you are using an older version of AQUA, you can find it in the ``cli/aqua-analysis`` folder.

Basic usage
-----------

.. code-block:: bash

    aqua analysis <options>

Without any argument, the script will run all the diagnostics available in AQUA on an hard-coded dataset,
with LUMI configuration and output directory in the ``./output`` folder.

All the diagnostic logfiles will be saved in this main folder, while the diagnostics output will be saved in subfolders
named after the diagnostic name.
Inside each diagnostic folder, the output will be saved in a subfolder named with the filetype (e.g. ``pdf``, ``netcdf``).

The exact list of diagnostics to run and technical details of the analysis
(such as the number of workers/thread/memory to use for the dask cluster) are specified in the configuration file ``config.aqua-analysis.yaml`` in the same folder.
This file is available in the ``$AQUA/config/analysis`` folder and it is installed with AQUA (see :ref:`aqua-install`).

Additional options
------------------

Some options are available to launch the script without having to modify the config file,
so that the script can be used in a batch job or in a workflow. These override corresponding options in the config file.

.. option:: -c <catalog>, --catalog <catalog>

    The catalog to use. If a default is not specified in the configuration file, 
    then the default first catalog installed is used.

.. option:: -m <model>, --model <model>

    The  model to use.

.. option:: -e <exp>, --exp <exp>

    The experiment to use.

.. option:: -s <source>, --source <source>

    The source to use.

.. option:: --source_oce <source_oce>

    Additional ocean source to use for diagnostics accepting it (currently only ECmean).

.. option:: -f <config>, --config <source>

    The config file to use.

.. option:: --regrid <target_grid>

    The target grid to use for regridding the data.
    If not specified, the default is ``null``, which means no regridding will be applied.

.. option:: --realization <realization>

    The realization to use. If not specified or set to ``None``,
    no realization argument will be passed to the diagnostics.

.. option:: --startdate <YYYY-MM-DD>

    Start date to limit the time range for the analysis.
    If not specified, all available data from the beginning will be used.
    Can be specified in the format YYYY-MM-DD.

.. option:: --enddate <YYYY-MM-DD>

    End date to limit the time range for the analysis.
    If not specified, all available data until the end will be used.
    Can be specified in the format YYYY-MM-DD.

.. option:: -d <dir>, --outputdir <dir>

    The output directory to use. 
    The default in the config file is ``$AQUA/cli/aqua-analysis/output``.
    Better to use an absolute path.

.. option:: -l <loglevel>, --loglevel <loglevel>

    The log level to use for the cli and the diagnostics.
    Default is ``WARNING``.

.. option:: -t <threads>, --threads <threads>

    This is the number of diagnostics running in parallel.
    Default is ``0``, which means no limit.

.. option:: -p, --parallel

    This flag activates running the diagnostics with multiple dask.distributed workers.
    By default the script will set up a common dask cluster/scheduler and close it when finished.
    
.. option:: --local_clusters
    
    This is a legacy feature to run the diagnostics with multiple dask.distributed 'local' clusters (not reccomended)
    In this case predefined number of workers is used for each diagnostic, set in the configuration file `config.aqua-analysis.yaml`.
    
.. note ::

    By default the script will run all the state-of-the-art diagnostics available in AQUA.
    It is possible to run only a subset of the diagnostics by modifying the ``run`` key in the configuration file.

Configuration file
------------------

The configuration file ``config.aqua-analysis.yaml`` contains the list of diagnostics to run and technical details of the analysis.
If a configuration is available also as a command line argument, the command line argument will take precedence.

The configuration file is divided in three main sections:

- ``job``: contains the technical details of the analysis.
- ``cluster``: contains the details of the dask cluster to use.
- ``diagnostics``: contains the list of diagnostics to run.

.. note::

    The configuration file allows for the definition of a custom folder path where the individual diagnostics configuration files are stored.
    This is done by setting an environment variable ``AQUA_CONFIG``.

Job
^^^

The job section contains the following keys:

- ``max_threads``: the maximum number of diagnostics running in parallel. Leave it to 0 for no limit
- ``loglevel``: the log level to use for the cli and the diagnostics. Default is ``WARNING``
- ``run_checker``: a boolean flag to activate the checker diagnostic. Default is ``true``
- ``outputdir``: the output directory to use. Default is ``$AQUA/cli/aqua-analysis/output``
- ``catalog``: the catalog to use. Default is ``null``
- ``model``: the model to use. Default is ``IFS-NEMO``
- ``exp``: the experiment to use. Default is ``historical-1990``
- ``source``: the source to use. Default is ``lra-r100-monthly``
- ``regrid``: the target grid to use for regridding the data. Default is ``null``, which means no regridding will be applied.
- ``script_path_base``: the base path for the diagnostic scripts. Default is ``${AQUA}/diagnostics``, but it is going to be updated.
- ``startdate``: the start date to limit the time range for the analysis. Default is ``null``.
- ``enddate``: the end date to limit the time range for the analysis. Default is ``null``.

.. note::

    The ``catalog``, ``model``, ``exp`` and ``source`` keys are used only if the corresponding command line arguments are not provided.

Cluster
^^^^^^^

The cluster section contains the following keys:

- ``workers``: the number of workers to use. Default is ``32``.
- ``threads``: the number of threads per worker. Default is ``2``.
- ``memory_limit``: the memory per worker. Default is ``7GiB``.
- ``reconnect_timeout``: the timeout in seconds to wait for client to connect to the cluster.
                        Default is ``120``.
                        Can be overridden also setting an environment variable: ``DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT=120s``.
- ``ftp_timeout``: the timeout in seconds for ftp connections.
                        Default is ``60``.
                        Can be overridden also setting an environment variable: ``DASK_DISTRIBUTED__COMM__TIMEOUTS__FTP=60s``

.. note::

    These values are optimized for LUMI. If you are running the script on a different machine, you may want to change them.

Diagnostics
^^^^^^^^^^^

The diagnostics section contains the list of diagnostics to run.
A ``run`` list contains the diagnostics to run. By default, all the diagnostics are in this list.

The diagnostics are specified as a dictionary with the following keys:

- ``nworkers``: the number of workers to use for this diagnostic.
- ``script_path``: the relative path to the diagnostic script with respect to ``script_path_base``. 
- ``config``: the configuration file for the diagnostic.
- ``nocluster``: a boolean flag to disable the use of the global dask cluster for this diagnostic (used by ECmean)
- ``source_oce``: a boolean flag to pass the additional ocean source to the diagnostic (currently only ECmean). Defaults to False.
- ``extra``: a string with extra arguments to pass to the diagnostic script.
- ``outname``: the name of the output folder if different from the diagnostic name.