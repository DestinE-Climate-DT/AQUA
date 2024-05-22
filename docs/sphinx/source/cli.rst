.. _cli:

Command Line Interface tools
============================

This sections describes the series of Command Line Interface (CLI) tools currently available in AQUA.
It includes software with a variety of goals, which are mostly made for advanced usage. 

.. _aqua_analysis:

AQUA analysis wrapper
---------------------

A wrapper containing calls to all the state-of-the-art diagnostic available in AQUA
is provided in the ``cli/aqua-analysis/`` folder.

Basic usage
^^^^^^^^^^^

.. code-block:: bash

    bash aqua-analysis.sh

Without any argument, the script will run all the diagnostics available in AQUA on an hard-coded dataset,
with LUMI configuration and output directory in the ``cli/aqua-analysis/output`` folder.

All the diagnostic logfiles will be saved in this main folder, while the diagnostics output will be saved in subfolders
named after the diagnostic name.
Inside each diagnostic folder, the output will be saved in a subfolder named with the filetype (e.g. ``pdf``, ``netcdf``).

Additional options
^^^^^^^^^^^^^^^^^^

Some options are available to launch the script without having to modify the script itself,
so that the script can be used in a batch job or in a workflow.

.. option:: -a <model>, --model_atm <model>

    The atmospheric model to use.

.. option:: -o <model>, --model_oce <model>

    The oceanic model to use.

.. option:: -e <exp>, --exp <exp>

    The experiment to use.

.. option:: -s <source>, source <source>

    The source to use.

.. option:: -d <dir>, --outputdir <dir>

    The output directory to use.
    Default is ``$AQUA/cli/aqua-analysis/output``.
    Prefer to use an absolute path.

.. option:: -m <machine>, --machine <machine>

    The machine to use.
    Default is ``lumi``.

.. option:: -l <loglevel>, --loglevel <loglevel>

    The log level to use for the cli and the diagnostics.
    Default is ``WARNING``.

.. option:: -t <threads>, --threads <threads>

    The number of threads to use for the cli and the diagnostics.
    Default is ``0``, which means the number of threads is automatically set to the number of available cores.
    Notice that the diagnostics are run in a single thread, which means that the parallelization
    is used to run multiple diagnostics at the same time.
    This is basically the number of diagnostics running in parallel.

.. option:: -p, --parallel

    This flag activates running the diagnostics with multiple dask.distributed workers.
    A predefined number of workers is used for each diagnostic, set in the script itself.
    For ecmean the multiprocessing option is used.
    

.. note ::

    By default the script will run all the state-of-the-art diagnostics available in AQUA.
    It is possible to run only a subset of the diagnostics by modifying the script itself,
    where arrays with atmospheric and oceanic diagnostics are defined.

.. _fdb-catalog-generator:

Catalog entry generator for FDB sources
---------------------------------------
A tool which streamlines the process of adding new experiments to the catalog.
It exploits the capabilities of the Jinja package to obtain a cleaner and more flexible code.
Users can easily customize their experiments by updating the ``config.tmpl`` file, with the experiment's specific details.
The script is available in the ``cli/fdb-catalog-generator`` folder.
Basic usage:

.. code-block:: bash

    ./catalog-jinja-generator.py -c config.tmpl -j ifs-nemo-default.j2 -l INFO


.. warning::

    Please note that currently only one Jinja template is available (``ifs-nemo-default.j2`` for IFS-NEMO), but it is possible to add more templates in the future.


.. _benchmarker:

Benchmarker
-----------

A tool to benchmark the performance of the AQUA analysis tools. The tool is available in the ``cli/benchmarker`` folder.
It runs a few selected methods for multiple times and report the durations of multiple execution: it has to be run in batch mode with 
the associated jobscript in order to guarantee robust results. 
It will be replaced in future by more robust performance machinery.

.. _gribber:

GRIB catalog generator
----------------------

A tool building on Gribscan, aiming at creating compact catalog entries through JSON files for massive GRIB archives.
A script in the ``cli/gribber`` folder is available.

.. warning ::

    This tool is currently deprecated, it might be removed in the future.


.. _grids-from-data:

Generation of grid from data
----------------------------

A tool to create CDO-compliant grid files (which are fundamental for proper regridding) specifically 
for oceanic model in order to ensure the right treatment of masks. 
Two scripts in the the ``cli/grid-fromd-data`` folder are available.

Both ``hpx-from-source.py`` and ``multiIO-from-source.py`` works starting from specific sources, 
saving them to disk and processing the final results with CDO to ensure the creation
of CDO-compliant grid files that can be later used for areas and remapping computation.

A YAML configuration file must be specified.

Basic usage:

.. code-block:: bash

    ./hpx-from-source.py -c config-hpx-nemo.yaml -l INFO

.. _grids-downloader:

Grids downloader
----------------

The grids used in AQUA are available for download.
A script in the ``cli/grids-downloader/`` folder is available

Basic usage:

.. code-block:: bash

    bash grids-downloader.sh all

This will download all the grids used in AQUA.
It is also possible to download only a subset of the grids,
by specifying the group of grids to download (usually one per model).

Grids synchronization
---------------------

Since the upload of the grids to the SWIFT platform used to store the grids is available only from Levante,
a simple script to synchronize the grids from Levante to LUMI and viceversa is available in the ``cli/grids-downloader/`` folder.
You will need to be logged to the destination platform to run the script and to have
passwordless ssh access to the source platform.

Basic usage:

.. code-block:: bash

    bash grids-sync.sh [levante_to_lumi | lumi_to_levante]

This will synchronize the grids from Levante to LUMI or viceversa.

.. warning::

    If more grids are added to the Levante platform, the SWIFT database should be updated.
    Please contact the AQUA team to upload new relevant grids to the SWIFT platform.

Grids uploader
--------------

A script to upload the grids to the SWIFT platform is available in the ``cli/grids-downloader/`` folder.
You will need to be on levante and to have the access to the SWIFT platform to run the script.
With the automatic setup updated folders will be uploaded in the same location on the SWIFT platform and 
no updates of the links in the `grids-downloader.sh` script will be needed.

Basic usage:

.. code-block:: bash

    bash grids-uploader.sh [all | modelname]

.. note::

    The script will check that a valid SWIFT token is available before starting the upload.
    If the token is not available, the script will ask the user to login to the SWIFT platform to obtain a new token.

HPC container utilities
-----------------------

Includes the script for the usage of the container on LUMI and Levante HPC: please refer to :ref:`container`

LUMI conda installation
-----------------------

Includes the script for the installation of conda environment on LUMI: please refer to :ref:`installation-lumi`

.. _orca:

ORCA grid generator
-------------------

A tool to generate ORCA grid files (with bounds) from the `mesh_mask.nc`. 
A script in the ``cli/orca-grids`` folder is available.

Basic usage:

.. code-block:: bash

    ./orca_bounds_new.py mesh_mask.nc orcefile.nc

.. _weights:

Weights generator
-----------------

A tool to compute via script or batch job the generation of interpolation weights which are 
too heavy to be prepared from notebook or login node. It can be configured to run on all the 
catalog enties so that it can be used to update existing weights if necessary, or to compute 
all the weights on a new machine.
A script in the ``cli/generate_weights`` folder is available.

Basic usage:

.. code-block:: bash

    ./generate_weights.py -c weights_config.yaml


.. _aqua_web:

Automatic uploading of figures and documentation to aqua-web
------------------------------------------------------------

AQUA figures produced by the analysis can be uploaded to the [aqua-web](https://github.com/DestinE-Climate-DT/aqua-web)
repository to publish them automatically on a dedicated website. The same site is used to host the documentation.
Two scripts in the ``cli/aqua-web`` folder are available to push figures or documentation to aqua-web.

Basic usage:

.. code-block:: bash

    # to generate and push the documentation to aqua-web
    ./make_push_docs.py 

    # to collect the figures from a directory $INDIR  figures to aqua-web
    INDIR=/path/to/figures_root
    MODELEXP=IFS-NEMO/historical-1990 # the subfolder of INDIR where the figures are stored (also model/exp pair for aqua-web)
    ./make_push_figures.py $INDIR IFS-NEMO/historical-1990 # to collect the figures and push them to aqua-web

The user running the script must have the right to push to the aqua-web repository and must have
set up the ssh keys to access the repository.
