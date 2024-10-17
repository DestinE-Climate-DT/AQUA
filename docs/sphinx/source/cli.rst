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

The exact list of diagnostics to run and technical details of the analysis
(such as the nuber of cpu cores to be used for each diagnostic) 
are specified in the configuration file ``config.aqua-analysis.yaml``. 

Additional options
^^^^^^^^^^^^^^^^^^

Some options are available to launch the script without having to modify the script itself,
so that the script can be used in a batch job or in a workflow.

.. option:: -m <model>

    The  model to use.

.. option:: -e <exp>, --exp <exp>

    The experiment to use.

.. option:: -s <source>, --source <source>

    The source to use.

.. option:: -c <catalog>, --catalog <catalog>

    The catalog to use.
    Default is using the catalog currently defined by the AQUA console.

.. option:: -f <config>, --config <source>

    The config file to use.

.. option:: -d <dir>, --outputdir <dir>

    The output directory to use.
    Default is ``$AQUA/cli/aqua-analysis/output``.
    Prefer to use an absolute path.

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


.. _aqua_web:

Automatic uploading of figures and documentation to aqua-web
------------------------------------------------------------

AQUA figures produced by the analysis can be uploaded to the [aqua-web](https://github.com/DestinE-Climate-DT/aqua-web)
repository to publish them automatically on a dedicated website. The same site is used to host the documentation.
Two scripts in the ``cli/aqua-web`` folder are available to push figures or documentation to aqua-web.

Basic usage
^^^^^^^^^^^

.. code-block:: bash

    bash push-analysis.sh [OPTIONS] INDIR EXPS

This script is used to push the figures produced by the AQUA analysis to the aqua-web repository.
``INDIR`` is the directory containing the output, e.g. ``~/work/aqua-analysis/output``.
``EXPS`` is the subfolder to push, e.g ``climatedt-phase1/IFS-NEMO/historical-1990``
or a text file containing a list of experiments in the format "catalog model experiment".

Additional options
^^^^^^^^^^^^^^^^^^

.. option:: -b <branch>, --branch <branch>

    The branch to push to (optional, default is ``main``).

.. option:: -u <user>, --user <user>

    Credentials (in the format username:PAT) to create an automatic PR for the branch (optional).
    If this is option is specified and a branch is used, then an automatic PR is generated.

.. option:: -m <message>, --message <message>

    Description of the automatic PR (optional, is generated automatically by default). 

.. option:: -t <title>, --title <title>

    Title for the automatic PR (optional).

Another script is used to upload the documentation to the aqua-web repository.

.. code-block:: bash

    bash make_push_docs.py 

.. _submit-aqua-web:

Multiple experiment analysis submitter
--------------------------------------

A wrapper containing to facilitate automatic submission of analysis of multiple experiments
in parallel and possible pushing to AQUA Explorer. This is used to implement overnight updates to AQUA Explorer.

Basic usage
^^^^^^^^^^^

.. code-block:: bash

    python ./submit-aqua-web.py EXPLIST

This will read a text file EXPLIST containing a list of models/experiments in the format

.. code-block:: rst

    # List of experiments to analyze in the format
    # model exp [source]

    IFS-NEMO  ssp370  lra-r100-monthly
    IFS-NEMO historical-1990
    ICON historical-1990
    ICON ssp370

A sample file ``aqua-web.experiment.list`` is provided in the source code of AQUA.
Specifying the source is optional ('lra-r100-monthly' is the default).

Before using the script you will need to specify details for SLURM and other options
in the configuration file ``config.aqua-web.yaml``. This file is searched in the same directories as 
other AQUA configuration files or in the current directory as last resort.

It is possible to run the analysis on a single experiment specifying model, experiment and source
with the arguments ``-m``, ``-e`` and ``-s`` respectively.

If run without arguments, the script will run the analysis on the default 
experiments specified in the list.

Adding the ``-p`` or ``--push`` flag will push the results to the AQUA Explorer.

Options
^^^^^^^

.. option:: -c <config>, --config <config>

    The configuration file to use. Default is ``config.aqua-web.yaml``.

.. option:: -m <model>, --model <model>

    Specify a single model to be processed (alternative to specifying the experiment list).

.. option:: -e <exp>, --exp <exp>

    Experiment to be processed.

.. option:: -s <source>, --source <source>

    Source to be processed.

.. option:: -r, --serial

    Run in serial mode (only one core). This is passed to the ``aqua-analysis.sh`` script.

.. option:: -x <max>, --max <max>

    Maximum number of jobs to submit without dependency.

.. option:: -t <template>, --template <template>

    Template jinja file for slurm job. Default is ``aqua-web.job.j2``.

.. option:: -d, --dry

    Perform a dry run for debugging (no job submission). Sets also ``loglevel`` to 'debug'.

.. option:: -l <loglevel>, --loglevel <loglevel>

    Logging level.

.. option:: -p, --push
    
    Flag to push to aqua-web. This uses the ``make_push_figures.py`` script.


.. _benchmarker:

Benchmarker
-----------

A tool to benchmark the performance of the AQUA analysis tools. The tool is available in the ``cli/benchmarker`` folder.
It runs a few selected methods for multiple times and report the durations of multiple execution: it has to be run in batch mode with 
the associated jobscript in order to guarantee robust results. 
It will be replaced in future by more robust performance machinery.


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

ecCodes fixer
-------------

In order to be able to read data written with recent versions of ecCodes,
AQUA needs to use a very recent version of the binary and of the definition files.
Data written with earlier versions of ecCodes should instead be read using previous definition files.
AQUA solves this problem by switching on the fly the definition path for ecCodes, as specified in the source catalog entry. 
Starting from version 2.34.0 of ecCodes older definitions are not compatible anymore.
As a fix we create copies of the original older definion files with the addition/change of 5 files (``stepUnits.def`` and 4 files including it).
A CLI script (``eccodes/fix_eccodes.sh``) is available to create such 'fixed' definition files.

.. warning::

    This change is necessary since AQUA v0.11.1.
    Please notice that this also means that earlier versions of the ecCodes binary will not work using these 'fixed' definition files.
    If you are planning to use older versions of AQUA (with older versions of ecCodes) you should not use these 'fixed' definition files
    and you may need to modify the ecCodes path in the catalog entries.
