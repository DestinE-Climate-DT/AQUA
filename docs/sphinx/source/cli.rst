.. _cli:

Command Line Interface tools
============================

This sections describes a series of Command Line Interface (CLI) tools currently available in AQUA.
It includes software with a variety of goals, which are mostly made for advanced usage. 


.. _benchmarker:

Benchmarker
-----------

A tool to benchmark the performance of the AQUA analysis tools. The tool is available in the ``cli/benchmarker`` folder.
It runs a few selected methods for multiple times and report the durations of multiple execution: it has to be run in batch mode with 
the associated jobscript in order to guarantee robust results. 
It will be replaced in future by more robust performance machinery.

.. _grids-management:

Grids management
----------------

This section describes the tools available to manage the grids used in AQUA,
from the download and validation to the synchronization between different HPC platforms.

.. _grids-downloader:

Grids downloader
^^^^^^^^^^^^^^^^

The grids used in AQUA are available for download.
A script in the ``cli/grids-downloader/`` folder is available

Basic usage:

.. code-block:: bash

    bash grids-downloader.sh all

This will download all the grids used in AQUA.
It is also possible to download only a subset of the grids,
by specifying the group of grids to download (usually one per model).

.. _grids-checker:

Checksum verifications for obs and grids
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

AQUA built on many grids files to speed up operations for interpolation and area evaluation, especially within
the ClimateDT workflow. To provide valuable diagnostics, it also builds on a catalog for observations.
These are available on multiple ClimateDT HPC but sometimes the synchronization
might not be complete following an update. In order to verify that all the grids files are ported on the used machine
the `cli/checksum-checker/grids-checker.py` and `cli/checksum-checker/obs-checker.py` scripts are available 
to verify the checksum of the grid and observation files is the same as it is expected.

To verify that everything is at it should be please run:

.. code-block:: bash

    ./grid-checker.py verify

To generate a new checksum should be please run:

.. code-block:: bash

    ./grid-checker.py generate -o checksum_file.md5

Please notice that not all the grid/obs folders will be checked, but only those defined in the file 
with ``GRIDS_FOLDERS`` and ``OBS_FOLDERS`` variables. 
Option ``-s`` can be used as well to scan a single grid/obs folder (e.g. HealPix, or ERA5)

.. _grid-from-data:

Generation of grid from data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. _grids-sync:

Grids synchronization
^^^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^

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

.. _orca:

ORCA grid generator
^^^^^^^^^^^^^^^^^^^

A tool to generate ORCA grid files (with bounds) from the `mesh_mask.nc`. 
A script in the ``cli/orca-grids`` folder is available.

Basic usage:

.. code-block:: bash

    ./orca_bounds_new.py mesh_mask.nc orcefile.nc

HPC container utilities
-----------------------

Includes the script for the usage of the container on LUMI and Levante HPC: please refer to :ref:`container`

LUMI conda installation
-----------------------

Includes the script for the installation of conda environment on LUMI: please refer to :ref:`installation-lumi`

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


.. _orography:

Orography generator
-------------------

A tool to generate orography files from a source that can be accessed via AQUA.
It is located in the ``cli/orography_from_data`` folder and it contains all the configurations to generate orography files
inside the script file itself.

It has been used to produce the orography files for the Tropical Cyclone diagnostic.

Basic usage:

.. code-block:: bash

    python orography_generator.py
