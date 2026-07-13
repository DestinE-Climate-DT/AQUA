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

.. _grid-from-data:

Generation of grid from data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A tool to create CDO-compliant grid files (which are fundamental for proper regridding) specifically
for oceanic model in order to ensure the right treatment of masks.
Two scripts in the the ``cli/grid-from-data`` folder are available.

Both ``hpx-from-source.py`` and ``multiIO-from-source.py`` works starting from specific sources,
saving them to disk and processing the final results with CDO to ensure the creation
of CDO-compliant grid files that can be later used for areas and remapping computation.

A YAML configuration file must be specified.

Basic usage:

.. code-block:: bash

    ./hpx-from-source.py -c config-hpx-nemo.yaml -l INFO


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

Includes the script for the usage of the container on LUMI and Levante HPC: please refer to :ref:`container`.

LUMI conda installation
-----------------------

Includes the script for the installation of conda environment on LUMI: please refer to :ref:`installation-lumi`.

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
