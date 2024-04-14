.. _teleconnections:

Teleconnections diagnostic
==========================

This package provides a diagnostic for teleconnection evaluation and comparison with ERA5 reanalysis.
This is done with the computation of the teleceonnection indices and of the regression and correlation
between the teleconnection index and the variable used to compute the teleconnection index.

Description
-----------

The diagnostic is based on the computation of the regression or correlation between the time series
of the teleconnection index and the time series of the variable used to compute the teleconnection index.
Teleconnections available:

- `NAO: notebook available <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/NAO.ipynb>`_
- `ENSO: notebook available <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/ENSO.ipynb>`_

More diagnostics or functionalities will be added in the future.

Structure
---------

The teleconnections diagnostic is a package with a class structure.
The core of the diagnostic is in the ``tc_class.py`` file, containing the ``Teleconnections`` class.

All the source code is available in the ``teleconnections`` folder inside the ``teleconnections`` folder.
The source code is organized in the following way:

- ``tc_class.py`` contains the class that is used to run the diagnostic.
- ``index.py`` contains functions for the direct evaluation of teleconnection indices.
- ``statistics.py`` contains functions for the regression and correlation analysis.
- ``bootstrap.py`` contains functions for the bootstrap evaluation for concordance maps of regression and correlation.
- ``plots`` folder contains functions for the visualization of time series and maps for teleconnection diagnostic. Part of the graphical functions are part of the AQUA framework.
- ``tools`` folder contains generic functions that may be useful to the whole diagnostic.
- ``cdo_testing.py`` contains function evaluating teleconnections with cdo bindings, in order to test the python libraries.

Configuration files are available in the ``config`` folder.
Different interfaces can be used to run the diagnostic, in the context of the Destination Earth Climate DT project the interface file 
is ``config/teleconnections_destine.py`` and it is used as default.
An argument ``interface`` is available in the ``Teleconnections`` class to change the interface.
It can be also customized to add new teleconnections or to change the default parameters of the diagnostic.

A ``pyproject.toml`` file is available to install the diagnostic as a part of the AQUA environment.
It is not tought to be used as a standalone package since it relies on the AQUA framework code.
Please refer to the :ref:`installation` section for more information.

Data with timeseries of teleconnection indices from NCAR are available in the ``data`` folder as txt files.
These data are used to show in the notebooks the comparison between the model and the observations.

Notebooks are available in the ``notebooks`` folder, with detailed examples of the usage of the diagnostic.
They are organized in the following way:

- `NAO.ipynb` contains an example of the usage of the diagnostic for the NAO index with ERA5 reanalysis.
- `ENSO.ipynb` contains an example of the usage of the diagnostic for the ENSO index with ERA5 reanalysis.
- `concordance_map.ipynb` contains an example of bootstrap evaluation for concordance maps of regression and correlation.

Other notebooks are left for legacy purposes and are related to the analysis of previous DestinE or nextGEMS simulations.
Additionally a ``deliverable`` folder is available, containing configuration files and notebooks used for the analysis of the DestinE simulations
for the final deliverable.

A command line interface is available in the ``cli`` folder.

Tests are available in the ``AQUA/tests/teleconnections`` folder.
They make use of the ``pytest`` library and of the functions available in the ``cdo_testing.py`` library file.

Command line interface
----------------------

A command line interface is available in the ``cli`` folder.
``cli_teleconnections.py`` is used to run the diagnostic from the command line.
It can analyze multiple models, exps, sources and reference datasets at the same time.
It provides a configuration file (one for the atmospheric NAO and one for the oceanic ENSO teleconnections)
to customize the details of the diagnostic.

Basic usage
^^^^^^^^^^^

Basic usage of the CLI is:

.. code-block:: bash

    python cli_teleconnections.py

This will run the diagnostic with the default configuration file and the default model, experiment and source.
It will not not compare the models with the reference dataset.

CLI options
^^^^^^^^^^^

The CLI accepts the following arguments:

- ``--model``: the model to analyze.
- ``--exp``: the experiment to analyze.
- ``--source``: the source to analyze.
- ``--ref``: activate the reference run.
- ``-c`` or ``--config``: path to the configuration file.
- ``--interface``: path to the interface file.
- ``--outputdir``: path to the output folder.
- ``-d`` or ``--dry``: dry run, no files are written.
- ``-l`` or ``--loglevel``: log level for the logger. Default is WARNING.

Input variables
---------------

The diagnostic requires the following input variables with the DestinE naming convention:

- ``msl``: (Mean sea level pressure, paramid 151) for NAO
- ``avg_tos``: (Time-mean sea surface temperature, paramid 263101) for ENSO

The diagnostic can be run on any dataset that provides these variables.

It is possible to evaluate regression maps and correlation maps with a teleconnection index and a different variable.
In this case, an additional variable is needed as input.
It is also possible to use different variables if variables are missing
(e.g. for the ENSO index, the skin temperature can be used as input and have an estimate even if ``avg_tos`` is missing).

Output
------

The diagnostic produces the following output:

- `NAO`: North Atlantic Oscillation index, regression and correlation maps.
- `ENSO`: El Niño Southern Oscillation index, regression and correlation maps.

All these outputs can be stored both as images (pdf format) and as netCDF files.

Example plot
------------

.. figure:: figures/teleconnections.png
   :width: 100%

   ENSO IFS-NEMO ssp370 regression map (avg_tos) compared to ERA5.
   The contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map.

Available demo notebooks
------------------------

- `NAO: notebook available <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/NAO.ipynb>`_
- `ENSO: notebook available <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/ENSO.ipynb>`_

Detailed API
------------

This section provides a detailed reference for the Application Programming Interface (API) of the Teleconnections diagnostic,
produced from the diagnostic function docstrings.

.. automodule:: teleconnections
    :members:
    :undoc-members:
    :show-inheritance: