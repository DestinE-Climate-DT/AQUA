.. _teleconnections:

Teleconnections diagnostic
==========================

This package provides a diagnostic for teleconnection indices evaluation and comparison with ERA5 reanalysis.

Description
-----------

The diagnostic is based on the computation of the regression or correlation between the time series of the teleconnection index and the time series of the variable used to compute the teleconnection index.
Teleconnections available:

- `NAO: notebook available <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/NAO.ipynb>`_
- `ENSO: notebook available <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/ENSO.ipynb>`_

More diagnostics or functionalities will be added in the future.

Structure
---------

The teleconnections diagnostic is a package with a class structure.
The core of the diagnostic is in the `tc_class.py` file, containing the `Teleconnections` class.

All the source code is available in the `teleconnections` folder inside the `teleconnections` folder.
The source code is organized in the following way:

- `plots` folder contains functions for the visualization of time series and maps for teleconnection diagnostic.
- `tools` folder contains generic functions that may be useful to the whole diagnostic.
- `cdo_testing.py` contains function evaluating teleconnections with cdo bindings, in order to test the python libraries.
- `index.py` contains functions for the direct evaluation of teleconnection indices.
- `statistics.py` contains functions for the regression and correlation analysis.
- `tc_class.py` contains the class that is used to run the diagnostic.

Configuration files are available in the `config` folder.
Different interfaces can be used to run the diagnostic, in the context of the `DestinE` project the interface file 
is `config/teleconnections_destine.py` and it is used as default.
An argument `interface` is available in the `Teleconnections` class to change the interface.
It can be also customized to add new teleconnections or to change the default parameters of the diagnostic.

A `pyproject.toml` file is available to install the diagnostic as a package.
Please refer to the README file in the main diagnostic folder for more details.
The standard way to install the diagnostic is to exploit the common environment for all the diagnostics
that is available in the main `AQUA` folder.

Data with timeseries of teleconnection indices are available in the `data` folder as txt files.

Notebooks are available in the `notebooks` folder, with detailed examples of the usage of the diagnostic.
They are organized in the following way:

- `NAO.ipynb` contains an example of the usage of the diagnostic for the NAO index with ERA5 reanalysis.
- `ENSO.ipynb` contains an example of the usage of the diagnostic for the ENSO index with ERA5 reanalysis.
- `NAO_cycle3.ipynb` is the notebook used to generate images for the nextGEMS cycle3 analysis.
- `NAO_control.ipynb` and `ENSO_control_2t.ipynb` are notebook used to analyze the control run of DestinE.

A command line interface is available in the `cli` folder.
`cli_teleconnections.py` is used to run the diagnostic from the command line.
It can analyze multiple model/exp/source and reference datasets at the same time.
It provides a configuration file (one for the atmospheric and one for the oceanic teleconnections) to customize the diagnostic.
Detailed description of the command line interface is available in the `README.md`
file in the `cli` folder and in the configuration files.

Minimal usage of the CLI is:

.. code-block:: bash

    python cli_teleconnections.py --model <model> --exp <experiment> --source <source> --ref

where `<model>`, `<experiment>`, `<source>` are the model, experiment, source and `--ref` is to activate the reference run.

Tests are available in the `tests/teleconnections` folder, from the main `AQUA` folder.
They make use of the `pytest` library and of the functions available in the `cdo_testing.py` file.

Input variables
---------------

The diagnostic requires the following input variables with the DestinE naming convention:

- `msl`: mean sea level pressure
- `avg_tos`: sea surface temperature

The diagnostic can be run on any dataset that provides these variables.
The diagnostic has been tested on ERA5 reanalysis and nextGEMS cycle3 data (use interface `config/teleconnections_nextgems.yaml`).

It is possible to evaluate regression maps and correlation maps with a teleconnection index and a different variable.
In this case, an additional variable is needed as input.

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