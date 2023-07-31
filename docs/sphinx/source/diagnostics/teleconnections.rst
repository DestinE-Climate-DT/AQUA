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

A configuration file is available in the `config` folder.
It can be customized to add new teleconnections or to change the default parameters of the diagnostic.

An environment file is available as `env-teleconnections.yaml` in the main diagnostic folder.
A `pyproject.toml` file is also available to install the diagnostic as a package.
Please refer to the README file in the main diagnostic folder for more details.
Notice as well that a common environment for all the diagnostics is available in the main `AQUA` folder.

Data with timeseries of teleconnection indices are available in the `data` folder as txt files.

Notebooks are available in the `notebooks` folder, with detailed examples of the usage of the diagnostic.
They are organized in the following way:

- `NAO.ipynb` contains an example of the usage of the diagnostic for the NAO index with ERA5 reanalysis.
- `ENSO.ipynb` contains an example of the usage of the diagnostic for the ENSO index with ERA5 reanalysis.
- `NAO_cycle3.ipynb` contains notebook used to generate images for the nextGEMS cycle3 analysis.

A command line interface is available in the `cli` folder.
Two scripts are available:

- `/single_analysis/cli_teleconnections.py` is used to run the diagnostic for a single experiment and a single teleconnection.
- `/comparison_analysis/cli_teleconnections.py` is used to run the diagnostic for multiple experiments, but only a single teleconnection.

Tests are available in the `tests/teleconnections` folder, from the main `AQUA` folder.
They make use of the `pytest` library and of the functions available in the `cdo_testing.py` file.

Input variables
---------------

The diagnostic requires the following input variables:

- `msl`: mean sea level pressure
- `sst`: sea surface temperature

The diagnostic can be run on any dataset that provides these variables.
The diagnostic has been tested on ERA5 reanalysis and nextGEMS cycle3 data.

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
   :width: 10 cm

   Example plot of the NAO index for ERA5 data.

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