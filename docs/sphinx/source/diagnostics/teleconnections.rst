Teleconnections diagnostic
==========================

This package provides a diagnostic for teleconnection indices evaluation and comparison with ERA5 reanalysis.

Description
-----------

Teleconnections available:

- NAO: notebook available `diagnostics/teleconnections/notebooks/NAO.ipynb`
- ENSO: notebook available `diagnostics/teleconnections/notebooks/ENSO_nino3.4.ipynb`

More diagnostics or functionalities can be added in the future.

Structure
---------

The teleconnections diagnostic is a package with a class structure.
It consists of the following library files:

- `cdotesting.py` contains function evaluating teleconnections with cdo bindings, in order to test the python libraries (see tests section).
- `index.py` contains functions for the direct evaluation of teleconnection indices. It is the core of the diagnostic.
- `plots.py` contains functions for the visualization of time series and maps for teleconnection diagnostic.
- `tc_class.py` contains the class that is used to run the diagnostic.
- `tools.py` contains generic functions that may be useful to the whole diagnostic.

A configuration file is available in the `config` folder.
It can be customized to add new teleconnections or to change the default parameters of the diagnostic.

An environment file is available as `env-teleconnections.yaml` in the main diagnostic folder.
A `pyproject.toml` file is also available to install the diagnostic as a package.
Please refer to the installation section for more details.

Data with timeseries of teleconnection indices are available in the `data` folder as txt files.

Notebooks are available in the `notebooks` folder, with detailed examples of the usage of the diagnostic,
both for ERA5 and nextGEMS cycle3 data and as individual functions or as a class.
These notebooks have been run on Levante.

A command line interface is available in the `cli` folder.

Tests are available in the `tests/teleconnections` folder, from the main `AQUA` folder.
Please notice that running tests will require to install the specific environment for the diagnostic.

Input variables
---------------

The diagnostic requires the following input variables:
- `msl`: mean sea level pressure
- `sst`: sea surface temperature

The diagnostic can be run on any dataset that provides these variables.
The diagnostic has been tested on ERA5 reanalysis and nextGEMS cycle3 data.

Output
------

The diagnostic produces the following output:
- `NAO`: North Atlantic Oscillation index
- `ENSO`: El Niño Southern Oscillation index

Regression and correlation plots are also produced.
All these outputs can be stored both as images and as netCDF files.

How to install the diagnostic
-----------------------------

The diagnostic is available in the `teleconnections` folder inside the `diagnostics` folder.
Inside the `teleconnections` folder there is a `pyproject.toml` file that can be used to install the diagnostic as a package with pip.
An `env-teleconnections.yml` file is also available to create a conda environment with all the dependencies needed to run the diagnostic.

To install the diagnostic as a package, run the following command from the `teleconnections` folder:

.. code-block:: bash

  pip install .

To create a conda environment with all the dependencies needed to run the diagnostic, run the following command from the `teleconnections` folder:

.. code-block:: bash

  conda env create -f env-teleconnections.yml

How to run the diagnostic
-------------------------

The diagnostic can be run in a notebook or in a script.
The following steps are necessary to run the diagnostic (we're taking the NAO as an example):

1. Import the necessary functions from the `teleconnections` package and from the `AQUA` framework.

.. code-block:: python

  from aqua import Reader
  from index import station_based_index
  from teleconnections.plots import index_plot
  from teleconnections.tools import load_namelist

2. Load the config specific of your teleconnection.

.. code-block:: python
  
  diagname  = 'teleconnections'
  telecname = 'NAO'

  config = load_namelist(diagname)
  field = namelist[telecname]['field']

Without any argument, the `load_namelist` function will load the default config file, trying to get the correct path.

The dictionary extracted from the config file will be used to set the parameters of the diagnostic.
For NAO:

.. code-block:: yaml
  
  # NAO coordinates
  NAO:
    field: msl
    lat1: 37.7
    lon1: -25.7
    lat2: 64.1
    lon2: -22

Please notice that the `field` parameter is the name of the field in the data.
It may be different for different datasets and fix features available in the `AQUA` framework may be needed to automatize the access to data.

3. Load the data

Reader is a class of the AQUA framework that can be used to load the data.
It has to be initialized with the details of the data to be loaded.

.. code-block:: python
  
  model = 'IFS'
  exp = 'tco1279-orca025-cycle3'
  source = '2D_monthly_native'

  reader = Reader(model=model,exp=exp,source=source,regrid='r025')

Regrid of the data, time aggregation and fix features may be needed to automatize and simplify the access to data.
Check the `AQUA` framework documentation for more details.
What is needed to run the diagnostic is a `xarray.Dataarray` object with the data to be used.

4. Run the diagnostic

.. code-block:: python
  
  nao = station_based_index(field=infile,namelist=namelist,telecname=telecname)

5. Plot the results

Some plot functions are available in the `plots.py` file.
Optionally, the `xarray.Dataarray` object returned by the `station_based_index` function can be used to plot the results in any way,
to save the results in a file or to do any other operation.

Command line interface
----------------------

The diagnostic can be run from the command line interface.
The `cli` folder contains a `cli_teleconnections.py` file that can be used to run the diagnostic.
A slurm script is also available in the `cli` folder to run the diagnostic on Levante (Lumi still to be tested).

In the same folder a `teleconnections_config.yaml` file is available with an example of the configuration file that can pass to the cli script the main user dependant parameters.

Example plot
------------

.. figure:: figures/teleconnections.png
   :width: 10 cm

    Example plot of the NAO index for ERA5 data.

Available demo notebooks
------------------------

To be updated with links to the correct repository path.

Detailed API
------------

This section provides a detailed reference for the Application Programming Interface (API) of the "dummy" diagnostic,
produced from the diagnostic function docstrings.

.. automodule:: teleconnections
    :members:
    :undoc-members:
    :show-inheritance: