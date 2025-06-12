.. _stateoftheart_diagnostics:

State of the art diagnostics
============================

AQUA provides a collection of built-in diagnostics to analyze climate model outputs. 
The family of diagnostics named **state-of-the-art** lists diagnostics which can be used for the simulation monitoring and
make use of low resolution data as input (1 degree in both latitude and longitude and monthly frequency).
Most of these diagnostics can be compared with observations to produce metrics of evaluation and aim at providing an assessment
of the model against observational datasets and, in some selected occasions, pre-existing climate simulations. 

List of diagnostics
+++++++++++++++++++

This list includes such diagnostics whose goal is to monitor and diagnose possible model drifts, imbalances and biases.

Currently implemented diagnostics are:

.. toctree::
   :maxdepth: 1

   global_biases
   ecmean
   timeseries
   ocean3d
   radiation
   seaice
   teleconnections

Running the monitoring diagnostics
----------------------------------

Each state-of-the-art diagnostic is implemented as a Python class and can be run independently.
All the diagnostic have a command line interface that can be used to run them.
A YAML configuration file is provided to set the options for the diagnostics.

Together with the individual diagnostics command line interfaces, AQUA provides a python script to run all the diagnostics
in a single command, with a shared Dask cluster, shared output directory and with parallelization.
The tool is called `aqua-analysis.py` and all the details can be found in :ref:`aqua_analysis`.

.. warning::
   The analysis has to be performed preferrably on LRA data, meaning that data should be aggregated
   to a resolution of 1 degree in both latitude and longitude and to a monthly frequency.

Minimum Data Requirements
-------------------------

In order to obtain meaningful results, the diagnostics require a minimum amount of data.
Here you can find the minimum requirements for each diagnostic.

.. list-table::
   :header-rows: 1

   * - Diagnostic
     - Minimum Data Required
   * - Global Biases
     - 1 year of data
   * - ecmean
     - 1 year of data
   * - Timeseries
     - 2 months of data
   * - Seasonal cycles
     - 1 year of data
   * - Ocean3d
     - 1 year of data, 2 months for the timeseries
   * - Radiation
     - 1 year of data
   * - Seaice
     - 1 year of data
   * - Teleconnections
     - 2 years of data

.. note::
   Some diagnostics will technically run with less data, but the results may not be meaningful.
   Some other will raise errors in the log files if the data is not enough.
