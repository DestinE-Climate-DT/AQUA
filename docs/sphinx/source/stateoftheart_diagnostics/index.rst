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

   boxplots
   global_biases
   ecmean
   timeseries
   ocean3d
   seaice
   teleconnections
   ensemble

.. _diagnostics-configuration-files:

Diagnostics configuration files
+++++++++++++++++++++++++++++++

Each diagnostic has a corresponding YAML configuration file that specifies the options and parameters for the diagnostic.
These configuration files are located in the ``config/diagnostics/<diagnostic-name>`` directory of the AQUA package and copied
to the ``AQUA_CONFIG`` folder during installation (see :ref:`aqua-install`, by default ``$HOME/.aqua/``).

Each diagnostic has its own configuration file, with a block devoted to the individual diagnostic settings.
However, general settings common to all the diagnostics have a common structure here described.
Please refer to the individual diagnostic documentation for the specific settings.
See :ref:`configuration-file-guidelines` for an example of diagnostic specific block.

* ``datasets``: a list of models to analyse (defined by the catalog, model, exp, source arguments).
  If the diagnostic can handle multiple datasets, all the models in the list will be processed, otherwise only the first one will be used.
  For simplicity the default in the repository should refer to only one model.

.. code-block:: yaml

    datasets:
      - catalog: climatedt-phase1
        model: IFS-NEMO
        exp: historical-1990
        source: lra-r100-monthly
        regrid: null
        reader_kwargs: null # it can be a dictionary with reader kwargs

      - catalog: climatedt-phase1
        model: ICON
        exp: historical-1990
        source: lra-r100-monthly
        regrid: null
        reader_kwargs: null # it can be a dictionary with reader kwargs

* ``references``: a list of reference datasets to use for the analysis.
  Some diagnostics may not work with multiple references, it is better to specify it in the documentation and in the configuration file.

.. code-block:: yaml

    references:
      - catalog: obs
        model: ERA5
        exp: era5
        source: monthly
        regrid: null
        reader_kwargs: null # it can be a dictionary with reader kwargs

* ``output``: a block describing the details of the output. Is contains:

    * ``outputdir``: the output directory for the plots.
    * ``rebuild``: a boolean that enables the rebuilding of the plots.
    * ``save_pdf``: a boolean that enables the saving of the plots in pdf format.
    * ``save_png``: a boolean that enables the saving of the plots in png format.
    * ``dpi``: the resolution of the plots.
    * ``create_catalog_entry``: a boolean that enables the creation of a catalog entry.

.. code-block:: yaml

    output:
      outputdir: "/path/to/output"
      rebuild: true
      save_pdf: true
      save_png: true
      dpi: 300
      create_catalog_entry: true

.. note::

  Not all the diagnostics support yet the ``create_catalog_entry`` keyword.

.. _diagnostics-cli-arguments:

Diagnostics CLI arguments
+++++++++++++++++++++++++

The following command line arguments are available for all the diagnostics:

- ``--config``, ``-c``: Path to the configuration file.
- ``--nworkers``, ``-n``: Number of workers to use for parallel processing.
- ``--cluster``: Cluster to use for parallel processing. By default a local cluster is used.
- ``--loglevel``, ``-l``: Logging level. Default is ``WARNING``.
- ``--catalog``: Catalog to use for the analysis. It can be defined in the config file.
- ``--model``: Model to analyse. It can be defined in the config file.
- ``--exp``: Experiment to analyse. It can be defined in the config file.
- ``--source``: Source to analyse. It can be defined in the config file.
- ``--outputdir``: Output directory for the plots.

If a diagnostic has extra arguments, these will be described in the individual diagnostic documentation.

Running the monitoring diagnostics
++++++++++++++++++++++++++++++++++

Each state-of-the-art diagnostic is implemented as a Python class and can be run independently.
All the diagnostic have a command line interface that can be used to run them.
A YAML configuration file is provided to set the options for the diagnostics.

Together with the individual diagnostics command line interfaces, AQUA provides an entry point to run all the diagnostics
in a single command, with a shared Dask cluster, shared output directory and with parallelization.
The entry point is called `aqua analysis` and all the details can be found in :ref:`aqua_analysis`.

.. warning::
   The analysis has to be performed preferrably on LRA data, meaning that data should be aggregated
   to a resolution of 1 degree in both latitude and longitude and to a monthly frequency.
   It is available the option to regrid the data on the fly, but the memory usage may be highly
   increased and it may be preferrable to run the diagnostics individually.

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

.. note::
  If you are a developer you can enforce the minimum data requirements by using the `months_required` argument in the `retrieve` and `_retrieve` methods
  available in the diagnostic core.
