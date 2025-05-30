.. _timeseries:

Timeseries
==========

Description
-----------

The Timeseries diagnostic is a set of tools to compute and plot time series, seasonal cycles and Gregory-like plot of a given variable or formula for a given model or list of models.
Time series and seasonal cycles can be computed globally or for a specific area.
The three functionalities are designed to have a class which analyse a single model to produce the netCDF files and another class to produce the plots.
The diagnostic can compare the model time series with a reference dataset, which is defined in the configuration file.

Classes
-------

There are three classes to compute the netCDF files:

* **Timeseries**: a class that computes time series of a given variable or formula for a given model or dataset. 
  Comparison with a reference dataset is also possible. It supports hourly, daily, monthly and yearly time series and area selection.
  It can also compute the standard deviation of the time series.
* **SeasonalCycles**: a class that computes the seasonal cycle of a given variable or formula for a given model or dataset.
  Comparison with a reference dataset is also possible. It can also compute the standard deviation of the seasonal cycle. It supports area selection.
* **Gregory**: a class that computes the monthly and annual time series necessary for the Gregory-like plot of net radiation TOA and 2 metre temperature.
  It can also compute the standard deviation of the time series.

There are three other classes to produce the plots:

* **PlotTimeseries**: a class that ingests xarrays and produces the plots for the time series.
  Info necessary for titles, legends and captions are deduced from the xarray attributes.
* **PlotSeasonalCycles**: a class that ingests xarrays and produces the plots for the seasonal cycle.
  Info necessary for titles, legends and captions are deduced from the xarray attributes.
* **PlotGregory**: a class that ingests xarrays and produces the Gregory-like plot.
  Info necessary for titles, legends and captions are deduced from the xarray attributes.

.. warning::

    Hourly and daily plots are not available yet, while the netCDF files are produced.

File structure
--------------

* The diagnostic is located in the ``src/aqua_diagnostics/timeseries`` directory, which contains both the source code and the command line interface (CLI) script.
* The configuration files are located in the ``config/diagnostics/timeseries`` directory and contains the default configuration for the diagnostic.
* Notebooks are available in the ``notebooks/diagnostics/timeseries`` directory and contain examples of how to use the diagnostic.
* A list of available regions is available in the ``config/diagnostics/timeseries/interface/regions.yaml`` file.

Input variables and datasets
----------------------------

By default, the diagnostic compare against the ERA5 dataset, with standard deviation calculated over the period 1990-2020.
The Gregory-like plot uses the CERES dataset for the Net radiation TOA and the ERA5 dataset for the 2 metre temperature.

The necessary input variables for the diagnostic are all the variables that are needed to compute the time series, seasonal cycle choosen in the configuration file.
The Gregory-like plot requires the Net radiation TOA and the 2 metre temperature.

CLI usage
---------

The diagnostic can be run from the command line interface (CLI) by running the following command:

.. code-block:: bash

    cd $AQUA/src/aqua_diagnostics/timeseries
    python cli_timeseries.py --config <path_to_config_file>

Three configuration files are provided and run when executing the aqua-analysis (see :ref:`aqua_analysis`).
Two configuration files are for atmospheric and oceanic timeseries and gregory plots, and the third one is for the seasonal cycles.

Additionally the CLI can be run with the following optional arguments:

- ``--config``, ``-c``: Path to the configuration file.
- ``--nworkers``, ``-n``: Number of workers to use for parallel processing.
- ``--cluster``: Cluster to use for parallel processing. By default a local cluster is used.
- ``--loglevel``, ``-l``: Logging level. Default is ``WARNING``.
- ``--catalog``: Catalog to use for the analysis. It can be defined in the config file.
- ``--model``: Model to analyse. It can be defined in the config file.
- ``--exp``: Experiment to analyse. It can be defined in the config file.
- ``--source``: Source to analyse. It can be defined in the config file.
- ``--outputdir``: Output directory for the plots.

Config file structure
---------------------

The configuration file is a YAML file that contains the following information:

* ``datasets``: a list of models to analyse (defined by the catalog, model, exp, source arguments)

.. code-block:: yaml

    datasets:
      - catalog: climatedt-phase1
        model: IFS-NEMO
        exp: historical-1990
        source: lra-r100-monthly
        regrid: null
      - catalog: climatedt-phase1
        model: ICON
        exp: historical-1990
        source: lra-r100-monthly
        regrid: null

* ``references``: a list of reference datasets to use for the analysis.

.. code-block:: yaml

    references:
      - catalog: obs
        model: ERA5
        exp: era5
        source: monthly
        regrid: null

* ``output``: a block describing the details of the output. Is contains:

    * ``outputdir``: the output directory for the plots.
    * ``rebuild``: a boolean that enables the rebuilding of the plots.
    * ``save_pdf``: a boolean that enables the saving of the plots in pdf format.
    * ``save_png``: a boolean that enables the saving of the plots in png format.
    * ``dpi``: the resolution of the plots.

.. code-block:: yaml

    output:
      outputdir: "/path/to/output"
      rebuild: true
      save_pdf: true
      save_png: true
      dpi: 300

* ``timeseries``: a block, nested in the ``diagnostics`` block, that contains the details required for the time series.
  The parameters specific to a single variable are merged with the default parameters, giving priority to the specific ones.

.. code-block:: yaml

    diagnostics:
      timeseries:
        run: true # to enable the time series
        variables: ['2t', 'tprate']
        formulae: ['tnlwrf+tnswrf']
        params:
          default:
            hourly: false # Frequency to enable
            daily: false
            monthly: true
            annual: true
            std_startdate: '1990-01-01'
            std_enddate: '2020-12-31'
          tnlwrf+tnswrf:
            standard_name: "net_top_radiation"
            long_name: "Net top radiation"
          tprate:
            units: 'mm/day'
            standard_name: 'tprate'
            long_name: 'Total precipitation rate'
            regions: ['tropics', 'europe'] # regions to plot the time series other than global


* ``seasonalcycle``: a block, nested in the ``diagnostics`` block, that contains the details required for the seasonal cycle.
  The parameters specific to a single variable are merged with the default parameters, giving priority to the specific ones.

.. code-block:: yaml

    diagnostics:
      seasonalcycle:
        run: true # to enable the seasonal cycle
        variables: ['2t', 'tprate']
        params:
          default:
            std_startdate: '1990-01-01'
            std_enddate: '2020-12-31'
          tprate:
            units: 'mm/day'
            long_name: 'Total precipitation rate'

* ``gregory``: a block, nested in the ``diagnostics`` block, that contains the details required for the Gregory-like plot.

.. code-block:: yaml

    diagnostics:
      gregory:
        run: true # to enable the Gregory plot
        t2m: '2t' # variable name for the 2 metre temperature
        net_toa_name: 'tnlwrf+tnswrf' # formula or variable name for the Net radiation TOA
        monthly: true
        annual: true
        std: true
        std_startdate: '1990-01-01'
        std_enddate: '2020-12-31'
        # Gregory needs 2 datasets and do not care about the references block above
        t2m_ref: {'catalog': 'obs', 'model': 'ERA5', 'exp': 'era5', 'source': 'monthly'}
        net_toa_ref: {'catalog': 'obs', 'model': 'CERES', 'exp': 'ebaf-toa42', 'source': 'monthly'}

Output
------

The diagnostic produces three types of plots (see :ref:`timeseries_examples`):

* A comparison of monthly and/or annual global mean time series of the model and the reference dataset.
* A comparison of the seasonal cycle of the model and the reference dataset.
* A Gregory-like plot of the model and the reference dataset as bands.

The timeseries, reference timeseries and standard deviation timeseries are also saved in the output directory as netCDF files.

Observations
------------

The diagnostic uses the following reference datasets:

* ERA5 as a default reference dataset for the global mean time series and seasonal cycle.
* ERA5 for the 2m temperature and CERES for the Net radiation TOA in the Gregory-like plot.

.. note::

    Multiple reference time series and seasonal cycles plot are planned for the future.

Custom reference datasets can be used.

.. _timeseries_examples:

Example Plots
-------------

A plot for each class is shown below.
All these plots can be produced by running the notebooks in the ``notebooks`` directory on LUMI HPC.

.. figure:: figures/gregory_doc.png
    :align: center
    :width: 100%

    Gregory plot of ICON historical-1990 simulation.

.. figure:: figures/timeseries_doc.png
    :align: center
    :width: 100%

    Global mean temperature time series of ICON historical-1990 and comparison with ERA5.
    Both monthly and annual timeseries are shown. A 2 sigma confidence interval is evaluated for ERA5 data (1990-2020).

.. figure:: figures/seasonalcycles_doc.png
    :align: center
    :width: 100%

    Seasonal cycle of the global mean temperature of IFS-NEMO historical-1990 and comparison with ERA5.
    The 2 sigma confidence interval is evaluated for ERA5 data (1990-2020).

Available demo notebooks
------------------------

Notebooks are stored in the ``notebooks/diagnostics/timeseries`` directory and contain examples of how to use the diagnostic.

* `timeseries.ipynb <https://github.com/DestinE-Climate-DT/AQUA/blob/main/notebooks/diagnostics/timeseries/timeseries.ipynb>`_
* `seasonalcycles.ipynb <https://github.com/DestinE-Climate-DT/AQUA/blob/main/notebooks/diagnostics/timeseries/seasonalcycles.ipynb>`_
* `gregory.ipynb <have://github.com/DestinE-Climate-DT/AQUA/blob/main/notebooks/diagnostics/timeseries/gregory.ipynb>`_

Authors and contributors
------------------------

This diagnostic is maintained by Matteo Nurisso (`@mnurisso <https://github.com/mnurisso>`_, `m.nurisso@isac.cnr.it <mailto:m.nurisso@isac.cnr.it>`_).
Contributions are welcome, please open an issue or a pull request.
If you have any doubt or suggestion, please contact the AQUA team or the maintainers.

Detailed API
------------

This section provides a detailed reference for the Application Programming Interface (API) of the ``timeseries`` diagnostic,
produced from the diagnostic function docstrings.

.. automodule:: aqua.diagnostics.timeseries
    :members:
    :undoc-members:
    :show-inheritance:
