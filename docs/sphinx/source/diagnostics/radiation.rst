Radiation Budget Diagnostic
=============================


Description
-----------

This package provides a diagnostic for assessing the model radiative budget imbalances at the top of atmosphere (toa) an surface (sfc). It aims to detect potential biases in the models. The diagnostic includes various plots, such as bias maps and boxplots, to provide insights into lingering model drifts or general biases in the radiation budget. The data this diagnostic is using for demonstration purposes is the Climate DT IFS NEMO historical data (notebooks and figures show the historical-1990-dev-lowres and historical-1990 data retrived from the AQUA Low Resolution Archive). To compare the models with observation the satellite data from the CERES Energy Balanced and Filled (CERES-EBAF catalogue) is used. The file ``radiation_functions.py`` contains all the functions to load, process and plot the model and observation data. Additionaly, the diagnastic can be executed from the command line interface using the ``cli_radiation.py`` file.

Structure
---------

*  ``README.md``: a readme file which contains some technical information on how to install the diagnostic and its environment;
*  ``radiation_functions.py``: a python file in which the functions are implemented;
*  ``time_series_IFS_NEMO.ipynb``: a notebook that shows how the output of the ``plot_model_comparison_timeseries`` function;
*  ``boxplot_IFS_NEMO.ipynb``: a notebook that demonstrates how to create bar plots;
*  ``bias_maps_IFS_NEMO.ipynb``: a notebook that creates bias maps to localise significant biases in comparison to CERES data. 

Input variables 
---------------

Usual radiative fluxes can be analyzed such as:

*  ``mtnlwrf`` (total thermal radiation): longwave radiation;
*  ``mtnswrf`` (total solar radiation): shortwave radiation;
*  ``tnr`` (total net radiation): net radiation.

The data we retrieve from the LRA through the provided functions have monthly timesteps and a 1x1 deg resolution.
A higher resolution is not necessary for this diagnostic.

Output
------

This diagnostic produces figures that are saved in a PDF file and NetCDF data per function used.

Functions used 
---------------

In the following, we report a usage example which illustrates how to load the data.
Since this diagnostic aims to inform about model stability, a high resolution is not necessary.
Instead monthly data with a regular 1 x 1 grid is used.
This data can be retrieved and processed from the Low Resolution Archive (LRA), that is part of the AQUA framework. 
Example on how to load the datasets from the LRA:

.. code-block:: python

   ifs_nemo_historical_dev = process_model_data(model='IFS-NEMO', exp='historical-1990',
                                                source='lra-r100-monthly', fix = True)

The returned data contains a dictionary containing the following information:

*  ``model``: Model name;
*  ``exp``: Experiment name``;
*  ``source``: Source name;
*  ``gm``: Global means of model output data;
*  ``data``: Model output data.

Other available functions:

*  ``process_ceres_data``: extracts CERES data for further analysis and creates global means;
*  ``process_model_data``: extracts model output data for further analysis and creates global means;
*  ``boxplot_model_data``: creates a box plot with various models and CERES data;
*  ``plot_model_comparison_timeseries``: creates a time series and visualizes the variability of the values wrt CERES years;
*  ``plot_mean_bias``: Compare the model climatology against CERES climatology for ttr, tnr and tsr


Observation
-----------

The radiation data are compared to CERES data.


References
----------

* https://github.com/nextGEMS/nextGEMS_Cycle3/blob/main/IFS/radiation_evaluation.ipynb


Example plots
-------------
   
.. figure:: figures/timeseries_ifs-nemo_historical-1990_lra-r100-monthly-1.png
   :width: 600px
   :align: center

   Figure 1: Time series of model biases with respect to CERES data.   

.. figure:: figures/boxplot_mtnlwrf_mtnswrf_ceres_ebaf-toa41_monthly_ifs-nemo_historical-1990_lra-r100-monthly-1.png
   :width: 600px
   :align: center

   Figure 2: Box plot to show the globally averaged incoming and outgoing TOA radiation.

.. figure:: figures/toa_mean_biases_tnr_ifs-nemo_historical-1990_lra-r100-monthly_1990_1998_ceres_ebaf-toa41_monthly-1.png
   :width: 600px
   :align: center

   Figure 3: Mean bias map of the net radiation with respect to CERES.
   
   
Available demo notebooks
------------------------

Notebooks are stored in ``diagnostics/radiation/notebooks``:

* `time_series_IFS_NEMO.ipynb <https://github.com/DestinE-Climate-DT/AQUA/blob/main/diagnostics/radiation/notebooks/time_series_IFS_NEMO.ipynb>`_
* `boxplot_IFS_NEMO.ipynb <https://github.com/DestinE-Climate-DT/AQUA/blob/main/diagnostics/radiation/notebooks/boxplot_IFS_NEMO.ipynb>`_
* `bias_maps_IFS_NEMO.ipynb <https://github.com/DestinE-Climate-DT/AQUA/blob/main/diagnostics/radiation/notebooks/bias_maps_IFS_NEMO.ipynb>`_    
   
Detailed API
------------
This section provides a detailed reference for the Application Programming Interface (API) of the "radiation" diagnostic, produced from the diagnostic function docstrings.

.. automodule:: radiation
    :members:
    :undoc-members:
    :show-inheritance:
