Radiation Budget Diagnostic
=====================================

This package provides a diagnostic for assessing the model radiative budget imbalances at the top of atmosphere (TOA) (surface to be added). It aims to detect potential biases in the models. The diagnostic includes various plots, such as Gregory plots, to provide insights into lingering model drifts or general biases in the radiation budget. The data this diagnostic uses are nextGEMS cycle 2 and nextGEMS cycle 3 data. Additionally data for comparison is needed: Satellite data from the CERES Energy Balanced and Filled (CERES-EBAF catalogue) and reanalysis (ERA5) is used.

Description
-----------

The file functions.py contains all the functions to load, process and plot the model and observation data. A class called radiation_diag contains all these functions.

Data requirements: 
Variables needed for this diagnostic so far are:
* ttr (total thermal radiadion): longwave radiation
* tsr (total solar radiation): shortwave radiation
* tnr (total net radiation): net radiation
* 2t (2 meter temperature): only used for the Gregory plots.

The data we retrieve through the provided functions have monthly timesteps and a 1x1 deg resolution. A higher resolution is not necessary for this diagnostic.

Run the diagnostic
-------------------

Import the necessary functions from the `AQUA` framework and import the radiation_diag-class

.. code-block:: python

  from aqua import Reader, catalogue, inspect_catalogue
  from functions import radiation_diag
  
Now we use some functions to load the data we need. Since this diagnostic aims to inform about model stability, a high resolution is not necessary. Instead monthly data with a regular 1 x 1 grid is used. This data can be retrieved and processed from the Low Resolution Archive (LRA), that is part of the AQUA framework. 
Example on how to load the datasets (e.g. IFS Cycle 3 4.4 km data) from the LRA:

.. code-block:: python

    TOA_ifs_4km_gm, reader_ifs_4km, data_ifs_4km, TOA_ifs_4km, TOA_ifs_4km_r360x180 = radiation_diag.process_model_data(model =  'IFS' , exp = 'tco2559-ng5-cycle3' , source = 'lra-r100-monthly')
    
The returned data contains:
* TOA_ifs_4km_gm: Global means of the data
* reader_ifs_4km: AQUA reader, that could also be retrieved via: 'reader = Reader(model=model,exp=exp,source=source)'
* data_ifs_4km: returned xarray-dataset, that contains all the variables
* TOA_ifs_4km: returned xarray-dataset, that contains the necessary variables for analysis

Some functions inside the radiation_diag class:
* process_ceres_data(): Extracts CERES data for further analysis and creates global means.
* process_model_data(): Extracts model output data for further analysis and creates global means.
* process_era5_data(): Extracts ERA5 data for further analysis.
* gregory_plot(): Creates a Gregory Plot with various models and ERA5 data.
* barplot_model_data(): Creates a bar plot with various models and CERES data.
* plot_maps(): Creates monthly bias maps of various models to show the bias wrt CERES data.

Have a look at the 'functions_demo.ipynb' notebook to see some of the outputs of this diagnostic.


Example plots
-------------
.. figure:: figures/radiation/Gregory_Plot.png
   :width: 200px
   :align: center

   Gregory Plot of different models and ERA5 data.
   
.. figure:: figures/radiation/TimeSeries.png
   :width: 200px
   :align: center

   Time series of model biases with respect to CERES data.   

.. figure:: figures/radiation/BarPlot.png
   :width: 200px
   :align: center

   Bar plot to show the radiation imbalances
   
   
