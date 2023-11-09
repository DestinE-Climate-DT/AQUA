Atmospheric Global Mean Biases Diagnostic
==========================================

Description
-----------

The tool provides a flexible approach to analyze and visualize 2D biases for multiple atmospheric fields. It primarily focuses on comparing the fields with ERA5 data in different seasons to identify areas of significant biases. The tool supports the analysis of variables such as surface temperature, winds, temperature, and moisture along different pressure levels. The data this diagnostic uses to demonstrate the functions is the nextGEMS cycle 3 data, but any data from the catalogue can be passed to the function. For accessing the data the Low Resolution Archive (LRA) is used. The file atm_global_mean.py contains the functions analyze the data and plot biases of the models.

Structure
---------
The diagnostic follows a class structure and consists of the file:
* atm_global_mean.py: a python file, where the class and the methods are included

Input variables 
----------------
Variables that can be analyzed within this tool are the following:

* 2m temperature (2t)
* Total Precipitation (tprate)
* Zonal and meridional wind (u, v)
* Temperature (t)
* Specific humidity (q)

The data we retrieve through the provided functions have monthly timesteps and a 1x1 deg resolution.
A higher resolution is not necessary for this diagnostic.


Output
------
The output files generated in this diagnostic are figures, that are saved in a PDF format as well as NetCDF data to reproduce theese figures. 

Run the diagnostic
-------------------

Import the necessary functions from the ``AQUA`` framework and import the ``AGM_diag-class``.

.. code-block:: python

  from aqua import Reader, catalogue, inspect_catalogue
  from atm_global_mean import AGM_diag
  

Functions used
--------------
Following methods are used in the AGM_diag class (inside the ``atm_global_mean.py`` file):

* ``seasonal_bias``: Plot the seasonal bias maps between two datasets for a specific variable and year.
* ``compare_datasets_plev``:  Compare two datasets and plot the zonal bias for a selected model time range with respect to the ERA5 climatology from 2000-2020.
* ``plot_map_with_stats``: Plot a map of a chosen variable from a dataset with colorbar and statistics.
* ``README.md`` : a readme file which contains some basic information about this tool.

Observations
------------
Data will be compared to ERA5 data

Example plots
-------------

.. figure:: figures/atm_global_mean.png
   :width: 20cm
   :align: center

   Example map to visualize the bias of the 2t-variable IFS 4.4 km run with respect to the ERA5 climatology.


Available demo noteboks
------------------------
    
* `agm_ng3_seasons.ipynb <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/atmglobalmean/notebooks/agm_ng3_seasons.ipynb>`_: Notebook to demonstrate the seasonal_bias method with example plots
* `agm_ng3_plev_test.ipynb <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/atmglobalmean/notebooks/agm_ng3_plev_test.ipynb>`_: Notebook to demonstrate the compare_datasets_plev method with example plots
* `agm_ng3_mean_plots.ipynb <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/atmglobalmean/notebooks/agm_ng3_mean_plots.ipynb>`_: Notebook to demonstrate the plot_map_with_stats method with example plots

    
Detailed API
------------

This section provides a detailed reference for the Application Programming Interface (API) of the "atm_global_mean" diagnostic,
produced from the diagnostic function docstrings.

.. automodule:: atmglobalmean
    :members:
    :undoc-members:
    :show-inheritance: