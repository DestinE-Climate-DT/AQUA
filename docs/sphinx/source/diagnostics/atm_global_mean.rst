Atmospheric Global Mean Diagnostic
=====================================

The tool provides a flexible approach to analyze and visualize 2D biases for multiple atmospheric fields. It primarily focuses on comparing the fields with ERA5 data in different seasons to identify areas of significant biases. The tool supports the analysis of variables such as surface temperature, winds, temperature, and moisture along different pressure levels. The data this diagnostic uses is nextGEMS cycle 2 and nextGEMS cycle 3 data. For accessing the data the Low Resolution Archive (LRA) is used.

Description
-----------

The file atm_global_mean.py contains the functions analyze the data and plot biases of the models

Data requirements
-----------------
This tool requires the following atmospheric variables:

- 2m temperature
- Zonal and meridional wind
- Temperature
- Specific humidity
- Precipitation


The data we retrieve through the provided functions have monthly timesteps and a 1x1 deg resolution. A higher resolution is not necessary for this diagnostic.

Run the diagnostic
-------------------

Import the necessary functions from the `AQUA` framework and import the AGM_diag-class

.. code-block:: python

  from aqua import Reader, catalogue, inspect_catalogue
  from atm_global_mean import AGM_diag

to be continued

Example plots
-------------
.. figure:: figures/atm_global_mean/x.png
   :width: 200px
   :align: center
