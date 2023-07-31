Ocean3D
================

Description
-----------

This package contains two submodules to monitor and evaluate the 3D ocean evolution and mean state:

`Ocean Drifts` to characterise and monitor model drifts 
`Ocean Circulation` to evaluate the realism of the model in simulating key precursors of the ocean circulation

All these diagnostics are produced in a consistent way, for a selected model simulation.

Structure
-----------

The ocean3d diagnostic follows a class structure and consists of the files:

* `ocean_drifts.py`: a python file that contains the functions that are specific for the computation of the ocean drift diagnostics;
* `ocean_circulation.py`: a python file that contains the functions that are specific for the computation of the ocean circulation diagnostics;
* `ocean_utils.py`: a python file that contains functions that are used in both ocean3d diagnostic modules; 
* `notebooks/ocean_drifts.ipynb`: an ipython notebook with examples on how to apply the ocean drift diagnostics;
* `notebooks/ocean_circulation.ipynb`: an ipython notebook with examples on how to apply the ocean circulation diagnostics;
* `README.md` : a readme file which contains some tecnical information on how to install the ocean3d diagnostic and its environment. 

Input variables example
------------------------

* ocpt (Ocean potential temperature, GRIB paramid 150129)
* so     (Sea water practical salinity, GRIB paramid 151130)



Output 
------

This module exports all the data and diagnostics  used to create the different figures.
  

Functions used 
---------------

.. code-block:: python

    hovmoller_lev_time_plot(data, region, anomaly: bool = False,standardise: bool =False, anomaly_ref= None ,
                            latS: float=None, latN: float=None, lonW: float=None,lonE: float=None,
                            output= False, output_dir= None)

*hovmoller_lev_time_plot*: This function requires data, a region, and the type of data processing. 
It produces a Hovmöller plot of regionally averaged temperature and salinity with the selected preprocessing of the data (e.g., whether anomalies are computed and how, and whether they are normalized or not). 
The region name supports all the major oceans and seas, in case users require a custom region, they can fill in the values of latitude, and longitude in the boxes as desired.


.. code-block:: python

    time_series_multilevs(data, region=None, anomaly: bool = False, standardise: bool =False, anomaly_ref = None,
                          customise_level=False, levels=None,
                          latS: float=None, latN: float=None, lonW: float=None,lonE: float=None,
                          output= True, output_dir = None)

*time_series_multilevs*: This function requires data, a region, the type of data processing, and optional depth levels. 
It produces time series plots of regionally averaged temperature and salinity with the selected preprocessing of the data for a predefined or customized list of vertical levels. 
The region name supports all the major oceans and seas, in case users require a custom region, they can fill in the values of latitude, and longitude in the boxes.


.. code-block:: python

    multilevel_t_s_trend_plot(data, region=None, customise_level=False, levels=None,
                              latS: float = None, latN: float = None, lonW: float = None,lonE: float = None,
                              output=True, output_dir= None)

*multilevel_t_s_trend_plot*: This function requires data, a region, and optional depth levels. 
It produces lon-lat maps of linear temporal trends of temperature and salinity over the selected region for a predefined or customized list of vertical levels. 
The region name supports all the major oceans and seas, in case users require a custom region, they can fill in the values of latitude, and longitude in the boxes.


.. code-block:: python

   zonal_mean_trend_plot(data, region=None,
                        latS: float = None, latN: float = None, lonW: float = None,lonE: float = None,
                        output=True, output_dir= None)

*Zonal Mean Trend Plot*: This function requires data for a region. 
It produces plots of zonally averaged linear temporal trends plot of temperature and salinity as a function of depth and latitude. 
The zonal average is produced over the selected region, whose name supports all the major oceans and seas; in case users require a custom region, 
they can fill in the values of latitude and longitude in the boxes.


.. code-block:: python

    plot_stratification(mod_data, region=None, time=None,
                        latS: float = None, latN: float = None, lonW: float = None,lonE: float = None,
                        output=True, output_dir= None)

*Stratification plot*: This function requires data, a region, and the time of the climatology. 
It produces a stratification plot of Temperature, salinity and Density, including the overlapped data with the observation. 
The region name supports all the major oceans and seas, in case users require a custom region, they can fill in the values of latitude, and longitude in the boxes.


.. code-block:: python

    plot_spatial_mld_clim(mod_data, region=None, time=None,
                          latS: float = None, latN: float = None, lonW: float = None, lonE: float = None,
                          overlap=False, output=False, output_dir= None)

Mixed Layer Depth Plot: This function requires data, a region, and the time of the climatology. 
It produces a time series plot of temperature and salinity.
Users have the option of choosing whether they want to use the whole obs data or overlapped obs data with the model. 
The region name supports all the major oceans and seas; in case users require a custom region, they can fill in the values of latitude, and longitude in the boxes.


Methods used 
---------------
All regional averages have been produced with area weights.

Temporal trends are computed as linear trends and estimated over the whole temporal span of the dataset.

The mixed layer depth is computed by the function `compute_mld_cont` from sigma0 monthly fields following the criteria from de Boyer Montegut et al (2004)

Density fields are computed from absolute salinity and conservative temperature fields using the TEOS-10 equations. Absolute salinity and conservative temperature are also computed from practical salinity and potential temperature fields with TEOS-10 equations.


Observations  
---------------

EN4.2.2.g10 ocpt and so observations for the period 1950-2022


.. This set of diagnostics has been developed to monitor potential drifts and initialization shock in the models.
.. Observations do not provide any added value for the identification of the drift and were not considered.


References
----------
de Boyer Montégut, C., Madec, G., Fischer, A. S., Lazar, A., and Iudicone, D. (2004): Mixed layer depth over the global ocean: An examination of profile data and a profile-based climatology. J. Geophys. Res., 109, C12003, doi:10.1029/2004JC002378

Gouretski and Reseghetti (2010): On depth and temperature biases in bathythermograph data: development of a new correction scheme based on analysis of a global ocean database. Deep-Sea Research I, 57, 6. doi: http://dx.doi.org/10.1016/j.dsr.2010.03.011

https://www.teos-10.org/


A code to compute very efficiently the linear trends has been adapted from this website:
https://stackoverflow.com/questions/52108417/how-to-apply-linear-regression-to-every-pixel-in-a-large-multi-dimensional-array


Example Plot(s)
---------------

* This is an example of one of the hovmöller T, S figures

.. figure:: figures/ocean3d1.png
    :width: 18cm


* This is an example of the multipanel plots of the spatially averaged T, S timeseries at different levels

.. figure:: figures/ocean3d2.png
    :width: 18cm


* This is an example of the multi-panel plots of lat-lon  maps of temporal trends in T, S at different levels

.. figure:: figures/ocean3d3.png
    :width: 18cm


* This is an example of the plots of lat-depth maps of temporal trends in T, S

.. figure:: figures/ocean3d4.png
    :width: 18cm


* This is an example of one of the climatological stratification profiles.

.. figure:: figures/ocean3d5.png
    :width: 20cm


* This is an example of one of the mixed-layer depth climatologies. Unit is meter. 

.. figure:: figures/ocean3d6.png
    :width: 20cm


Available demo notebooks
------------------------

Notebooks are stored in diagnostics/ocean3d/notebooks

* `global_ocean.ipynb <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/ocean3d/notebooks/ocean_drifts.ipynb>`_

    
* `ocean_circulation.ipynb <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/ocean3d/notebooks/ocean_circulation.ipynb>`_
    
Detailed API
------------

This section provides a detailed reference for the Application Programming Interface (API) of the "ocean3d" diagnostic,
produced from the diagnostic function docstrings.

.. automodule:: ocean3d
    :members:
    :undoc-members:
    :show-inheritance:
