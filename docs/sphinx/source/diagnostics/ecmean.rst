Performance Indices
===================

`ECmean4 <https://pypi.org/project/ECmean4>`_ is an open source package which has been integrated within AQUA,
aiming at computing the Reichler and Kim Performance Indices (PIs).
Those numbers provides an estimates of the DE_340 models climatological *skill* of some selected atmospheric and oceanic fields.

Description
-----------

The diagnostic computes the `Reichler and Kim Performance Indices <https://journals.ametsoc.org/view/journals/bams/89/3/bams-89-3-303.xml>`_, usually known as PIs. 
Following the ECmean4 implementation, some minor differences from the original definition has been introduced,
so that the PIs are computed on a common (1x1 deg) grid rather than on the model grid.
From the original definition a few improvements has been introduced, using updated climatologies and provides the PIs also for a set of selected regions and seasons. 

From the formal point of view, PIs are computed as the root mean square error of a selected 2D field normalized by the
interannual variance estimated from the observations. Larger values implies worse performance (i.e. larger bias).
In the plots produced by ECmean4 implementation, PIs are normalized by the (precomputed) average of CMIP6 climate models,
so that number smaller than one implies a better performance than CMIP6 model.

Structure
-----------

For detailed information on the code, please refer to the `official ECmean4 documentation <https://ecmean4.readthedocs.io/en/latest/>`_.  

Input variables 
---------------

For Performance Indices the following variables are requested:

* ``mtpr`` (Mean total precipitation rate, GRIB paramid 235055)
* ``2t``     (2 metre temperature, GRIB paramid 167)
* ``msl``    (mean sea level pressure, GRIB paramid 151)
* ``metss``  (eastward wind stress, GRIB paramid 180)
* ``mntss``  (northward wind stress, GRIB paramid 181)
* ``t``      (air temperature, GRIB paramid 130)        
* ``u``      (zonal wind, GRIB paramid 131)
* ``v``      (meridional wind, GRIB paramid 132)
* ``q``      (specific humidity, GRIB paramid 133)
* ``avg_tos``    (sea surface temperature, GRIB paramid 263101)
* ``avg_sos``    (sea surface salinity, GRIB paramid 263100)
* ``avg_siconc``     (sea ice concentration, GRIB paramid 263001)
* ``msshf``     (surface sensible heat flux, GRIB paramid 235033, required for net surface flux computation)
* ``mslhf```    (surface latent heat flux, GRIB paramid 235034, required for net surface flux computation)
* ``msnlwrf``  (surface net longwave radiation flux, GRIB paramid 235038, required for net surface flux computation)
* ``msnswrf``   (surface net shortwave radiation flux, GRIB paramid 235037, required for net surface flux computation)
* ``msr``      (snowfall rate, GRIB paramid 235031, required for net surface flux computation)

If a variable (or more) is missing, blank line will be reported in the output figures. 

3D fields are zonally averaged, so that the PIs reports the performance on the zonal field. 


.. note ::
    ECmean4 is made to work with CMOR variables, but can handle name and file conversion with specification of
    an `interface file <https://ecmean4.readthedocs.io/en/latest/configuration.html#interface-files>`_.
    An AQUA specific one has been designed for this purpose to work with Climate DT Phase 1. 
    Updates in the Data Governance will require updates to the interface file.  
    In addition, although PI can work directly on the model raw output, the interface file is made to work only
    with the Low Resolution Archive (LRA) to reduce the amount of computation required. 


Output 
------

The result are stored as a YAML file, indicating PIs for each variable, region and season, that can be stored for later evaluation.
Most importantly, a figure is produced showing a score card for the different regions, variables and seasons.
For the sake of simplicity, the PIs figure is computed as the ratio between the model PI and the average value estimated over the (precomputed) ensemble of CMIP6 models. 
Numbers lower than one implies that the model is performing better than the average of CMIP6 models. 

Methods and functions used
--------------------------

Please refer to the `official ECmean4 documentation <https://ecmean4.readthedocs.io/en/latest/>`_. 

Observations
------------

ECmean4 uses multiple sources as reference climatologies: please refer to the `Official climatology description <https://ecmean4.readthedocs.io/en/latest/performanceindices.html#climatologies-available>`_ to get more insight. 

References
----------

* Reichler, T., and J. Kim, 2008: How Well Do Coupled Models Simulate Today's Climate?. Bull. Amer. Meteor. Soc., 89, 303-312, https://doi.org/10.1175/BAMS-89-3-303.

Example Plot(s)
---------------

.. figure:: figures/ecmean-pi.png
    :width: 15cm

    An example of the Performance Indices computed on a single year of the tco2599-ng5 simulation from NextGEMS Cycle2 run.

Available demo notebooks
------------------------

Notebooks are stored in ``diagnostics/ecmean/notebook``.

* `ecmean-test.ipynb <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/ecmean/notebook/ecmean-test.ipynb>`_
