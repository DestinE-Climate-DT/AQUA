Diagnostics
===========

Overview of Available Diagnostics
---------------------------------

AQUA provides a collection of built-in diagnostics to analyze climate model outputs. 
In order to provide a comprehensive assessment, AQUA diagnostics have been divided in two big families: 

1.	The first set, named **frontier**, identifies a set of diagnostics which will run digesting the high-resolution - both temporal and spatial - full GSV data to provide insight  at climatological scales on physical/dynamical processes that could not be investigated with classical climate simulations so far.  
2.	The second family, named **state-of-the-art**, lists diagnostics which will use the archived low resolution GSV data as input, accessed through Data Bridge. Most of these diagnostics can be compared with observations to produce metrics of evaluation and aim at providing an assessment of the model against observational datasets and, in some selected occasions, pre-existing climate simulations. 

Frontier Diagnostics
++++++++++++++++++++

This list includes diagnostics which will provide novel insight on specific climate phenomena which cannot be resolved or studied 
in the current generation of global climate models configured at standard resolutions. 

The goal of most of these diagnostics is not to make an assessment about the quality 
of the simulations or to compare them against existing simulations, but rather to demonstrate the scientific and technical possibilities
offered by the new high-resolution data from the Digital Twin. 
Most importantly, they will work directly on the full temporal and/or spatial resolution of the climate models. 

Currently implemented diagnostics are:

.. toctree::
   :maxdepth: 1
   
   ssh_variability
   tropical_cyclones
   tropical-rainfall

State-of-the-art diagnostics
++++++++++++++++++++++++++++

This list includes such diagnostics whose goal is to monitor and diagnose possible model drifts, imbalances and biases. 

These diagnostics differs from the “frontier” ones by not needing the full climate model resolution and the full data frequency 
since they will work on aggregated coarse resolution data. They are mostly based on data from Low Resolution Archive (LRA). 

Currently implemented diagnostics are:

.. toctree::
   :maxdepth: 1

   atm_global_mean
   ecmean
   global_mean_timeseries
   ocean3d
   radiation
   seaice
   teleconnections

Creating Custom Diagnostics
---------------------------

AQUA allows users to create custom diagnostics to suit their specific needs. 
Custom diagnostics can be implemented as Python functions or classes and integrated into AQUA's diagnostic framework.

To support the development of new diagnostic, a `dummy diagnostic <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/dummy/README.md>`_
has been introduced into the code to support future development and serve as starting point. 
This includes multiple template files and demo code, ranging from the creation 
of a proper README up to a command line interface tool, going through the documentation, notebook and tests. 

