.. _diagnostics:
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

Running Diagnostics
-------------------

Together with the diagnostics, AQUA provides a command line interface tool to run them.
The tool is called `aqua-analysis` and it is available in the `cli` directory of the repository.
It can be used to run a single diagnostic or a group of diagnostics.
It consists of a single bash script that can be run from the command line.
This script will take care of calling the correct Python script for each diagnostic.

It is possible to set the following options by changing the values of the variables in the script
or by passing them as command line arguments:

- **model_atm**: name of the atmospheric model to use (default: `IFS`)
- **model_ocean**: name of the ocean model to use (default: `FESOM`)
- **exp**: name of the experiment to use (default: `tco2559-ng5-cycle3`)
- **source**: name of the source to use (default: `lra-r100-monthly`)
- **outputdir**: name of the output directory to use (default: `./output`)
- **machine**: name of the machine to use (default: `levante`)
- **max_threads**: maximum number of threads to use (default: `-1`, i.e. use all available threads)
- **loglevel**: log level to use (default: `WARNING`)

The AQUA folder can be defined by modifying the `aqua` variable in the script, but the
preferred way is to set the `AQUA` environment variable to the path of the AQUA folder.

.. warning::
   The analysis has to be performed on LRA data, meaning that data should be aggregated
   to a resolution of 1 degree in both latitude and longitude and to a monthly frequency.

.. note::
   Only the model can be specified differently for atmosphere and ocean (i.e. IFS and FESOM),
   while the experiment and the source have to be the same for both atmosphere and ocean.
   This is because the diagnostics are designed to work on both
   atmosphere and ocean with the LRA data produced by the same simulation.

Once the options have been set, the diagnostics can be run by executing the script:

.. code-block:: bash

   ./aqua-analysis

or by passing the options as command line arguments:

.. code-block:: bash

   ./aqua-analysis --model-atm IFS --model-ocean FESOM --exp tco2559-ng5-cycle3 --source lra-r100-monthly --outputdir ./output --machine levante --max-threads -1 --loglevel WARNING

Advanced options
++++++++++++++++

It is possible to run a subset of the diagnostics by modifying the arrays `atm_diagnostics` and
`oce_diagnostics` in the script. By default, all the diagnostics are run.

A preparation diagnostic called `dummy` is run first. This diagnostic is used to check that at
least one between the atmospheric and oceanic model is accessible. To turn off this check, 
set the `run_dummy` variable to `false`.

Some diagnostics may accept additional options. These options are listed in the comments of the
script and can be set by modifying the `atm_extra_args` or `oce_extra_args` arrays.

.. note::
   The `aqua-analysis` tool is a simple way to run the diagnostics with a single command and predefined options.
   However, it is possible to run the diagnostics in other ways, for example by calling the Python scripts directly or by
   running them in a Jupyter notebook, allowing more flexibility and customization.

Creating Custom Diagnostics
---------------------------

AQUA allows users to create custom diagnostics to suit their specific needs. 
Custom diagnostics can be implemented as Python functions or classes and integrated into AQUA's diagnostic framework.

To support the development of new diagnostic, a `dummy diagnostic <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/dummy/README.md>`_
has been introduced into the code to support future development and serve as starting point. 
This includes multiple template files and demo code, ranging from the creation 
of a proper README up to a command line interface tool, going through the documentation, notebook and tests. 
