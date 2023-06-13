Getting Started
===============

Basic Concepts
--------------

AQUA is designed to simplify the diagnostics process on high-resolution climate models. 
This is done by creating a series of tools that simplifies data access and basic data operations so that the 
users - diagnostics developers or climate researchers interested in accessing model data - can focus only on scientific analysis.
For this reason, whatever object accessed by AQUA is delivered as a `xarray <https://docs.xarray.dev/en/stable/>`_ object.
The package is built around a few core concepts:

- Data reading and preprocessing: Data are exposed through `intake <https://intake.readthedocs.io/en/stable/>`_ catalogues 
  and represented as `xarray <https://docs.xarray.dev/en/stable/>`_ objects. This allows us to easily read and preprocess data from various sources, including local files, remote servers, and cloud storage, from climate models and observational datasets.
- Data fixing: AQUA provides capabilities to change metadata (e.g., variable names) and data themselves
  (e.g., convert to different units) so that model data reach the users with a common data format.
- Regridding and interpolation: AQUA offers robust regridding and interpolation functionalities 
  to align datasets with different grids and spatial resolutions.
- Averaging and aggregation: AQUA provides tools to perform temporal and spatial averaging and aggregation of climate data.
- Parallel processing: AQUA supports parallel processing through `dask <https://examples.dask.org/xarray.html>`_ to 
  speed up the execution of diagnostics.
- Diagnostics: most importantly, AQUA includes a set of built-in diagnostic tools,
  and it allows users to create custom diagnostics as well.

Example Use Case
----------------

To demonstrate a simple use case, we will walk you through an example of using AQUA to interpolate atmospherically
temperature data to 1°x1° grid and calculate the mean global temperature time series on the original grid.

.. code-block:: python

   from aqua import Reader

   # Instantiate a Reader object specifying the type of data which we want to read from the catalogue. 
   # We also specify that we would like to regrid to a 1°x1° grid.
   reader = Reader(model="ICON", exp="ngc2009", source="atm_2d_ml_R02B09", regrid='r100')

   # Retrieve the data. The fix=True will convert variable names to GRIB standard. 
   data = reader.retrieve(fix=True)

   # Perform temperal averaging of the first 10 time steps, using standard xarray syntax.:
   tasr_mean = data['2t'][:10].mean(dim='time')
   
   # Regrid the data to a 1°x1° grid
   tasr_mean_regrided = reader.regrid(tasr_mean)

   # Do weighted average of the data on original grid:
   global_mean = reader.fldmean(data['2t'][100:200, :])


For more detailed examples and tutorials, refer to the :doc:`examples_tutorials` section of this documentation
or explore the Jupyter notebooks provided with AQUA.
