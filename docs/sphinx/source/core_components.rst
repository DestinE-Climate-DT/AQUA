Core Components
===============

The `Reader` Class
------------------
The ``Reader`` class provides AQUA access to data, developed to offer a centralized common data access point.
AQUA ``Reader`` can, in fact, access different file formats and data from the FDB or intake catalogues, 
and delivers xarray objects.
On top of data access, the ``Reader`` is also able to perform multiple operations on the data: interpolation and regridding,
spatial and temporal averaging and metadata correction. 
Since streaming is not yet available, a streaming emulator has been introduced and can mimic streaming.


Data Reading and Preprocessing
------------------------------

Supported File Formats
~~~~~~~~~~~~~~~~~~~~~~

AQUA supports a variety of climate data file formats, including:

- NetCDF
- GRIB files
- Zarr
- FDB GRIB

Data Structures
~~~~~~~~~~~~~~~~

AQUA uses xarray data structures to manage and manipulate climate data. The primary data structures used in AQUA are:

- DataArray: Represents a single variable with multiple dimensions (e.g., time, latitude, longitude).
- Dataset: Represents a collection of DataArrays with shared dimensions.

Basic Usage
~~~~~~~~~~~~~~~~
Accessing data with the AQUA ``Reader`` is very straightforward.
To check what is available in the catalogue, we can use the ``inspect_catalogue`` function.
Three hierarchical layer structures describe each dataset. At the top level, there are "models" (e.g., ICON, IFS, etc.). 
Each model has different "experiments," and each "experiment" (keyword ``exp``) can have different "sources".

Calling, for example, 

.. code-block:: python

    from aqua import inspect_catalogue
    inspect_catalogue(model='CERES')

will return experiments available in the catalogue for model CERES.
If model, experiment and source are all specified, for FDB sources (e.g. on LUMI) the function
will return a list of available variables, if specified in the catalogue.
For non-FDB sources or if the variables have not been defined it will simply return a boolean
value to indicate if that combination exists.

It is also possible to get an overview of the full catalogue on the current machine with the
``catalogue()`` function.
This will both list to screen the contents of the catalogue and
return a catalogue object.

Let's try to load some ICON data with the AQUA ``Reader``.
We first instantiate a ``Reader`` object specifying the type of data we want to read from the catalogue.
As mentioned, the ``Reader`` access is built on a 3-level hierarchy of model, experiment, and source.  

.. code-block:: python

    reader = Reader(model="ICON", exp="ngc2009", source="atm_2d_ml_R02B09", fix=False)

Now we can read the actual data with the ``retrieve`` method.

.. code-block:: python

    data = reader.retrieve()

The reader returns an xarray.Dataset with raw ICON data on the original grid.

If some information about the data is needed, it is possible to use the ``info`` method of the ``Reader`` class.

.. code-block:: python

    reader.info()

This will print to screen some information about the data, including the grid, fixes, regrid setup and FDB details if available.


Interpolation and Regridding
----------------------------
AQUA provides functions to interpolate and regrid data to match the spatial resolution of different datasets. 
AQUA regridding functionalities are based on the external tool `smmregrid <https://github.com/jhardenberg/smmregrid>`_ which 
operates sparse matrix computation based on externally-computed weights. 


The idea of the regridder is first to generate the weights for the interpolation and then to use them for each regridding operation. 
The reader generates the regridding weights automatically (with CDO) if not already existent and stored
in a directory specified in the ``config/machine/<machine-name>/catalog.yaml`` file.
A list of predefined target grids (only regular lon-lat for now) is available in the ``config/aqua-grids.yaml`` file.
For example, "r100" is a regular grid at 1° resolution.

In other words, weights are computed externally by CDO (an operation that needs to be done only once) and 
then stored on the machine so that further operations are considerably fast. 

Such an approach has two main advantages:

1. All operations are done in memory so that no I/O is required, and the operations are faster than with CDO
2. Operations can be easily parallelized with Dask, bringing further speedup.

In the long term, it will be possible to support also other interpolation software,
such as `ESMF <https://earthsystemmodeling.org/>`_ or `MIR <https://github.com/ecmwf/mir>`_.

Let's see a practical example.
We instantiate a reader for ICON data specifying that we will want to interpolate to a 1° grid. 
As mentioned, if the weights file does not exist in our collection, it will be created automatically.

.. code-block:: python

    reader = Reader(model="ICON", exp="ngc2009", source="atm_2d_ml_R02B09", regrid="r100")
    data = reader.retrieve()

.. note::
    Notice that these data still need to be regridded. You could ask to regrid it to the destination grid which we chose when we instantiated the reader.
    Please be warned that this will take longer without a selection.
    It is usually more efficient to load the data, select it, and then regrid it.

Now we regrid part of the data (the temperature of the first 100 frames):

.. code-block:: python

    tasr = reader.regrid(data.tas[0:100,:])

The result is an xarray containing 360x180 grid points for each timeframe. 

Aside from the horizontal interpolation, AQUA offers also the possibility of performing a simple linear vertical interpolation building 
on the capabilities of Xarray. This is done with the ``vertinterp`` method of the ``Reader`` class.
This can of course be use in the combination of the ``regrid`` method so that it is possible to operate 
both interpolations in a few steps.
Users can also change the unit of the vertical coordinate, and the method works with both Datasets and DataArrays

.. code-block:: python

    reader = Reader(model="IFS", exp="tco2559-ng5", regrid = 'r100', source="ICMU_atm3d")
    data = reader.retrieve()
    field = reader.regrid(data['u'][0:5,:,:])
    interp = reader.vertinterp(field, [830, 835], units = 'hPa', method = 'linear')

Averaging and Aggregation
-------------------------

Since AQUA is based on xarray, all spatial and temporal aggregation options are available by default. 
On top of that, AQUA attempts to load or compute the grid point areas of each dataset so area-weighted averages can be produced without hassle. 
When we instantiate the ``Reader`` object, grid areas for the source files are computed if not already available. 
After this, we can use them for spatial averaging using the ``fldmean`` method, obtaining time series of global (field) averages.
For example, if we run the following commands:

.. code-block:: python

    tprate = data.tprate
    global_mean = reader.fldmean(tprate)

we get a time series of the global average tprate.

It is also possible to apply a regional section to the domain before performing the averaging

.. code-block:: python

    tprate = data.tprate
    global_mean = reader.fldmean(tprate, lon_limits=[-50, 50], lat_limits=[-10,20])

.. warning ::
    Please note that in order to apply an area selection the data Xarray must include `lon` and `lat` as coordinates.
    It can work also on unstructured grids, but information on coordinate must be available. 

Input data may not be available at the desired time frequency. It is possible to perform time averaging at a given
frequency by using the ``timmean`` method. 

.. code-block:: python

    reader = Reader(model="IFS", exp="tco2559-ng5", source="ICMGG_atm2d")
    data = reader.retrieve()
    daily = reader.timmean(data, freq='daily')

Data have now been averaged at the desired daily timescale.
If you want to avoid to have incomplete average over your time period (for example, be sure that all the months are complete before doing the time mean)
it is possible to activate the ``exclude_incomplete=True`` flag which will remove averaged chunks which are not complete. 
If you want to center the time mean on the time period, you can activate the ``center_time=True`` flag.
This is at the moment only available yearly averages.

..  note ::
    The ``time_bounds`` boolean flag can be activated to build time bounds in a similar way to CMOR standard.


Fixing: Metadata correction 
---------------------------

The reader includes a "data fixer" that can edit the metadata of the input datasets, 
fixing variable or coordinate names and performing unit conversions.
The general idea is to convert data from different models to a uniform file format
with the same variable names and units. The default format is GRIB2.

The fixing is done by default when we initialize the reader, 
using the instructions in the ``config/fixes`` folder. Each model has its own YAML file that specify the fixes.
Fixes can be specified in two different ways:
- Using the ``fixer_name`` definitions, to be then provided as a metadata in the catalog. This represents fixes that have a common nickname which can be used in multiple sources when defining the catalog. There is the possibility of specifing a `parent` fix so that a fix can be re-used with minor correction, merging small change to a larger family.
- Using the source-based definition. Each experiment/source can have its own specific fix, or alternatively a ``default.yaml`` that can be used in the case of necessity. Please note that this is the older AQUA implementation and will be deprecated in favour of the new `family` approach.
A ``default.yaml`` is used for common unit corrections. 

The fixer performs a range of operations on data:

- adopt a common 'coordinate data model' (default is the CDS data model): names of coordinates and dimensions (lon, lat, etc.),
  coordinate units and direction, name (and meaning) of the time dimension. 
- Changing variable name, by using ``source`` key, deriving the correct metadata from GRIB tables. The fixer can identify these derived variables by their ShortNames and ParamID (ECMWF and WMO eccodes tables are used).
- Derive new variables executing trivial operations as multiplication, addition, etc, by using the ``derived`` key. In particular, it derives from accumulated variables like ``tp`` (in mm), the equivalent mean-rate variables
  (like ``tprate``, paramid 172228; in mm/s). 
- using the ``metpy.units`` module, it is capable of guessing some basic conversions.
  In particular, if a density is missing, it will assume that it is the density of water and will take it into account.
  If there is an extra time unit, it will assume that division by the timestep is needed. 

In the ``fixer`` folder, it is also possible to specify in a flexible way custom derived variables. For example:

.. code-block:: yaml

    mytprate:
        derived: tprate*86400
            attributes:
                units: mm day-1
                long_name: My own test precipitation in mm / day

When adding the fixes for a new source/experiment, it is possible to exploit of the ``default`` provided.
However, in some cases more fine tuning might be required.
In order to do so, since AQUA v0.4 it is possible to specify the ``method`` key in the fix, so that it allows for 
three different fixing strategies:

- ``replace``: use the source-specific fixes overriding the default ones. If you do not specify anything, this is the basic behaviour.
- ``merge``: merge the source-specific fixes with the default ones, with priority for the former.
  It can be used if the most of fixes from default are good, but something different in the specific source is required.
- ``default``: for this specific source, roll back to default fixes.
  This might be necessary if a default fix exists for a specific experiment and it should not be used in a specific source.

.. warning ::
    Recursive fixes (i.e. fixes of fixes) cannot be implemented.

Fixing: Data model and coordinate names
---------------------------------------

The fixer can adopt a common 'coordinate data model' (default is the CDS data model as described in the previous section).
If this data model is not appropriate for a specific source, it is possible to specify a different one in the catalogue.
If the data model coordinate treatment is not enough to fix the coordinates, it is possible to specify a custom fix in the catalogue.
For example, if the longitude coordinate is called ``longitude`` instead of ``lon``, it is possible to specify a fix like:

.. code-block:: yaml

    lon:
        source: longitude

This will rename the coordinate to ``lon``. The block has to be specified in the ``coords`` section of the fixer file.

Streaming simulation
--------------------
The reader includes the ability to simulate data streaming to retrieve chunks of data of a specific time length.
To activate the streaming mode the user should specify the argument `streaming=True` in the Reader initialization.
The user can also choose the length of the data chunk with the ``aggregation`` keyword (in pandas notation "D", "M", "Y", or "daily", "monthly" etc. or "days", "months" etc.).
The default is ``S`` (step), i.e. single saved timesteps are read at each iteration.
The user can also specify the desired initial and final dates with the keywords `startdate` and `enddate`.
If, for example, we want to stream the data every three days from '2020-05-01', we need to call:

.. code-block:: python

    reader = Reader(model="IFS", exp= "tco2559-ng5", source="ICMGG_atm2d", streaming=True, aggregation = '3D', startdate = '2020-05-01')    
    data = reader.retrieve()

If the ``retrieve`` method in streaming mode is called multiple times, 
it will return the data in chunks until all of the data has been streamed.
The function will automatically determine the appropriate start and end points for each chunk based on
the internal state of the streaming process.
If we want to reset the state of the streaming process, we can call the ``reset_stream`` method.

Another possibility to deal with data streaming (which we actually recommend)
is to use the argument ``stream_generator=True`` in the Reader initialization:

.. code-block:: python

    reader = Reader(model="IFS", exp= "tco2559-ng5", source="ICMGG_atm2d", stream_generator = 'True', aggregation = 'monthly')
    data_gen = reader.retrieve()
    
`data_gen` is now a generator object that yields the requested one-month-long chunks of data.
We can do operations with them by iterating on the generator object like

.. code-block:: python

    for data in data_gen:
        # Do something with the data

Please notice that when accessing FDB sources and streaming mode is desired (instead of direct xarrayay dask access), we recommend to set ``stream_generator=True``,
since the FDB interface can provide directly a generator if desired.

Parallel Processing
-------------------

Since most of the objects in AQUA are based on ``xarray``, you can use parallel processing capabilities provided by 
``xarray`` through integration with ``dask`` to speed up the execution of data processing tasks.
For example, if you are working with AQUA interactively
in a Jupyter Notebook, you can start a dask cluster to parallelize your computations.

.. code-block:: python

    from dask.distributed import Client
    import dask
    dask.config.config.get('distributed').get('dashboard').update({'link':'{JUPYTERHUB_SERVICE_PREFIX}/proxy/{port}/status'})

    client = Client(n_workers=40, threads_per_worker=1, memory_limit='5GB')
    client

The above code will start a dask cluster with 40 workers and one thread per worker.

AQUA also provides a simple way to move the computation done by dask to a compute node on your HPC system.
The description of this feature is provided in the section :ref:`slurm`.

Reading from FDB/GSV
--------------------

Dask access
~~~~~~~~~~~

If an appropriate entry has been created in the catalogue, the reader can also read data from a FDB/GSV source. 
The request is transparent to the user (no apparent difference to other data sources) in the call.

For example (on Lumi):

.. code-block:: python

    reader = Reader(model="IFS", exp="control-1950-devcon", source="hourly-1deg")
    data = reader.retrieve(var='2t')

The default is that this call returns a regular dask-enabled (lazy) xarray.Dataset, like all other data sources.
The magic behind this is performed by an intake driver for FDB which has been specifically developed from scratch in AQUA.
Please notice that in the case of FDB access specifying the variable is compulsory, but a list can be provided. 
If not specified, the default variable defined in the catalogue is used.

.. warning::
    In general we recommend to use the default xarray (dask) dataset output, since this also supports ``dask.distributed`` multiprocessing.
    For example you could create a ``LocalCluster`` and its client with:

.. code-block:: python

    import dask
    from dask.distributed import LocalCluster, Client
    cluster = LocalCluster(threads_per_worker=1, n_workers=8)
    client = Client(cluster)

This will enormously accelerate any computation on the xarray.

An optional keyword, which in general we do **not** recommend to specify for dask access, is ``aggregation``,
which specifies the chunk size for dask access.
Values could be "D", "M", "Y" etc. (in pandas notation) to specify daily, monthly and yearly aggregation.
It is best to use the default, which is already specified in the catalogue for each data source.
This default is based on the memory footprint of single grib message, so for example for IFS/NEMO dative data
we use "D" for Tco2559 (native) and "1deg" streams, "Y" for monthly 2D data and "M" for 3D monthly data.
In any case, if you use multiprocessing and run into memory troubles for your workers, you may wish to decrease
the aggregation (i.e. chunk size).

Iterator access
~~~~~~~~~~~~~~~

In alternative it is also possible to ask the reader to return an *iterator/generator* object passing the ``stream_generator=True`` 
keyword to the ``retrieve()`` method.
In that case the next block of data can be read from the iterator with ``next()`` as follows:

.. code-block:: python

    reader = Reader(model="IFS", exp="fdb-tco399", source="fdb-long", aggregation="D",
                    regrid="r025")
    data = reader.retrieve(startdate='20200120', enddate='20200413', var='ci',
                           stream_generator=True)
    dd = next(data)

or with a loop iterating over ``data``. The result of these operations is in turn a regular xarray.Dataset containg the data.
Since this is a data stream the user should also specify the desired initial time and the final time (the latter can be omitted and will default to the end of the dataset).
When using an iterator it is possible to specify the size of the data blocks read at each iteration with the ``aggregation`` keyword
(``M`` is month, ``D``is day etc.). 
The default is ``S`` (step), i.e. single saved timesteps are read at each iteration.

Please notice that the resulting object obtained at each iteration is not a lazy dask array, but is instead entirely loaded into memory.
Please consider memory usage in choosing an appropriate value for the ``aggregation`` keyword.

Level selection and regridding
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When reading 3D data it is possible to specify already during `retrieve()` which levels to select using the `level` keyword.
The levels are specified in the same units as they are stored in the archive (for example in hPa for atmospheric IFS data, but an index for NEMO data in the FDB archive).

In the case of FDB data this presents the great advantage that a significantly reduced request will be read from the FDB 
(by default all levels would be read for each timestep even if later a `sel()` or `isel()` selection is performed on the XArray).
For non-FDB sources the reader simply uses the `sel()` method to select the desired levels.

When reading 3D data the reader also adds an additional coordinates with prefix `idx_`
and suffix the names of vertical dimensions to the Dataset. These represent the indices 
of the (possibly selected) levels in the original archive.
This hidden index helps the regridder to choose the appropriate weights for each level even if a level
selection has been performed.

This means that when regridding 3D data the regridding can be performed first on a full dataset and then
levels are selected or vice versa. In both cases the regridding will be performed using the correct weights.
By default in xarray when a single vertical level is selected the vertical dimension is dropped, but
the regridder is still able to deal with this situation using the information in the hidden index.

Please avoid performing regridding on datasets in which single levels have been selected for multiple
3D variables using different vertical dimensions or on datasets containing also 2D data,
because in such cases it may not be possible to reconstruct which vertical dimension
each variable was supposed to be using. 
In these cases it is better to first select a variable, then select levels and finally regrid. 
The regridder will issue a warning if it detects such a situation.
An alternative is to maintain the vertical dimension when selecting a single level by specifying a list with one element,
for example using `isel(nz1=[40])` instead of `isel(nz1=40)`.
If level selection was performed at the `retrieve()` stage this is not a problem,
since in that case the vertical level information is preserved by producing 3D variables
with a single vertical level.

Accessor
~~~~~~~~

AQUA also provides a special 'aqua' accessor to Xarray which allows to call most functions and methods of the reader
class as if they were methods of an Xarray DataArray or Dataset.
Reader methods like `reader.regrid()` or functions like `plot_single_map()` can now also be accessed by appending
the suffix `aqua`to a DataArray or Dataset, followed by the function of interest, like in `data.aqua.regrid()`

This means that instead of writing:

.. code-block:: python

    reader.fldmean(reader.timmean(data.tcc, freq="Y"))

we can write

.. code-block:: python

    data.tcc.aqua.timmean(freq="Y").aqua.fldmean()

Please notice that the accessor always assumes that the Reader instance to be used is either
the one with which a Dataset was created or, for new derived objects and for *DataArrays in the Datasets*,
the last instantiated Reader or the last use of the `retrieve()` method.
This means that if more than one reader instance is used (for example to compare different datasets)
we recommend not to use the accessor.

As an alternative the Reader class contains a special `set_default()` method which sets that reader
as an accessor default in the following. The accessor itself also has a `set_default()` method
(accepting a reader instance as an argument) which sets the default and returns the same object.
Usage examples when multiple readers are used:

.. code-block:: python

    from aqua import Reader
    reader1=Reader(model="IFS", exp="test-tco79", source="short", regrid="r100")  # the default is now reader1
    reader2=Reader(model="IFS", exp="test-tco79", source="short", regrid="r200")  # the default is now reader2
    data1 = reader1.retrieve()  # the default is now reader1 
    data2 = reader2.retrieve()  # the default is now reader2
    reader1.set_default()  # the default is now reader1 
    data1r = data1.aqua.regrid()
    data2r = data2.aqua.regrid()  # data2 was created by retrieve(), so it remembers its default reader
    data2r = data2['2t'].aqua.set_default(reader2).aqua.regrid()  # the default is set to reader2 before using a method


