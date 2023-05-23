Core Components
===============

The `Reader` Class
------------------
The `Reader` class provides AQUA access to data, developed to offer a centralized common data access point.
AQUA `Reader` can, in fact, access different file formats and data from the FDB or intake catalogues, 
and delivers xarray objects.
On top of data access, the `Reader` is also able to perform multiple operations on the data: interpolation and regridding,
spatial and temporal averaging and metadata correction. 
Since streaming is not yet available, a streaming emulator has been introduced and can mimic streaming.


Data Reading and Preprocessing
------------------------------

Supported File Formats
~~~~~~~~~~~~~~~~~~~~~~

AQUA supports a variety of climate data file formats, including:

- NetCDF
- GRIB (also through FDB)
- Zarr

Data Structures
~~~~~~~~~~~~~~~~

AQUA uses xarray data structures to manage and manipulate climate data. The primary data structures used in AQUA are:

- DataArray: Represents a single variable with multiple dimensions (e.g., time, latitude, longitude).
- Dataset: Represents a collection of DataArrays with shared dimensions.

Basic Usage
~~~~~~~~~~~~~~~~
Accessing data with the AQUA `Reader` is very straightforward.
To check what is available in the catalogue, we can use the `inspect_catalogue` function.
Three hierarchical layer structures describe each dataset. At the top level, there are "models" (e.g., ICON, IFS, etc.). 
Each model has different "experiments," and each "experiment" can have different "sources".

Calling, for example, 

.. code-block:: python

    from aqua import catalogue, inspect_catalogue
    cat = catalogue()
    inspect_catalogue(cat, model = 'CERES')

will return experiments available in the catalogue for model CERES.

Let's try to load some ICON data with the AQUA "Reader".
We first instantiate a "Reader" object specifying the type of data we want to read from the catalogue.
As mentioned, the `Reader` access is built on a 3-level hierarchy of model, experiment, and source.  

.. code-block:: python

    reader = Reader(model="ICON", exp="ngc2009", source="atm_2d_ml_R02B09")

Now we can read the actual data with the "retrieve" method.

.. code-block:: python

    data = reader.retrieve(fix=False)

The reader returns an xarray.Dataset with raw ICON data on the original grid.


Interpolation and Regridding
----------------------------
AQUA provides functions to interpolate and regrid data to match the spatial resolution of different datasets. 
AQUA regridding functionalities are based on the external tool `smmregrid <https://github.com/jhardenberg/smmregrid>`_ which 
operates sparse matrix computation based on externally-computed weights. 


The idea of the regridder is first to generate the weights for the interpolation and then to use them for each regridding operation. 
The reader generates the regridding weights automatically (with CDO) if not already existent and stored
in a directory specified in the `config/regrid.yaml` file. The same file also contains a list of predefined target grids
(only regular lon-lat for now). For example, "r100" is a regular grid at 1° resolution.

In other words, weights are computed externally by CDO (an operation that needs to be done only once) and 
then stored on the machine so that further operations are considerably fast. 
Such an approach has two main advantages:
1. All operations are done in memory so that no I/O is required, and the operations are faster than with CDO
2. Operations can be easily parallelized with Dask, bringing further speedup. 

In the long term, it will be possible to support also other interpolation software,
such as `ESMF <https://earthsystemmodeling.org/>`_ or `MIR <https://github.com/ecmwf/mir>`_. 
Let's see a practical example. We instantiate a reader for ICON data specifying that we will want to interpolate to a 1° grid. 
As mentioned, if the weights file does not exist in our collection, it will be created automatically.

.. code-block:: python

    reader = Reader(model="ICON", exp="ngc2009", source="atm_2d_ml_R02B09", regrid="r100")
    data = reader.retrieve()

Notice that these data still need to be regridded. You could ask to regrid them directly by specifying the argument `regrid=True`. 
Please be warned that this will take longer without a selection.
It is usually more efficient to load the data, select it, and regrid it.

Now we regrid part of the data (the temperature of the first 100 frames):

.. code-block:: python

    tasr = reader.regrid(data.tas[0:100,:])

The result is an xarray containing 360x180 grid points for each timeframe.

Averaging and Aggregation
-------------------------

Since AQUA is based on xarray, all spatial and temporal aggregation options are available by default. 
On top of that, AQUA attempts to load or compute the grid point areas of each dataset so area-weighted averages can be produced without hassle. 
When we instantiate the `Reader` object, grid areas for the source files are computed if not already available. 
After this, we can use them for spatial averaging using the `fldmean` method, obtaining time series of global (field) averages.
For example, if we run the following commands:

.. code-block:: python

    tprate = data.tprate
    global_mean = reader.fldmean(tprate)

we get a time series of the global average tprate.

Input data may not be available at the desired time frequency. It is possible to perform time averaging at a given
frequency by specifying a frequency in the reader definition and then using the `timmean` method. 

.. code-block:: python

    reader = Reader(model="IFS", exp="tco2559-ng5", source="ICMGG_atm2d", freq='daily')
    data = reader.retrieve()
    daily = reader.timmean(data)

Data have now been averaged at the desired daily timescale.


Fixing: Metadata correction 
---------------------------
The reader includes a "data fixer" that can edit the metadata of the input datasets, 
fixing variable or coordinate names and performing unit conversions.
The general idea is to convert data from different models to a uniform file format
with the same variable names and units. The default format is GRIB2.

The fixing is done by default (``apply_unit_fix=False`` to switch it off) when we retrieve the data, 
using the instructions in the 'config/fixes.yaml' file.

The fixer performs a range of operations on data:

- adopt a common 'coordinate data model' (default is the CDS data model): names of coordinates and dimensions (lon, lat, etc.), coordinate units and direction, name (and meaning) of the time dimension. 
- derive new variables. In particular, it derives from accumulated variables like "tp" (in mm), the equivalent mean-rate variables (like "tprate", paramid 172228; in mm/s). The fixer can identify these derived variables by their Short Names (ECMWF and WMO eccodes tables are used).
- using the metpy.units module, it is capable of guessing some basic conversions. In particular, if a density is missing, it will assume that it is the density of water and will take it into account. If there is an extra time unit, it will assume that division by the timestep is needed.


In the `fixer.yaml` file, it is also possible to specify in a flexible way custom derived variables. For example:

.. code-block:: markdown

    mytprate:
        derived: tprate*86400
            attributes:
                units: mm day-1
                long_name: My own test precipitation in mm / day


Streaming simulation
--------------------
The reader includes the ability to simulate data streaming to retrieve chunks of data of a specific time length.
The user can specify the length of the chunk, the data units (days, weeks, months, years), and the starting date.
If, for example, we want to stream the data every three days from '2020-05-01', we need to call:

.. code-block:: python

    reader = Reader(model="IFS", exp="tco2559-ng5", source="ICMGG_atm2d")
    data = reader.retrieve(streaming=True, stream_step=3, stream_unit='days', stream_startdate = '2020-05-01')

If the unit parameter is not specified, the data is streamed, keeping the original time resolution of input data. 
If the starting date parameter is not specified, the data stream will start from the first date of the input file.

If the `retrieve` method in streaming mode is called multiple times with the same parameters, 
it will return the data in chunks until all of the data has been streamed. The function will automatically determine the
appropriate start and end points for each chunk based on the internal state of the streaming process.
If we want to reset the state of the streaming process, we can call the `reset_stream` method.

Another possibility to deal with data streaming is to call the `stream_generator` method of the class `Reader`. 
This can be done from the retrieve method through the argument ``streaming_generator = True``:

.. code-block:: python

    data_gen = reader.retrieve(streaming_generator=True, stream_step=3, stream_unit = 'months')

`data_gen` is now a generator object that yields the requested three month-chunks of data. 
We can do operations with them by iterating on the generator object.

Parallel Processing
-------------------

Since most of the objects in AQUA are based on `xarray`, you can use parallel processing capabilities provided by 
`xarray` through integration with `dask` to speed up the execution of data processing tasks.
For example, if you are working with AQUA interactively
in a Jupyter Notebook, you can start a dask cluster to parallelize your computations.

.. code-block:: python

    from dask.distributed import Client
    import dask
    dask.config.config.get('distributed').get('dashboard').update({'link':'{JUPYTERHUB_SERVICE_PREFIX}/proxy/{port}/status'})

    client = Client(n_workers=40, threads_per_worker=1, memory_limit='5GB')
    client

The above code will start a dask cluster with 40 workers and one thread per worker.


