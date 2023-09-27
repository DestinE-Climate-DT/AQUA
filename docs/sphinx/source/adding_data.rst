Adding data to AQUA
===================

To add your data to AQUA, you have to provide an intake catalogue that describes your data, 
and in particular, the location of the data. This can be done in two different way, by adding a 
standard entry in the form of files or using the specific FDB interface. 
To exploit of the regridding functionalities, you will also need to configure the regrid.yaml. 

The 3-level hierarchy on which AQUA is based, i.e. model - exp - source, must be respected so that 
specific files must be created within the catalog of a specific machine. 

.. contents::
   :local:
   :depth: 1

Files-based sources
^^^^^^^^^^^^^^^^^^^

Adding files-based sources in AQUA is done with default interface by intake. 
Files supported can include NetCDF files or other formats as GRIB or Zarr.

The best way to explain the process is to follow the example of adding some fake dataset.

Let's imagine we have a dataset called `yearly_SST` that consists of the following:

- 2 netCDF files, each file contains one year of data (`/data/path/1990.nc` and `/data/path/1991.nc`)
- data are stored in 2D arrays, with dimensions `lat` and `lon`
- coordinate variables are `lat` and `lon`, and the time variable is `time`, all one dimensional
- data located on the Levante machine

We will create a catalogue that will describe this dataset, and then we will add it to AQUA.
The catalog name will be `yearly_SST`.

The first step is to add this catalogue to the `config/machines/levante/catalog.yaml` file. 
The additional entry in this file will look like this:

.. code-block:: yaml

    yearly_SST:
        description: amazing yearly_SST dataset
        driver: yaml_file_cat
        args:
          path: "{{CATALOG_DIR}}/yearly_SST/main.yaml"

This will create the `model` entry within the catalog. Then we will need to create the `exp` entry, which will be included in the `main.yaml`
In our case, the `main.yaml` file will look like this (but many other experiments can be added aside of this):

.. code-block:: yaml

    sources:
      yearly_sst:
        description: amazing yearly_SST dataset
        driver: yaml_file_cat
        args:
          path: "{{CATALOG_DIR}}/yearly_SST.yaml"

We finally need to define the specific source, using the `yearly_SST.yaml` file and saving it in the `config/machines/levante/catalog/yearly_SST` directory (that we should create first if missing).
The most straightforward intake catalogue describing our dataset will look like this: 

.. code-block:: yaml

    plugins:
    source:
        - module: intake_xarray

    sources:
      annual:
        description: my amazing yearly_SST dataset    
        driver: netcdf
        args:
            chunks:
                time: 1
            urlpath:
            - /data/path/1990.nc
            - /data/path/1991.nc

Now we can access our dataset from AQUA with the following command:

.. code-block:: python

    from aqua import Reader
    reader = Reader(model="yearly_SST", exp="yearly_sst", source="annual")
    data = reader.retrieve()


You can add fixes to your dataset by following examples in the `config/fixes/` directory.


FDB-based source
^^^^^^^^^^^^^^^^

FDB based sources are built on a specific interface built by AQUA. While the procedure of adding the catalog tree entries is the same, 
the main difference is on how the specific source is descrived. We report here an example and we later describe the different element.

.. code-block:: yaml

    sources:
        hourly-native:
            args:
                request:
                    domain: g
                    class: rd
                    expver: a06x
                    type: fc
                    stream: lwda
                    date: 19500101
                    time: '0000'
                    param: 2t
                    levtype: sfc
                    step: 0
                data_start_date: 19500101T0000
                data_end_date: 19591231T2300
                aggregation: D  # Default aggregation / chunk size
                savefreq: H  # at what frequency are data saved
                timestep: H  # base timestep for step timestyle
                timestyle: step  # variable date or variable step
            description: hourly data on native grid TCo1279 (about 10km). Contains tprate(260048),
            2t(167), 10u(165), 10v(166), 100u(228246), 100v(228247), sr(173), blh(159),
            2d(168), skt(235), chnk(148). See fix yaml for derived vars.
            driver: gsv
            metadata: 
                fdb_path: /pfs/lustrep3/scratch/project_465000454/pool/data/EXPERIMENTS/fdb-config-CONTROL_1950_DEVCON.yaml
                eccodes_path: /projappl/project_465000454/jvonhar/aqua/eccodes/eccodes-2.30.0/definitions
                variables: ['tprate', '2t', '10u', '10v', '100u', '100v', 'sr', 'blh', '2d', 'skt', 'chnk']

This is a source entry from the FDB of one of the AQUA control simulation from the IFS model. 
The source name is ``hourly-native``, because is suggesting that the catalog is made hourly data at the native model resolution.
It describes 

request
-------

The ``request`` entry in the intake catalogue primarily serves as a template for making data requests, following the standard MARS-style syntax used by the GSV retriever. 

The ``date`` parameter will be automatically overwritten by the appropriate data_start_date. For the ``step`` parameter, when using ``timestyle: step``, setting it to a value other than 0 signals that the initial steps are missing. 

This is particularly useful for data sets with irregular step intervals, such as 6-hourly output.

This documentation provides an overview of the key parameters used in the catalogue, helping users better understand how to configure their data requests effectively.

data_start_date
---------------

This defines the starting date of the experiment. It is mandatory to be set up because the FDB data is usually stored with steps not with dates and will be used internally for calculation

data_end_date
-------------

As above, it tells AQUA when to stop reading from the FDB

aggregation
-----------

The aggregation parameter is essential, whether you are using Dask or a generator. It determines the size of the chunk loaded in memory at each iteration. 

When using a generator, it corresponds to the chunk size loaded into memory during each iteration. For Dask, it signifies the size of each chunk used by Dask's parallel processing.

The choice of aggregation value is crucial as it strikes a balance between memory consumption and distributing enough work to each worker when Dask is utilized with multiple cores. 
In most cases, the default values in the catalog have been thoughtfully chosen through experimentation.

For instance, an aggregation value of ``D`` (for daily) works well for hourly-native data because it occupies approximately 1.2GB in memory. Increasing it beyond this limit may lead to memory issues. 

It is possible to choose a smaller aggregation value, but keep in mind that each worker has its own overhead, and it is usually more efficient to retrieve as much data as possible from the FDB for each worker.
There is also a consideration to rename this parameter to "chunksize."

timestep
--------

The timestep parameter, denoted as ``H``, represents the original frequency of the model's output. 

When timestep is set to ``H``, requesting data at step=6 and step=7 from the FDB will result in a time difference of 1 hour (1H).

This parameter exists because even when dealing with monthly data, it is still stored at steps like 744, 1416, 2160, etc., which correspond to the number of hours since 00:00 on January 1st.

savefreq
--------

Savefreq, indicated as ``M`` for monthly or ``H`` for hourly, signifies the actual frequency at which data are available in this stream. 

Combining this information with the timestep parameter allows us to anticipate data availability at specific steps, such as 744 and 1416 for monthly data.

timestyle
---------

The timestyle parameter can be set to either ``step`` or ``date``. It determines how data is written in the FDB. 

The recent examples have used ``step``, which involves specifying a fixed date (e.g., 19500101) and time (e.g., 0000) in the request. Time is then identified by the step in the request.

Alternatively, when timestyle is set to ``date``, you can directly specify both date and time in the request, and ``ste`` is always set to 0.

timeshift
---------

Timeshift is a boolean parameter used exclusively for shifting the date of monthly data back by one month. Without this shift, data for January would have a date like 19500201T0000. 

Implementing this correctly in a general case can be quite complex, so it was decided to implement only the monthly shift.

metadata
--------

this includes supplementary very useful information to define the catalog

- ``fdb_path``: the path of the FDB configuration file (mandatory)
- ``eccodes_path``: the path of the eccodes version used for the encoding/decoding of the FDB
- ``variables``: a list of variables available in the fdb.



Regridding
^^^^^^^^^^

In order to make use of the AQUA regridding capabilities we will need to define the way the grid are defined. 
AQUA is shipped with multiple grids definition, which are defined in the `config/aqua-grids.yaml` file. 

A machine-dependent file is found in ``config/machines/levante/regrid.yaml``, and will instruct the regridder how to map the sources and the grids.

In our case, we might imagine to have something as 

.. code-block:: yaml

    sources:
        yearly_SST:
            yearly_sst:
                default: lon-lat
                
        IFS:
            control-1950-devcon:
                hourly-native: tco1279









