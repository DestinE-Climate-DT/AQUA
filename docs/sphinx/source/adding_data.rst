Adding data to AQUA
===================

To add your data to AQUA, you have to provide an ``intake`` catalogue that describes your data, and in particular, the location of the data. 
This can be done in two different way, by adding a standard entry in the form of files or using the specific AQUA FDB interface. 
Finally, to exploit of the regridding functionalities, you will also need to configure the machine-dependent ``regrid.yaml``. 

The 3-level hierarchy on which AQUA is based, i.e. model - exp - source, must be respected so that 
specific files must be created within the catalog of a specific machine. How to create a new source and add new data is documented in the next sections. 

.. contents::
   :local:
   :depth: 1

Files-based sources
^^^^^^^^^^^^^^^^^^^

Adding files-based sources in AQUA is done with default interface by ``intake``. 
Files supported can include NetCDF files - as the one described in the example below - or other formats as GRIB or Zarr. 
The best way to explain the process is to follow the example of adding some fake dataset.

Let's imagine we have a dataset called ``yearly_SST`` that consists of the following:

- 2 netCDF files, each file contains one year of data (``/data/path/1990.nc`` and ``/data/path/1991.nc``)
- data are stored in 2D arrays, with dimensions ``lat`` and ``lon``
- coordinate variables are ``lat`` and ``lon``, and the time variable is ``time``, all one dimensional
- data located on the Levante machine

We will create a catalogue entry that will describe this dataset. The catalog name will be ``yearly_SST``.

The additional entry in this file will look like this:

.. code-block:: yaml

    yearly_SST:
        description: amazing yearly_SST dataset
        driver: yaml_file_cat
        args:
          path: "{{CATALOG_DIR}}/yearly_SST/main.yaml"

The first step is to add this catalogue to the ``config/machines/levante/catalog.yaml`` file.  
This will create the ``model`` entry within the catalog that can be used later by the ``Reader()``.

Then we will need to create the ``exp`` entry, which will be included in the ``main.yaml``.
In our case, the ``main.yaml`` file will look like this (but many other experiments - corresponding to the same model - can be added aside of this):

.. code-block:: yaml

    sources:
      yearly_sst:
        description: amazing yearly_SST dataset
        driver: yaml_file_cat
        args:
          path: "{{CATALOG_DIR}}/yearly_SST.yaml"

We finally need to define the specific experiment file that we linked in the ``main.yaml``, using the ``yearly_SST.yaml`` file and saving it in the ``config/machines/levante/catalog/yearly_SST`` directory (that we should create first if missing).
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

Where we have specified the ``source`` name of the catalog entry. As for the ``exp`` case, we could have multiple sources for the same experiment. 
Once this is defined, we can access our dataset from AQUA with the following command:

.. code-block:: python

    from aqua import Reader
    reader = Reader(model="yearly_SST", exp="yearly_sst", source="annual")
    data = reader.retrieve()

In the case is needed, you can add fixes to your dataset by following examples in the ``config/fixes/`` directory.

FDB-based source
^^^^^^^^^^^^^^^^

FDB based sources are built on a specific interface built by AQUA.
While the procedure of adding the catalog tree entries is the same, the main difference is on how the specific source is descrived.
We report here an example and we later describe the different element.

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
Some of the parameters are here described:

.. option:: request

    The ``request`` entry in the intake catalogue primarily serves as a template for making data requests, following the standard MARS-style syntax used by the GSV retriever. 

    The ``date`` parameter will be automatically overwritten by the appropriate ``data_start_date``.
    For the ``step`` parameter, when using ``timestyle: step``, setting it to a value other than 0 signals that the initial steps are missing. 

    This is particularly useful for data sets with irregular step intervals, such as 6-hourly output.

    This documentation provides an overview of the key parameters used in the catalogue, helping users better understand how to configure their data requests effectively.

.. option:: data_start_date

    This defines the starting date of the experiment.
    It is mandatory to be set up because the FDB data is usually stored with steps not with dates and will be used internally for calculation.

.. option:: data_end_date

    As above, it tells AQUA when to stop reading from the FDB.

.. option:: aggregation

    The aggregation parameter is essential, whether you are using Dask or a generator.
    It determines the size of the chunk loaded in memory at each iteration. 

    When using a generator, it corresponds to the chunk size loaded into memory during each iteration.
    For Dask, it signifies the size of each chunk used by Dask's parallel processing.

    The choice of aggregation value is crucial as it strikes a balance between memory consumption and distributing enough work to each worker when Dask is utilized with multiple cores. 
    In most cases, the default values in the catalog have been thoughtfully chosen through experimentation.

    For instance, an aggregation value of ``D`` (for daily) works well for hourly-native data because it occupies approximately 1.2GB in memory.
    Increasing it beyond this limit may lead to memory issues. 

    It is possible to choose a smaller aggregation value, but keep in mind that each worker has its own overhead, and it is usually more efficient to retrieve as much data as possible from the FDB for each worker.
    There is also a consideration to rename this parameter to "chunksize."

.. option:: timestep

    The timestep parameter, denoted as ``H``, represents the original frequency of the model's output. 

    When timestep is set to ``H``, requesting data at ``step=6`` and ``step=7`` from the FDB will result in a time difference of 1 hour (``1H``).

    This parameter exists because even when dealing with monthly data, it is still stored at steps like 744, 1416, 2160, etc., which correspond to the number of hours since 00:00 on January 1st.

.. option:: savefreq

    Savefreq, indicated as ``M`` for monthly or ``H`` for hourly, signifies the actual frequency at which data are available in this stream. 

    Combining this information with the timestep parameter allows us to anticipate data availability at specific steps, such as 744 and 1416 for monthly data.

.. option:: timestyle

    The timestyle parameter can be set to either ``step`` or ``date``. It determines how data is written in the FDB. 

    The recent examples have used ``step``, which involves specifying a fixed date (e.g., 19500101) and time (e.g., 0000) in the request.
    Time is then identified by the step in the request.

    Alternatively, when timestyle is set to ``date``, you can directly specify both date and time in the request, and ``ste`` is always set to 0.

.. option:: timeshift

    Timeshift is a boolean parameter used exclusively for shifting the date of monthly data back by one month.
    Without this shift, data for January would have a date like 19500201T0000. 

    Implementing this correctly in a general case can be quite complex, so it was decided to implement only the monthly shift.

.. option:: metadata

    this includes supplementary very useful information to define the catalog

    - ``fdb_path``: the path of the FDB configuration file (mandatory)
    - ``eccodes_path``: the path of the eccodes version used for the encoding/decoding of the FDB
    - ``variables``: a list of variables available in the fdb.
    - ``source_grid_name``: the grid name defined in aqua-grids.yaml to be used for areas and regridding
    - ``fix_family``: the fix family definition defined in the fixes folder


Regridding capabilities
^^^^^^^^^^^^^^^^^^^^^^^

In order to make use of the AQUA regridding capabilities we will need to define the way the grid are defined for each source. 
AQUA is shipped with multiple grids definition, which are defined in the ``config/aqua-grids.yaml`` file.
In the following paragraphs we will describe how to define a new grid if needed.
Once the grid is defined, you can come back to this section to understand how to use it for your source.

Let's imagine that for our ``yearly_SST`` source we want to use the ``lon-lat`` grid, which is defined in the ``config/aqua-grids.yaml`` file
and consists on a regular lon-lat grid.

Since AQUA v0.5 the informations about which grid to use for each source are defined in the metadata of the source itself.
In our case, we will need to add the following metadata to the ``yearly_SST.yaml`` file as ``source_grid_name``.

.. code-block:: yaml

     yearly_SST:
        description: amazing yearly_SST dataset
        driver: yaml_file_cat
        args:
          path: "{{CATALOG_DIR}}/yearly_SST/main.yaml"
        metadata:
            source_grid_name: lon-lat


Grid definitions
^^^^^^^^^^^^^^^^

As mentioned above, AQUA has some predefined grids available in ``config/aqua-grids.yaml``: here below we provide some information on the grid key so that it might me possibile define new grids.
As an example, we use the healpix grid for ICON and tco1279 for IFS:

.. code-block:: yaml

    icon-healpix:
        path:
            2d: $grids/HealPix/icon_hpx{zoom}_atm_2d.nc   # this is the default 2d grid
            2dm: $grids/HealPix/icon_hpx{zoom}_oce_2d.nc  # this is an additional and optional 2d grid used if data are masked
            depth_full: $grids/HealPix/icon_hpx{zoom}_oce_depth_full.nc
            depth_half: $grids/HealPix/icon_hpx{zoom}_oce_depth_half.nc
        masked:   # This is the attribute used to distinguish variables which should go into the masked category
            component: ocean
        space_coord: ["cell"]
        vert_coord: ["depth_half", "depth_full"]


    tco1279:
        path: 
            2d: $grids/IFS/tco1279_grid.nc
            2dm: $grids/IFS/tco1279_grid_masked.nc
        masked_vars: ["ci", "sst"]
        vert_coord: ["2d", "2dm"]


- **path**: Path to the grid data file, can be a single file if the grid is 2d, but can include multiple files as a function of the grid used. ``2d`` refers to the default grids, ``2dm`` to the grid for masked variables, any other key refers to specific 3d vertical structure (see `vert_coord`)

- **space_coord**: The space coordinate how coordinates are defined and used for interpolation. Since AQUA v0.4 there is an automatic guessing routine, but this is a bit costly so it is better to specify this if possible.

- **masked** (if applicable): Keys to define variables which are masked. When using this, the code will search for an attribute to make the distinction (``component: ocean`` in this case). In alternative, if you want to apply masking only on a group of variables, you can defined ``vars: [var1, var2]``. In all the cases, the `2dm` grid will be applied to the data.

- **vert_coords** (if applicable): Vertical coordinate options for the grid. Specific for oceanic models where interpolation is changing at each depth level.

- **extra** (if applicable): Additional CDO command-line options to be used to process the files defined in `path`.

- **cellareas**, **cellarea_var** (if applicable): Optional path and variable name where to specify a file to retrieve the grid area cells when the grid shape is too complex for being automatically computed by CDO.

- **regrid_method** (if applicable): Alternative CDO regridding method which is not the `ycon` default. To be used when grid corners are not available. Alterntives might be `bil`, `bic` or `nn`.

Other simpler grids can be defined using the CDO syntax, so for example we have ``r100: r360x180``. Further CDO compatible grids can be of course defined in this way. 

A standard `lon-lat` grid is defined for basic interpolation and can be used for most of the regular cases, as long as the ``space_coord`` are ``lon`` and ``lat``.

DE_340 source syntax convention
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Although free combination of model-exp-source can be defined in each catalog to get access to the data, inside DE_340 a series of decision has been 
taken to try to homogenize the definition of experiments and of sources. We decide to use the dash (`-`) to connect the different elements of the syntax below

Models (`model` key)
--------------------

This will be simply one of the four models used in the project: IFS, NEMO, FESOM and ICON. 
We will not merge atmospheric and oceanic models which have not the same grid (so only ICON will be represented as a single model)

Experiments (`exp` key)
-----------------------

Considering that we have strict set of experiments that must be produced, we will follow this 4-string convention:

1. **Experiment kind**: historical, control, sspXXX
2. **Starting year**: 1950, 1990, etc...
3. **Oceanic model**: nemo, fesom, icon (this is required since IFS is run with both configurations)
4. **Extra info** (optional): any information that might be important to define an experiment, as dev, test, the expid of the simulation, or anything else that can help for defining the experiment.

Examples are `historical-1990-fesom-dev` or `control-1950-nemo-dev`. We plan to incorporate info on the expid in the metadata, so that we can potentially use it as an alias.

Sources (`source` key)
----------------------

For the sources, we will need to uniform the different requirements of grids and temporal resolution. Sometimes we use native sometimes original, sometimes r100 sometimes 1deg. Do we want to use the 2d/3d key every time? This is confusing. Some options might be...

1. **Time resolution**: monthly, daily, 6hourly, hourly, etc.
2. **Space resolution**: native, 1deg, 025deg, r100, etc... For some oceanic model we could add the horizontal grid so native-elem or native-gridT could be an option). Similarly, healpix can be healpix-0 or healpix-6 in the case we want to specify the zoom level. 
3. **Extra info**: 2d or 3d. Not mandatory, but to be used when confusion might arise.



