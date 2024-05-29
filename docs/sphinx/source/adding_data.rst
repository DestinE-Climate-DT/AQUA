.. _add-data:

Adding new data
===============

To add new data the 3-level hierarchy on which AQUA is based, i.e. **model** - **exp** - **source**, must be respected so that 
specific files must be created within the catalog of a specific machine.
How to create a new source and add new data is documented in the next sections.

- To add your data to AQUA, you have to provide an ``intake`` catalogue that describes your data,
  and in particular, the location of the data. 
  This can be done in two different way, by adding a standard entry in the form of files (:ref:`file-based-sources`)
  or by adding a source from the FDB (:ref:`FDB-based-sources`) with the specific AQUA FDB interface.
- A set of pre-existing fixes can be applied to the data, or you can modify or create your own fixes (see :ref:`fixer`).
- Finally, to exploit the regridding functionalities, you will also need to verify the grid is available in the ``config/grids`` folder 
  or to add it (see :ref:`grid_definition`).

.. note::
    A method to add new catalogues to the configuration folder has been developed.
    You can find more information in the :ref:`aqua-add` section.

.. _file-based-sources:

File-based sources
------------------

Adding file-based sources in AQUA is done with default interface by ``intake``. 
Files supported can include NetCDF files (as the one described in the example below) or other formats as GRIB or Zarr. 
The best way to explain the process is to follow the example of adding some fake dataset.

Let's imagine we have a dataset called ``yearly_SST`` that consists of the following:

- 2 netCDF files, each file contains one year of data (``/data/path/1990.nc`` and ``/data/path/1991.nc``)
- data are stored in 2D arrays, with dimensions ``lat`` and ``lon``
- coordinate variables are ``lat`` and ``lon``, and the time variable is ``time``, all one dimensional
- data located on the LUMI machine

We will create a catalogue entry that will describe this dataset.
The catalog name will be ``yearly_SST``.

The additional entry in this file will look like this:

.. code-block:: yaml

    yearly_SST:
        description: amazing yearly_SST dataset
        driver: yaml_file_cat
        args:
          path: "{{CATALOG_DIR}}/yearly_SST/main.yaml"

The first step is to add this catalogue to the ``config/machines/lumi/catalog.yaml`` file.  
This will create the ``model`` entry within the catalog that can be used later by the ``Reader()``.

.. note::
    Sources are built using intake, which means that they can exploit of the built-in Jinja2 template replacamente as done in the example above with `{{CATALOG_DIR}}`

Then we will need to create the ``exp`` entry, which will be included in the ``main.yaml``.
In our case, the ``main.yaml`` file will look like this (but many other experiments,
corresponding to the same model, can be added aside of this):

.. code-block:: yaml

    sources:
      yearly_sst:
        description: amazing yearly_SST dataset
        driver: yaml_file_cat
        args:
          path: "{{CATALOG_DIR}}/yearly_SST.yaml"

We finally need to define the specific experiment file that we linked in the ``main.yaml``,
using the ``yearly_SST.yaml`` file and saving it in the ``config/machines/lumi/catalog/yearly_SST`` directory
(that we should create first if missing).

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
        metadata:
            source_grid_name: lon-lat
            fixer_name: amazing_fixer

Where we have specified the ``source`` name of the catalog entry.
As for the ``exp`` case, we could have multiple sources for the same experiment.

Once this is defined, we can access our dataset from AQUA with the following command:

.. code-block:: python

    from aqua import Reader
    reader = Reader(model="yearly_SST", exp="yearly_sst", source="annual")
    data = reader.retrieve()

Finally, the ``metadata`` entry contains optional additional information useful to define how to postprocess the data:

    - ``source_grid_name``: the grid name defined in ``aqua-grids.yaml`` to be used for areas and regridding
    - ``fixer_name``: the name of the fixer defined in the fixes folder

You can add fixes to your dataset by following examples in the ``config/fixes/`` directory (see :ref:`fixer`).

.. note::

    If you want to add a Zarr or GRIB source the syntax may be slightly different,
    but the general structure of the catalogue will be the same.
    You can find examples in the existing catalogue or more information on the 
    `intake <https://intake.readthedocs.io/en/stable/>`_ and
    `intake-xarray <https://intake-xarray.readthedocs.io/en/latest/>`_ documentation.

.. _FDB-based-sources:

FDB-based sources
-----------------

FDB based sources are built using a specific interface developed by AQUA.
While the procedure of adding the catalog tree entries is the same,
the main difference is on how the specific source is descrived.

We report here an example and we later describe the different elements.

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
                chunks: D  # Default time chunk size
                savefreq: h  # at what frequency are data saved
                timestep: h  # base timestep for step timestyle
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

    - The ``request`` entry in the intake catalogue primarily serves as a template for making data requests,
      following the standard MARS-style syntax used by the GSV retriever. 
    - The ``date`` parameter will be automatically overwritten by the appropriate ``data_start_date``.
      For the ``step`` parameter, when using ``timestyle: step``, setting it to a value other than 0
      signals that the initial steps are missing. 
      This is particularly useful for data sets with irregular step intervals, such as 6-hourly output.
    
    This documentation provides an overview of the key parameters used in the catalogue, helping users better understand how to configure their data requests effectively.

.. option:: data_start_date

    This defines the starting date of the experiment.
    It is mandatory to be set up because there is no easy way to get this information directly from the FDB.
    In the case of the schema used in the operational experiments, which use the 'date' ``timestyle`` (see below), 
    it is possible to set this parameter to ``auto``.
    In that case the date will be automatically determined from the FDB.
    Please notice that, due to how the date information is retrieved in the ``auto`` case,
    the time of the last date wll always be ``0000``. If there is more data available on the 
    last day, please consider setting the date manually.

.. option:: data_end_date

    As above, it tells AQUA when to stop reading from the FDB and it can be set to ``auto`` too (only if ``timestyle`` is 'date').

.. option:: chunks

    The chunks parameter is essential, whether you are using Dask or a generator.
    It determines the size of the chunk loaded in memory at each iteration. 

    When using a generator, it corresponds to the chunk size loaded into memory during each iteration.
    For Dask, it controls the size of each chunk used by Dask's parallel processing.

    The choice of the chunks value is crucial as it strikes a balance between memory consumption and
    distributing enough work to each worker when Dask is utilized with multiple cores. 
    In most cases, the default values in the catalog have been thoughtfully chosen through experimentation.

    For instance, an chunks value of ``D`` (for daily) works well for hourly-native data because it
    occupies approximately 1.2GB in memory.
    Increasing it beyond this limit may lead to memory issues. 

    It is possible to choose a smaller chunks value, but keep in mind that each worker has its own overhead,
    and it is usually more efficient to retrieve as much data as possible from the FDB for each worker.

    By the ``chunks`` argument is a string and refers to time-chunking.
    In more advanced cases it is possible to chunk both in time and in the vertical (along levels)
    by passing a dictionary to chunks with the keys ``time`` and ``vertical``. 
    In this case ``time`` is as usual a time frequency (in pandas notations) and ``vertical`` is instead the maxmimum number of vertical levels
    in each chunk.

    An example would be:

.. code-block:: yaml

    chunks:
      time: D  # Default time chunk size
      vertical: 3  # Three vertical levels in each chunk

.. option:: timestep

    The timestep parameter, denoted as ``H``, represents the original frequency of the model's output. 

    When timestep is set to ``H``, requesting data at ``step=6`` and ``step=7`` from the FDB will result
    in a time difference of 1 hour (``1H``).

    This parameter exists because even when dealing with monthly data,
    it is still stored at steps like 744, 1416, 2160, etc., which correspond to the number of hours since 00:00 on January 1st.

.. option:: savefreq

    Savefreq, indicated as ``M`` for monthly or ``h`` for hourly, signifies the actual frequency at which data are
    available in this stream. 

    Combining this information with the timestep parameter allows us to anticipate data availability at specific steps,
    such as 744 and 1416 for monthly data.

.. option:: timestyle

    The timestyle parameter can be set to either ``step``, ``date`` or ``yearmonth`` according to the FDB schema.
    Indeed, it determines how the time axis data is written in the FDB. 

    The above examples have used ``step``, which involves specifying a fixed ``date`` (e.g., 19500101) and ``time`` (e.g., 0000)
    in the request. Time axis is then identified by the ``step`` in the request.

    Alternatively, when timestyle is set to ``date``, you can directly specify both ``date`` and ``time`` in the request,
    and ``step`` is always set to 0.

    Finally, when using the ``yearmonth`` timestyle you do not have to set neither time, step, and date in the request.
    On the contrary, the ``year`` and ``month`` keys need to be specified. The FDB module will then build the corresponding
    request. 

    Please note that it is very important to know which timestyle has been used in the FDB before creating the request

.. option:: timeshift

    Timeshift is a boolean parameter used exclusively for shifting the date of monthly data back by one month.

    Implementing this correctly in a general case can be quite complex, so it was decided to implement only the monthly shift.

.. option:: metadata

    This includes important supplementary information:

    - ``fdb_path``: the path of the FDB configuration file (mandatory)
    - ``eccodes_path``: the path of the eccodes version used for the encoding/decoding of the FDB
    - ``variables``: a list of variables available in the fdb.
    - ``source_grid_name``: the grid name defined in aqua-grids.yaml to be used for areas and regridding
    - ``fixer_name``: the name of the fixer defined in the fixes folder
    - ``levels``: for 3D FDB data with a `levelist` in the request, this is the list of physical levels 
                  (e.g. [0.5, 10, 100, ...] meters while levelist contains [1, 2, 3, ...]).

    If the ``levels`` key is defined, then retrieving 3D data is greatly accelerated, since only one level 
    of each variable will actually have to be retrieved in order to define the Dataset.

.. warning::

    For FDB sources the ``metadata`` section contains very important informations that are used to
    retrieve the correct variables and levels.

Regridding capabilities
-----------------------

In order to make use of the AQUA regridding capabilities we will need to define the way the grid are defined for each source. 
AQUA is shipped with multiple grids definition, which are defined in the ``config/aqua-grids.yaml`` file.
In the following paragraphs we will describe how to define a new grid if needed.
Once the grid is defined, you can come back to this section to understand how to use it for your source.

Let's imagine that for our ``yearly_SST`` source we want to use the ``lon-lat`` grid,
which is defined in the ``config/aqua-grids.yaml`` file
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

.. _grid_definition:

Grid definitions
----------------

As mentioned above, AQUA has some predefined grids available in the ``config/grids`` folder.
Here below we provide some information on the grid key so that it might me possibile define new grids.
As an example, we use the healpix grid for ICON and tco1279 for IFS:

.. code-block:: yaml

    icon-healpix:
        path:
            2d: '{{grids}}/HealPix/icon_hpx{zoom}_atm_2d.nc'   # this is the default 2d grid
            2dm: '{{grids}}/HealPix/icon_hpx{zoom}_oce_2d.nc'  # this is an additional and optional 2d grid used if data are masked
            depth_full: '{{grids}}/HealPix/icon_hpx{zoom}_oce_depth_full.nc'
            depth_half: '{{grids}}/HealPix/icon_hpx{zoom}_oce_depth_half.nc'
        masked:   # This is the attribute used to distinguish variables which should go into the masked category
            component: ocean
        space_coord: ["cell"]
        vert_coord: ["depth_half", "depth_full"]


    tco1279:
        path: 
            2d: '{{grids}}/IFS/tco1279_grid.nc'
            2dm: '{{grids}}/IFS/tco1279_grid_masked.nc'
        masked_vars: ["ci", "sst"]
        vert_coord: ["2d", "2dm"]

.. note::

    Two kinds of template replacament are available in the files contained in the ``config/grids`` folder. The Jinja formatting ``{{ var }}`` is used to set
    variables as path that comes from the ``catalog.yaml`` file. The default python formatting ``{}`` is used for file structure which comes
    Reader arguments, as model, experiment or any other kwargs the user might set. Please pay attention to which one you are using in your files.
    In the future we will try to uniform this towards the Jinja formatting.


- **path**: Path to the grid data file, can be a single file if the grid is 2d,
  but can include multiple files as a function of the grid used.
  ``2d`` refers to the default grids, ``2dm`` to the grid for masked variables,
  any other key refers to specific 3d vertical structure (see ``vert_coord``)
- **space_coord**: The space coordinate how coordinates are defined and used for interpolation.
  Since AQUA v0.4 there is an automatic guessing routine, but this is a bit costly so it is better to specify this if possible.
- **masked** (if applicable): Keys to define variables which are masked.
  When using this, the code will search for an attribute to make the distinction (``component: ocean`` in this case).
  In alternative, if you want to apply masking only on a group of variables, you can defined ``vars: [var1, var2]``.
  In all the cases, the ``2dm`` grid will be applied to the data.
- **vert_coords** (if applicable): Vertical coordinate options for the grid.
  Specific for oceanic models where interpolation is changing at each depth level.
- **extra** (if applicable): Additional CDO command-line options to be used to process the files defined in ``path``.
- **cellareas**, **cellarea_var** (if applicable): Optional path and variable name where to specify a file to retrieve
  the grid area cells when the grid shape is too complex for being automatically computed by CDO.
- **regrid_method** (if applicable): Alternative CDO regridding method which is not the ``ycon`` default.
  To be used when grid corners are not available. Alterntives might be ``bil``, ``bic`` or ``nn``.

Other simpler grids can be defined using the CDO syntax, so for example we have ``r100: r360x180``.
Further CDO compatible grids can be of course defined in this way. 

A standard `lon-lat` grid is defined for basic interpolation and can be used for most of the regular cases,
as long as the ``space_coord`` are ``lon`` and ``lat``.


Compact catalogues with YAML override
-------------------------------------

In order to avoid having to write the same catalogue entry for each source,
in AQUA we can use the YAML override functionality also for the intake catalogues.
This allows to write the full rquest information only for a first 
base catalogue source and then define the following ones as copies of the first,
overriding only the keys that are different.

For example, let's imagine that we have a first source called ``hourly-native``
that is defined as:

.. code-block:: yaml

    sources: 
    hourly-native: &base-default
        description: hourly data on native grid TCo1279 (about 10km).
        args: &args-default
        request: &request-default
            class: d1
            resolution: high
            [ ... other request parameters ... ]
        data_start_date: 19900101T0000
        data_end_date: 19941231T2300
        chunks: D  
        [ ... other keys ... ]
        metadata: &metadata-default
            fdb_path: [ ... some path to the FDB ... ]
            eccodes_path: [ ... some path to the eccodes ... ]
            [ ... other keys ... ]

We can then define a second source as a copy of the first one,
specifying only what is different:

.. code-block:: yaml

    hourly-r025:
        <<: *base-default
        description: hourly 2D atmospheric data on regular r025 grid (1440x721).
        args:
            <<: *args-default
            request:
                <<: *request-default
                resolution: standard
        metadata:
            <<: *metadata-default
            fdb_path: [ ... some different path to the FDB ... ]

This second source will have the same keys as the first one, except for
the ones that are explicitly overridden.

.. Checking new data
.. -----------------

.. Checking that all the details of the source and of the experiments are fine can be exhausting task,
.. considering that several surces can be added to the same experiment. A good thing to do is to check that all 
.. sources are correctly working and most important reader functionalities as regridding and spatial averaging are working

.. We thus developed a basic function to run a check, `check_experiment()`, which can be simply called as:

.. .. code-block:: python

..     from aqua import check_experiment

..     check_experiment(model="IFS-NEMO", exp="awesome-exp")

.. This will open all the sources available and will regrid them. It can take a while and can be memory intensive, so it would be 
.. safer to not launch it from notebook. 


Intake capabilities and kwargs data access
------------------------------------------

Intake ships a template replacement capabilities based on Jinja2 which is able to "compress" multiple sources. 
This is combined by the capacity of AQUA of elaborating extra arguments which goes beyond the classical model-exp-source hierarchy
For example, we could assume we have a FDB source as the one above. However, this sources is made by multiple ensemble
members, and we want to described this in the catalog. This is something intake can easily handle with the Jinja `{{ }}`` syntax.


.. code-block:: yaml

    sources:
        hourly-native:
            args:
                request:
                    domain: g
                    class: rd
                    expver: a06x
                    realization: {{ realization }}

                    ...
                   
                driver: gsv
                parameters:
                    realization:
                        allowed: ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
                        description: realization member
                        type: str
                        default: '1'

This can be later accessed via the reader providing an extra argument, or kwargs in python jargon, which define the realization

.. code-block:: python

    reader = Reader(model="IFS", exp="control-1950-devcon", source="hourly-native", realization=5)
    data = reader.retrieve(var='2t')

This will load the realizaiton number 5 of my experiment above. Of course, if we do not specify the realization in the `Reader()`
call a default will be provided, so in the case above the number 1 will be loaded. 

This capacity can be tuned to multiple features according to source characteristics, and will be further expaned in the future.

.. warning::

    Some kwargs might have an impact on the resolution of the data, and consequently on the grid file name and format. An example is the `zoom` key used for some ICON data. 
    In this case, AQUA will modify the file templates accordingly. If this modication is required or not can be controlled through the
    variable ``default_weights_areas_parameters`` in the reader.py module. This is a test feature and will be expanded in the future. 



DE_340 source syntax convention
-------------------------------

Although free combination of model-exp-source can be defined in each catalog to get access to the data,
inside DE_340 a series of decision has been  taken to try to homogenize the definition of experiments and of sources.
We decide to use the dash (`-`) to connect the different elements of the syntax below.

Models (`model` key)
^^^^^^^^^^^^^^^^^^^^

This will be simply one of the three coupled models used in the project: IFS-NEMO, IFS-FESOM and ICON. 
Since version v0.5.2 we created coupled models catalog entries, though only on Lumi.
Analysing specific atmosphere-only or oceanic-only runs will still be possible.

Experiments (`exp` key)
^^^^^^^^^^^^^^^^^^^^^^^

Considering that we have strict set of experiments that must be produced, we will follow this 3-string convention:

1. **Experiment kind**: historical, control, sspXXX
2. **Starting year**: 1950, 1990, etc...
3. **Extra info** (optional): any information that might be important to define an experiment, as dev, test,
   the expid of the simulation, or anything else that can help for defining the experiment.

Examples are `historical-1990-dev` or `control-1950-dev`. For test experiments, we use simply the expid of the experiment

Sources (`source` key)
^^^^^^^^^^^^^^^^^^^^^^

For the sources, we decide to uniform the different requirements of grids and temporal resolution. 

0. **Domain**: Oceanic sources will have a `oce` prepended to all their sources
1. **Time resolution**: `monthly`, `daily`, `6hourly`, `hourly`, etc.
2. **Space resolution**: `native`, `1deg`, `025deg`, `r100`, etc... For some oceanic model we could add the horizontal grid so `native-elem` or `native-gridT`` could be an option. Similarly, if multiple healpix are present, they can be `healpix-0` or `healpix-6` in the case we want to specify the zoom level. 
3. **Extra info**: `2d` or `3d`. Not mandatory, but to be used when confusion might arise.





