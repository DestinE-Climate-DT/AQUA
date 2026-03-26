.. _drop:

DROP - Data Reduction OPerator
===============================================

DROP (Data Reduction OPerator) is a comprehensive tool within the AQUA framework designed
to extract, process, and organize data from any climate dataset.

What is DROP?
-------------

DROP is a comprehensive data reduction operator that combines the regridding, fixing, and time
averaging capabilities included in AQUA. The ``Drop`` class uses ``dask`` to exploit parallel
computations and can process any supported dataset, and it serves as a general-purpose data
reduction platform.


DROP Capabilities
-----------------

DROP's architecture enables various data processing tasks:

**Temporal Processing:**

- Custom frequency resampling (any frequency to any frequency)
- Multiple statistics: mean, std, max, min, sum or histogram
- Handling of incomplete time chunks
- Support for kwargs to specify arguments of callable statistics (e.g. histogram bins and range)

**Spatial Processing:**

- Regridding to any supported resolution or native grid
- Regional data extraction with configurable boundaries
- Support for both regular and irregular grids

**Data Management:**

- Automatic catalog entry generation for DROP-generated outputs
- Zarr reference creation for faster access
- Parallel processing with configurable workers
- Memory-efficient chunked processing

**Example use cases:**

- Extract daily European data from global monthly archives
- Convert model output from native grid to regular 0.25° grid
- Create statistical summaries (std, max, min) instead of just means
- Process specific ensemble members or realizations

DROP can be explored in the `DROP notebook <https://github.com/DestinE-Climate-DT/AQUA/blob/main/notebooks/drop/drop.ipynb>`_.


The Low Resolution Archive (LRA) Context
----------------------------------------

The Low Resolution Archive is a key use case for DROP. The LRA is an intermediate layer of data
reduction that simplifies analysis of extreme high-resolution data by providing monthly data 1
degree resolution, permitting reduced storage and computational requirements.

.. note ::

    LRA built available on Levante and Lumi by AQUA team are all at ``r100`` (i.e. 1 deg
    resolution) and at ``monthly`` frequency. The corresponding catalog entry name is
    ``lra-r100-monthly``.

Source Naming Convention
------------------------

DROP automatically generates catalog source names following a consistent pattern:

**Standard naming format:**

- **Pattern**: ``{resolution}-{frequency}``
- **Examples**:

  - ``r100-monthly`` (1° resolution, monthly frequency)
  - ``r100-daily`` (1° resolution, daily frequency)
  - ``r25-monthly`` (0.25° resolution, monthly frequency)

**Default LRA source:**

- ``lra-r100-monthly``

**Zarr variants:**
All sources have corresponding Zarr reference versions with ``-zarr`` suffix:

- ``r100-monthly-zarr``
- ``r100-daily-zarr``

**Resolution codes:**

- ``r100`` = 1° (100km approximately)
- ``r25`` = 0.25° (25km approximately)
- ``native`` = original model grid

**Parameter-based access:**
Different processing options can be accessed via Reader kwargs:

.. code-block:: python

    # Access specific statistics (if generated)
    reader = Reader(model="IFS-NEMO", exp="historical-1990",
                   source="r100-monthly", stat="std")

    # Access regional data (if generated)
    reader = Reader(model="IFS-NEMO", exp="historical-1990",
                   source="r100-monthly", region="europe")

    # Access specific ensemble realizations
    reader = Reader(model="IFS-NEMO", exp="historical-1990",
                   source="r100-monthly", realization="r2")


Accessing DROP-generated data
-----------------------------

Once DROP has processed the data, generated outputs can be accessed via the standard ``Reader``
interface using the automatically created catalog sources.

.. code-block:: python

    from aqua import Reader
    reader = Reader(model="IFS-NEMO", exp="historical-1990", source="lra-r100-monthly")
    data = reader.retrieve()

**Advanced access patterns:**

.. code-block:: python

    # Access standard deviation instead of mean
    reader = Reader(model="ERA5", exp="era5", source="r100-monthly", stat="std")
    std_data = reader.retrieve()

    # Access regional European data
    reader = Reader(model="IFS-NEMO", exp="historical-1990",
                   source="r25-daily", region="europe")
    eu_data = reader.retrieve()

    # Access specific ensemble member
    reader = Reader(model="IFS-NEMO", exp="historical-1990",
                   source="r100-daily", realization="r3")
    member_data = reader.retrieve()

**Zarr access for faster performance:**

You can access data using Zarr reference files for improved performance, when available:

.. code-block:: python

    # Faster access using Zarr references
    reader = Reader(model="IFS-NEMO", exp="historical-1990", source="r100-monthly-zarr")
    data = reader.retrieve()

.. note ::
    The specific source names depend on the resolution and frequency you configured when
    running DROP. See the "Source Naming Convention" section above for details.

.. warning ::
    Zarr reference access is experimental and may not work with all experiment configurations.

Using DROP to process data
--------------------------

DROP processes data through a command line interface (CLI) available with the subcommand ``aqua drop``.

Configuration is done via a YAML file that can be built from the ``drop_config.tmpl``,
available in the ``.aqua/templates/drop`` folder after installation. The configuration
file allows you to specify:

- Target resolution and frequency
- Variables to process
- Regional boundaries (optional)
- Output and temporary directories
- SLURM options and number of workers

**Configuration structure:**

The configuration follows the model-exp-source 3-level hierarchy in the ``data`` dictionary.
Key configuration options include:

- ``vars``: variables to process
- ``resolution``: target spatial resolution (e.g., ``r100``, ``r25``, ``native``)
- ``frequency``: target temporal frequency (e.g., ``monthly``, ``daily``, ``3hourly``)
- ``stat``: statistic to compute (``mean``, ``std``, ``max``, ``min``)
- ``region``: spatial subsetting configuration
- ``engine``: The engine used for the GSV retrieval, options are 'fdb' and 'polytope'.

.. warning::
    Catalog detection is automatic, but specify the catalog name explicitly in the configuration
    file if you have identically named triplets in different catalogs.

Configuration File
^^^^^^^^^^^^^^^^^^

The DROP configuration file is structured in YAML format with four main sections: ``target``,
``paths``, ``options``, ``slurm``, and ``data``. Below is a detailed explanation of each
configuration parameter.

**Target Section**

The ``target`` section defines the primary output characteristics for the DROP processing:

.. code-block:: yaml

    target:
      resolution: r100
      frequency: monthly
      catalog: my_catalog
      startdate: "2020-01-01T00:00:00"
      enddate: "2020-12-31T23:00:00"
      region:
        name: Europe
        lat: [35, 70]
        lon: [-10, 40]
      stat: mean
      stat_kwargs: {}

- **resolution** (string, required): Target spatial resolution for regridding.

  - ``r100``: 1° resolution (~100km) or any other supported target grid (see :ref:`available-target-grids`)
  - ``native``: Keep original model grid (no regridding)

- **frequency** (string, required): Target temporal frequency for output.

  - ``monthly``, ``daily``, ``3hourly``, ``6hourly``, ``hourly``
  - Any valid frequency string supported by pandas ``resample``
  - If not specified, keeps original data frequency

- **catalog** (string, optional): Name of the catalog to process.

  - It will be used for all the models listed in the ``data`` section.

- **startdate** (string, optional): Starting date for data processing.

  - Format: ``YYYY-MM-DD`` or any valid date string parsable by pandas
  - Example: ``"2020-01-01"``
  - If omitted, processes from the first available date

- **enddate** (string, optional): Ending date for data processing.

  - Format: ``YYYY-MM-DD`` or any valid date string parsable by pandas
  - Example: ``"2020-12-31"``
  - If omitted, processes until the last available date

- **region** (dict, optional): Spatial subsetting configuration. If omitted, processes global data.

  - **name** (string): Region identifier (e.g., ``Europe``, ``Tropics``)
  - **lat** (list): Latitude range as ``[min, max]`` (e.g., ``[35, 70]``)
  - **lon** (list): Longitude range as ``[min, max]`` (e.g., ``[-10, 40]``)

- **stat** (string, optional): Statistical operator for temporal aggregation. Default: ``mean``

  - ``mean``: Arithmetic mean
  - ``std``: Standard deviation
  - ``max``: Maximum value
  - ``min``: Minimum value
  - ``sum``: Sum of values
  - ``histogram``: Compute histogram (requires ``stat_kwargs`` to specify the `range` argument)

- **stat_kwargs** (dict, optional): Additional arguments for the statistical function. Default: ``{}``

  - For ``histogram`` e.g.: ``{bins: 20, range: [0, 100]}``
  - Empty dict or missing line for other statistics that don't require additional arguments

**Paths Section**

Defines the directory structure for outputs and temporary files:

.. code-block:: yaml

    paths:
      outdir: /path/to/output
      tmpdir: /path/to/tmp

- **outdir** (string, required): Directory where final DROP outputs will be stored.

  - Should have sufficient space for processed data
  - Subdirectories are automatically created based on catalog/model/exp/source hierarchy

- **tmpdir** (string, required): Directory for temporary files during processing.

  - Must be on fast storage (ideally local to compute node)
  - Should have space for intermediate monthly files and aggregated yearly files

**Options Section**

Controls processing behavior and performance settings:

.. code-block:: yaml

    options:
      engine: fdb
      loglevel: INFO
      zarr: False
      verify_zarr: False
      overwrite: False
      exclude_incomplete: False
      rebuild: False
      compact: xarray
      cdo_options: ["-f", "nc4", "-z", "zip_1"]
      performance_reporting: False

- **engine** (string, optional): Data retrieval engine. Default: ``fdb``

  - needed only for GSV retrieval, options are 'fdb' and 'polytope'
  - ``fdb``: Fields DataBase, you should be on the same machine where the database is located
  - ``polytope``: Polytope service (remote access). Be sure to have the correct credentials and network access to use this option.

- **loglevel** (string, optional): Logging verbosity. Default: ``WARNING``

  - Available levels: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``

- **zarr** (bool, optional): Create Zarr reference files for faster subsequent access. Default: ``False``

  - ``True``: Generate Zarr references after processing
  - ``False``: Only create NetCDF files, default behavior

- **verify_zarr** (bool, optional): Verify Zarr references after creation. Default: ``False``

  - ``True``: Test Zarr references by loading data
  - ``False``: Skip verification
  - Only relevant when ``zarr: True``

- **overwrite** (bool, optional): Overwrite existing output files. Default: ``False``

  - ``True``: Replace existing files
  - ``False``: Skip processing if files exist
  - DROP checks if the existing files are complete before skipping, so it won't skip if files are incomplete or corrupted

- **exclude_incomplete** (bool, optional): Exclude incomplete temporal chunks. Default: ``False``

  - ``True``: Drop months/periods with missing data
  - ``False``: Process all available data

- **rebuild** (bool, optional): Force rebuilding of regridding weights. Default: ``False``

  - ``True``: Regenerate area and weight files
  - ``False``: Use cached weights if available
  - Set to ``True`` if you suspect weights are outdated (e.g., after a major update to CDO or AQUA)

- **compact** (string, optional): Method for concatenating monthly files into yearly files. Default: ``xarray``

  - ``xarray``: Use xarray for concatenation
  - ``cdo``: Use Climate Data Operators
  - ``null`` or omit: No compacting, keep monthly files

- **cdo_options** (list, optional): Options passed to CDO when ``compact: cdo``. Default: ``["-f", "nc4", "-z", "zip_1"]``

  - ``-f nc4``: NetCDF4 format
  - ``-z zip_1``: Compression level 1
  - Add additional CDO flags as list elements

- **performance_reporting** (bool, optional): Generate Dask performance HTML report. Default: ``False``

  - ``True``: Create detailed performance report for one chunk. Then the job will stop.
  - ``False``: No performance monitoring

**SLURM Section**

Configuration for HPC job submission (used by parallel DROP tools):

.. code-block:: yaml

    slurm:
      partition: standard
      username: myuser
      account: myproject
      time: "02:00:00"
      mem: "64GB"

- **partition** (string): SLURM partition name (e.g., ``standard``, ``compute``, ``large-mem``)
- **username** (string): Your HPC username
- **account** (string): Project or account name for billing
- **time** (string): Maximum wall time (format: ``HH:MM:SS``)
- **mem** (string): Memory allocation per job (e.g., ``64GB``, ``128GB``)

**Data Section**

Defines the hierarchical structure of data to process. They have all to be inside the same catalog specified in the ``target`` section.

.. code-block:: yaml

    data:
      MODEL_NAME:
        EXPERIMENT_NAME:
          SOURCE_NAME:
            vars: ['var1', 'var2', 'var3']
            workers: 12
            realizations: [0, 1, 2]
            zoom: 8
            resolution: r25
            frequency: daily
            stat: std

The ``data`` section uses a three-level nested structure:

1. **Model level**: Top-level key for each model (e.g., ``ICON``, ``IFS-NEMO``)
2. **Experiment level**: Second-level key for each experiment (e.g., ``historical-1990``)
3. **Source level**: Third-level key for each data source (e.g., ``hourly-hpz10-atm2d``)

Each source configuration supports the following parameters:

- **vars** (list, required): List of variable short names to process.

  - Example: ``['2t', 'tprate', 'msl']``

- **workers** (int, optional): Number of Dask workers for parallel processing. Default: 1

  - Typical range: 4-16 depending on available memory and vertical levels
  - 1 worker disables parallel processing

- **realizations** (list, optional): Specific ensemble members to process.

  - Example: ``[0, 1, 2]`` processes r0, r1, and r2
  - If omitted, processes the default realization (r1)
  - Only applicable to ensemble datasets

- **resolution** (string, optional): Override target resolution for this specific source.

- **frequency** (string, optional): Override target frequency for this specific source.

- **stat** (string, optional): Override statistical operator for this specific source.

**Example: Multiple Models and Configurations**

.. code-block:: yaml

    data:
      ICON:
        historical-1990:
          hourly-hpz10-atm2d:
            vars: ['2t', 'tp', 'msl']
            workers: 12
            resolution: r100
            frequency: daily
            stat: mean

          daily-hpz10-oce2d:
            vars: ['avg_sithick', 'avg_siconc']
            workers: 16
            frequency: monthly

      IFS-NEMO:
        historical-1950:
          daily:
            vars: ['2t', 'tp']
            workers: 8
            stat: max
            region:
              name: Europe
              lat: [35, 70]
              lon: [-10, 40]

This configuration will process:

1. ICON historical-1990 atmospheric variables at daily/r100 resolution
2. ICON historical-1990 ocean variables at monthly frequency
3. IFS-NEMO historical-1950 daily maximum values for European region

**Configuration Precedence**

When the same parameter appears at multiple levels, the precedence order is:

1. **Command-line arguments** (highest priority)
2. **Source-level settings** in the ``data`` section
3. **Target-level settings** in the ``target`` section (lowest priority)

This allows you to set global defaults in ``target`` and override them for specific
sources or via command line.

Usage
^^^^^

.. code-block:: python

    aqua drop <options>

**Options:** these override the configuration file options.

.. option:: -c CONFIG, --config CONFIG

    Set up a specific configuration file

.. option:: -d, --definitive

    Run the code and produce the data (a dry-run will take place if this flag is missing)

.. option:: -f, --fix

    Set up the Reader fixing capabilities (default: True)

.. option:: -w, --workers

    Set up the number of dask workers (default: 1, i.e. dask disabled)

.. option:: -l, --loglevel

    Set up the logging level.

.. option:: -o, --overwrite

    Overwrite existing data (default: WARNING).

.. option:: --monitoring

    Enable a single chunk run to produce the html dask performance report. Dask should be activated.

.. option:: --only-catalog

    Will generate/update only the catalog entry for DROP, without running the code for generating DROP output itself

.. option:: --rebuild

    This option will force the rebuilding of the areas and weights files for the regridding.
    If multiple variables or members are present in the configuration, this will be done only once.

.. option:: --stat

    Statistic to be computed (default: 'mean')

.. option:: --frequency

    Frequency of the DROP output (default: as the original data)

.. option:: --resolution

    Resolution of the DROP output (default: as the original data)

.. option:: --realization

    Which realization (e.g. ensemble member) to use for the DROP output (default: 'r1')

.. option:: --startdate

    Start date for the DROP output (default: as the original data).
    Accepted format: 'YYYY-MM-DDT00:00:00'

.. option:: --enddate

    End date for the DROP output (default: as the original data).
    Accepted format: 'YYYY-MM-DDT23:00:00'

.. option:: --engine

    The engine used for the GSV retrieval, options are 'fdb' (default) and 'polytope'.

**Examples:**

Process data to create monthly 1° resolution output:

.. code-block:: bash

    aqua drop -c drop_config.yaml -d -w 4

Generate daily data at 0.25° resolution with 8 workers:

.. code-block:: bash

    aqua drop -c drop_config.yaml -d -w 8 --resolution r25 --frequency daily

.. warning ::

    Keep in mind that this script is ideally submitted via batch to a HPC node,
    so that a template for SLURM is also available in the same directory (``.aqua/templates/drop/drop-submitter.tmpl``).
    Be aware that although the computation is split among different months, the memory consumption of loading very big data
    is a limiting factor, so that unless you have very fat node it is unlikely you can use more than 16 workers.

**Output:**

After processing, new catalog entries are automatically created following the naming
convention described above, allowing immediate access to your processed data.

Parallel DROP tool
^^^^^^^^^^^^^^^^^^

Using DROP can be a memory-intensive task, that cannot be easily parallelized within a single job.
For processing multiple variables or large datasets, use the parallel execution script
``cli_drop_parallel_slurm.py`` to submit multiple SLURM jobs simultaneously:

.. code-block:: bash

    ./cli_drop_parallel_slurm.py -c drop_config.yaml -d -w 4 -p 4

This processes data using 4 workers per node with up to 4 concurrent SLURM jobs.
It builds on Jinja2 template replacement from a typical SLURM script `aqua_drop.j2`.
For now it is configured only to be run on LUMI but further development should allow for
larger portability.

A ``-s`` option to call the run via container instead of using the local installation.

.. warning ::

    Use with caution. This script rapidly submits tens of jobs to the SLURM scheduler!
