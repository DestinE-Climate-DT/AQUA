.. _drop:

DROP - Data Reduction OPerator
===============================================

DROP (Data Reduction OPerator) is a comprehensive tool within the AQUA framework designed 
to extract, process, and organize data from any climate dataset.

.. warning ::

    Starting from AQUA v0.18, DROP is replacing Lor Resolution Archive (LRA) in most of the 
    terminology, since DROP is a more generic tool that deals with many more options other
    than just LRA. However, LRA is still a key use case for DROP (see below).
    
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
- Multiple statistics: mean, std, max, min
- Handling of incomplete time chunks

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

.. warning::
    Catalog detection is automatic, but specify the catalog name explicitly in the configuration 
    file if you have identically named triplets in different catalogs.

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