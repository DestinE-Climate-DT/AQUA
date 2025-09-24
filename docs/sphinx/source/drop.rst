.. _drop:

DROP - Data Reduction OPerator
===============================================

DROP (Data Reduction OPerator) is a comprehensive tool within the AQUA framework designed 
to extract, process, and organize data from any dataset, with particular focus on climate 
data reduction and archival.


What is DROP?
-------------

DROP evolved from the original LRA Generator concept, extending its capabilities beyond just 
creating Low Resolution Archives. While it maintains full compatibility with LRA generation, 
DROP now serves as a general-purpose data reduction platform that can:

- Extract data from any supported dataset
- Apply regridding, fixing, and temporal averaging
- Generate organized archives at various resolutions and frequencies
- Create catalog entries for seamless data access

DROP is a comprehensive data reduction platform that combines the regridding, fixing, and time 
averaging capabilities included in AQUA. The ``Drop`` class uses ``dask`` to exploit parallel 
computations and can process any supported dataset, with LRA generation being its primary but 
not exclusive use case. The platform can be explored in the `DROP notebook <https://github.com/oloapinivad/AQUA/blob/main/notebooks/drop/drop.ipynb>`_.


The Low Resolution Archive (LRA) Context
----------------------------------------

The Low Resolution Archive is a key use case for DROP. The LRA is an intermediate layer 
of data reduction that simplifies analysis of extreme high-resolution data by providing:

- Daily or monthly data at coarse resolution (typically 1°)
- Fast access for climate model assessment
- Reduced storage and computational requirements

Access to the LRA
-----------------

Once the LRA has been generated, access is possible via the standard ``Reader`` interface.
The only difference is that a specific source must be defined, following the syntax ``lra-$resolution-$frequency``

.. code-block:: python

    from aqua import Reader
    reader = Reader(model="IFS-NEMO", exp="historical-1990", source="lra-r100-monthly")
    data = reader.retrieve()

.. note ::

    LRA built available on Levante and Lumi by AQUA team are all at ``r100`` (i.e. 1 deg resolution) and at ``monthly`` or ``daily`` frequency. 

.. note ::
    Since version v0.11 the LRA access is granted not only with usual NetCDF files but also with Zarr reference files.
    This is possible by setting ``source="lra-r100-monthly-zarr"`` in the Reader initialization. This will allow for faster access to the data.
    Please notice this access is experimental and could not work with some specific experiment.


Generation of the LRA using DROP
--------------------------------

Given the computational intensity required, the standard approach is to use DROP through a 
command line interface (CLI) available from the console with the subcommand ``aqua drop``

The configuration of the CLI is done via a YAML file that can be build from the 
``drop_config.tmpl``, available in the ``.aqua/templates/drop`` folder after the installation.
This includes the target resolution, the target frequency, the name and the boundaries of a 
possible subselection, the temporary directory and the directory where you want to store the 
obtained LRA.
SLURM options as well as the number of workers can be set up with the configuration file.

Most importantly, you have to edit the entries of the ``data`` dictionary, which follows the model-exp-source 3-level hierarchy.
You must specify the variables you want to process under the ``vars`` key, and optionally configure:

- ``resolution``: target spatial resolution 
- ``frequency``: target temporal frequency
- ``stat``: statistic to compute (mean, std, max, min)
- ``region``: spatial subsetting configuration

.. caution::
    Catalog detection is done automatically by the code. 
    However, if you have triplets with same name in two different catalog, you should also specify the catalog name in the configuration file.


Usage
^^^^^

.. code-block:: python

    aqua drop <options>

Options: 

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

    Overwrite LRA existing data (default: WARNING).

.. option:: --monitoring

    Enable a single chunk run to produce the html dask performance report. Dask should be activated.

.. option:: --only-catalog

    Will generate/update only the catalog entry for the LRA, without running the code for generating the LRA itself

.. option:: --rebuild

    This option will force the rebuilding of the areas and weights files for the regridding.
    If multiple variables or members are present in the configuration, this will be done only once.

.. option:: --stat

    Statistic to be computed (default: 'mean')

.. option:: --frequency

    Frequency of the LRA (default: as the original data)

.. option:: --resolution

    Resolution of the LRA (default: as the original data)

.. option:: --realization

    Which realization (e.g. ensemble member) to use for the LRA (default: 'r1')

Please note that these options override the ones available in the configuration file. 

A basic example usage can thus be: 

.. code-block:: bash

    aqua drop -c drop_config.yaml -d -w 4

.. warning ::

    Keep in mind that this script is ideally submitted via batch to a HPC node, 
    so that a template for SLURM is also available in the same directory (``.aqua/templates/drop/drop-submitter.tmpl``). 
    Be aware that although the computation is split among different months, the memory consumption of loading very big data
    is a limiting factor, so that unless you have very fat node it is unlikely you can use more than 16 workers.

At the end of the generation, a new entry for the LRA is added to the catalog structure, 
so that you will be able to access the exactly as shown above.

Parallel DROP tool
^^^^^^^^^^^^^^^^^^

Building LRA data can be a memory-intensive task that cannot be easily parallelized within a 
single job.
An additional script for parallel execution is provided: using `cli_drop_parallel_slurm.py`, 
you can submit multiple SLURM jobs to process different variables simultaneously. It builds 
on Jinja2 template replacement from a typical SLURM script `aqua_drop.j2`.
For now it is configured only to be run on LUMI but further development should allow for 
larger portability.

A basic example usage:

.. code-block:: bash

    ./cli_drop_parallel_slurm.py -c drop_config.yaml -d -w 4 -p 4

This launches the `definitive` LRA generation using 4 workers per node and a maximum of 4 
concurrent SLURM jobs.

A ``-s`` option to call the run via container instead of using the local installation

.. warning ::
    Use this script with caution since it will submit very rapidly tens of job to the SLURM scheduler!

    
DROP Capabilities Beyond LRA
----------------------------

While LRA generation remains the primary use case, DROP's flexible architecture enables various data processing tasks:

**Temporal Processing:**
- Custom frequency resampling (any frequency to any frequency)
- Multiple statistics: mean, std, max, min
- Handling of incomplete time chunks

**Spatial Processing:**
- Regridding to any supported resolution or native grid
- Regional data extraction with configurable boundaries  
- Support for both regular and irregular grids

**Data Management:**
- Automatic catalog entry generation
- Zarr reference creation for faster access
- Parallel processing with configurable workers
- Memory-efficient chunked processing

**Quality Control:**
- Data fixing and validation
- Integrity checking of output files
- Performance monitoring and reporting

**Example use cases:**
- Extract daily European data from global monthly archives
- Convert model output from native grid to regular 0.25° grid
- Create statistical summaries (std, max, min) instead of just means
- Process specific ensemble members or realizations