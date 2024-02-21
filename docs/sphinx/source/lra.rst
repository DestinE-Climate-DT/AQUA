Low Resolution Archive
======================


The creation and the usage of the Low Resolution Archive (LRA) is a key concept of AQUA framework to simplify
the analysis of extreme high resolution data. Indeed, many diagnostics included within AQUA do not need extremely
high-resolution high-frequency data, and they can assess the model behaviour with daily or monthly data at 
relatively coarse resolution. Therefore LRA represents an intermediate layer of data reduction that can be used 
for simpler and fast analysis which can be valuable for climate model assessment. 

Basic Concepts
--------------

The LRA is basically a wrapper of the functionalities included in AQUA, combining the regridding, fixing
and time averaging capabilities. A specific class ``LRAgenerator`` has been developed, using ``dask`` in order to exploit parallel
computations, and can be investigated in a the `LRA generator notebook <https://github.com/oloapinivad/AQUA/blob/main/notebooks/lra_generator/lra_generator.ipynb>`_.

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

Generation of the LRA
---------------------

Given the character of the computation required, the standard approach is to use the LRA through a command line 
interface (CLI) which is available in ``cli/lra/cli_lra_generator.py``

The configuration of the CLI is done via a YAML file that can be build from the ``lumi_lra_config.tmpl`` or ``levante_lra_config.tmpl`` 
templates, which include the target resolution, the target frequency, the temporary directory and the 
directory where you want to store the obtained LRA.

Most importantly, you have to edit the entries of the ``catalog`` dictionary, which follows the model-exp-source 3-level hierarchy.
On top of that you must specify the variables you want to produce under the ``vars`` key.

Usage
^^^^^

.. code-block:: python

    ./cli_lra_generator.py

Options: 

.. option:: -d, --definitive

    Run the code and produce the data (a dry-run will take place if this flag is missing)

.. option:: -f, --fixer

    Set up the Reader fixing capabilities (default: True)

.. option:: -w, --workers

    Set up the number of dask workers (default: 1)

.. option:: -l, --loglevel

    Set up the logging level.

.. option:: -c CONFIG, --config CONFIG

    Set up a specific configuration file (default: lra_config.yaml).

.. option:: -o, --overwrite

    Overwrite LRA existing data (default: WARNING).


Please note that this options override the ones available in the configuration file. 

A basic example usage can thus be: 

.. code-block:: python

    ./cli_lra_generator.py -c lra_config.yaml -d -w 4

.. warning ::

    Keep in mind that this script is ideally submitted via batch to a HPC node, 
    so that a template for SLURM is also available in the same directory (``lra-submitter.tmpl``). 
    Be aware that although the computation is split among different months, the memory consumption of loading very big data
    is a limiting factor, so that unless you have very fat node it is unlikely you can use more than 16 workers.

At the end of the generation, a new entry for the LRA is added to the catalog structure, 
so that you will be able to access the exactly as shown above.

Parallel LRA tool
^^^^^^^^^^^^^^^^^

Building the LRA can be an heavy task, which requires a lot of memory and thus cannot be easily parallized in the same job.
To this end, an extra script for parallel execution is also provided. Using `cli_lra_parallel_slurm.py` it is possible to submit to SLURM multiple jobs,
one for each of the variables to be processed. For now it is configured only to be run on LUMI but further development should allow for larger portability.

A basic example usage can thus be: 

.. code-block:: python

    ./cli_lra_parallel_slurm.py -c lra_config.yaml -d -w 4

.. note ::
    Please consider that this script will call both SLURM and the standard `cli_lra_generator.py`, so that modification to the latter will influence this. 

.. warning ::
    Use this script with caution since it will submit very rapidly tens of job to the SLURM scheduler!

Workflow LRA tool
^^^^^^^^^^^^^^^^^

Due to integration with workflow, another LRA CLI tool is available to generate the LRA in a more complex environment accessing data from the GSV
The script, named ``cli_lra_workflow.py`` is the LRA generator used within the DE_340 workflow. 
It is made to work from OPA output and then process them to fix and standardize it.
Please refer to AQUA team or workflow developers to get more information on how to use this tool.
A template configuration file ``workflow_lra.tmpl`` is included in the folder. 
The usage is the same as the main LRA generator script discussed above since it builts on the same `LRAGenerator` class. 
    
