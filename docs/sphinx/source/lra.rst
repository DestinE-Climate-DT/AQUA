Low Resolution Archive
======================


The creation and the usage of the Low Resolution Archive (LRA) is a key concept of AQUA framework to simplify
the analysis of very high resolution data. Indeed, many diagnostics included within AQUA does not need extremely
high-resolution high-frequency data, and they can assess the model behaviour with daily or monthly data at 
relatively coarse resolution. Therefore LRA will represent an intermediate layer of data reduction that can be used 
for simpler and fast analysis which can be valuable for climate model assessment. 

.. note ::

    In the long term this will be handled by the Data Bridge and by the Multi I/O features
    but on the short term AQUA  provides a series of tool to aggregate data

Basic Concepts
--------------

The LRA is basically a wrapper of the functionalities included in AQUA, combining the regridding, the fixing
and time averaging capabilities. A specific class `LRAgenerator` has been developed, using `dask` in order to exploit parallel
computations, and can be investigated in a the `LRA generator notebook <https://github.com/oloapinivad/AQUA/blob/main/notebooks/lra_generator/lra_generator.ipynb>`_

Access to the LRA
-----------------

Once the LRA has been generated, access is possible via the standard `Reader` interface.
The only difference is that a specific source must be defined, following the syntax `lra-$resolution-$frequency`

.. code-block:: python

    from aqua import Reader
    reader = Reader(model="FESOM", exp="tco2559-ng5", source="lra-r100-mon")
    data = reader.retrieve()


.. note ::

    LRA built available on Levante and Lumi by AQUA team are all at `r100` (i.e. 1 deg resolution) and at `mon` or `day` frequency

Generation of the LRA
---------------------

Given the character of the computation required, the standard approach is to use the LRA through a command line 
interface (CLI) which is available in `cli/lra/cli_regridder.py`

The configuration of the CLI is done via a `lra_config.yaml` file, which include the target resolution, the target frequency,
the temporary directory and the directory where you want to store the obtained LRA. A template for the file is included in the folder.

Other options includes the logging level and the usage of the One Pass Algorithm (in beta mode)  to produce the temporal
aggregation.

Most importantly, you have to edit the entries of the `catalog` dictionary, which follows the model-exp-source hierarchy.
On top of that you must specify the variables you want to produce under the `vars` key.


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

    ./cli_lra_generator.py -d -w 4


.. warning ::

    Keep in mind that this script is ideally submitted via batch to a HPC node, 
    so that a template for SLURM is also available in the same directory. 
    Be aware that although the computation is split among different months, the memory consumption of loading very big data
    is a limiting factor, so that unless you have very fat node it is unlikely you can use more than 16 nodes

At the end of the generation, a new entry for the LRA is added to the catalog structure, 
so that you will be able to access the exactly as shown above.

