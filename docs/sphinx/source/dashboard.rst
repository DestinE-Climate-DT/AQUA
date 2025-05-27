.. _dashboard:

Dashboard
=========

General description and main components
---------------------------------------

AQUA can act as a backend for an online web application (a "dashboard") to monitor the progress of the analysis and of the results.

A series of scripts is used to automatize running of the diagnostics on experiment output, collecting the figures and storing them 
in a remote object storage. The figures are then used to update a website that shows the results of the analysis.

Currently AQUA supports `aqua-web <https://github.com/DestinE-Climate-DT/aqua-web>`_ (The "AQUA Explorer"), providing a basic static webpage for showing aqua results, 
hosted in CSC Openshift and using the LUMI-O object storage.

An alternative, more complete, dashboard is under development. The future dashboard will use the same LUMI-O storage already used by aqua-web.

The full pipeline is composed of the following components:

1.  The ``aqua-analysis.py`` script that runs several diagnostics for a single experiment on the HPC. 
    This script produces all figures in a specified output directory following a ``catalog/model/experiment`` structure.
    After the analysis run is completed, the figures for each diagnostic will be stored in individual subdirectories of the output directory.
    The script also produces an ``experiment.yaml`` file with metadata about the experiment (extracted from the catalog entry).

    See the :ref:`aqua_analysis` section for more details.


2.  **(optional, not used in the workflow)** In order to submit several ``aqua-analysis.py`` jobs in parallel, for different experiments,
    the ``submit-aqua-web.py`` script can be used. This script reads a list of experiments from a file and submits the ``aqua-analysis`` 
    jobs to the HPC. After all the jobs are completed, the script can push the produced figures to aqua-web wesite using ``push_analysis.sh``.

    See the :ref:`submit-aqua-web` section for more details.


3.  The ``push_analysis.sh`` bash script is used to push the produced figures to a remote object store (LUMI-O for DestinE).
    It collects figures from the ``aqua-analysis.py`` output directory, pushes them to the ``aqua-web`` bucket on LUMI-O,
    creates a ``content.yaml``/ ``content.json`` file for each experiment, and
    updates the ``updated.txt`` file on the aqua-web github repository to trigger the website update.
    It uses the ``experiment.yaml`` file for each experiment created by ``aqua-analysis.py`` to get the metadata.
    It also generates a general ``experiments.yaml`` file listing all experiments available, stored on LUMI-O in ``s3://aqua-web/content/png``.
    
    See the :ref:`aqua_web` section for more details.


4.  The ``make_push_docs.sh`` script can be used to build the documentation, push it to the LUMI-O bucket and trigger a rebuild of aqua-web.
   
    See the :ref:`aqua_web` section for more details.


5.  The AQUA Explorer software is stored on github in the repository `aqua-web <https://github.com/DestinE-Climate-DT/aqua-web>`_.
    Any commit in the `aqua-web` repository will trigger a rebuild of the enclosed Dockerfile by the CSC `Rahti 2 service <https://research.csc.fi/-/rahti>`_ using OpenShift. The resulting container will run serving the web pages.
    As described in the corresponding Dockerfile, contents (figures and documentation) are downloaded from the ``aqua-web`` bucket on LUMI-O, appropriate markdown files are created and  we use python package `mkdocs` to construct static web pages from the markdown files.


More details on the available tools and on their dependencies are provided in the following.

.. _aqua_web:

Automatic uploading of figures and documentation to aqua-web
------------------------------------------------------------

AQUA figures produced by the analysis can be uploaded to the `aqua-web <https://github.com/DestinE-Climate-DT/aqua-web>`_ 
repository to publish them automatically on a dedicated website. The same site is used to host the documentation.
Two scripts in the ``cli/aqua-web`` folder are available to push figures or documentation to aqua-web.

If you plan to use these scripts outside the AQUA container or environment to push figures to aqua-web,
you will need the following scripts: ``push-analysis.sh``, ``make_contents.py``, ``pdf_to_png.sh``
and ``push_s3.py``. 
The following python packages will be needed: ``boto3``, ``pyYAML`` and ``pypdf`` and the ``imagemagick`` package.

Basic usage
^^^^^^^^^^^

.. code-block:: bash

    bash push-analysis.sh [OPTIONS] INDIR EXPS

This script is used to push the figures produced by the AQUA analysis to the aqua-web repository.
``INDIR`` is the directory containing the output, e.g. ``~/work/aqua-analysis/output``.
``EXPS`` is the subfolder to push, e.g ``climatedt-phase1/IFS-NEMO/historical-1990``
or a text file containing a list of experiments in the format "catalog model experiment".
It creates ``content.yaml`` files for each experiment, pushes the images to the ``aqua-web`` bucket on LUMI-O and
updates the ``updated.txt`` file on the aqua-web github repository to trigger the website update.

The needed AWS credentials can be stored in the ``~/.aws/credentials`` file or in environment 
variables ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``.

Additional options
^^^^^^^^^^^^^^^^^^

.. option:: -b <bucket>, --bucket <bucket>

    The bucket to use for the LUMI-O push (default is 'aqua-web').

.. option:: -c <configfile>, --config <configfile>

    Alternate config file for make_contents (default is 'config.aqua-web.yaml' in the script directory). 
    To be used together with the rsync option.

.. option:: -d, --no-update

    Do not update the aqua-web Github repository.

.. option:: -h, --help

    Display the help and exit.

.. option:: -l <level>, --loglevel <level>

    Set the log level (1=DEBUG, 2=INFO, 3=WARNING, 4=ERROR, 5=CRITICAL). Default is 2.  

.. option:: -n, --no-convert

    Do not convert PDFs to PNGs. To be used only if all needed figures have already been generated by the diagnostics.

.. option:: -r <repository>, --repository <repository>

    The remote aqua-web repository to update (default is 'DestinE-Climate-DT/aqua-web').
    If it starts with 'local:', a local directory is used.

.. option:: --rsync <target>
    
    Remote rsync target (takes priority over s3 bucket if specified).
    The syntax is for example:
    ``--rsync user@myremotemachine.csc.fi:/path/to/my/dest/dir``


Grouping configuration file
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The file ``config.grouping.yaml``, located in the script directory, contains a custom configuration for the aqua-web portal, describing how to group diagnostics.
It is used by ``make_contents.py``` to create the ``content.yaml`` files for each experiment. A custom config file can be passed with the ``-c`` option.


AWS credentials file
^^^^^^^^^^^^^^^^^^^^

The best way to store the credentials is by setting up a ``.aws/credentials`` file in the home directory.
As an example, the file should look like this:

.. code-block:: yaml

    [default]
    aws_access_key_id = 5RQ83GL0NJ4XXC72Y9VK
    aws_secret_access_key = DZW9SaKtIhRqYXXX3P2Sbv0te2Lb4R0kTxCsTEoc

The `access_key` and `secret_key` are the AWS credentials for the LUMI-O S3 bucket (the tokens above are fake).
As an alternative, set the environment variables ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` 
(the endpoint url ``https://lumidata.eu`` for LUMI-O is used by default).


.. _push_s3:

Pushing to LUMI-O or another S3 bucket
--------------------------------------

Tool to upload the contents of a directory or a single file to an S3 bucket.
The AWS credentials can be stored in the ``~/.aws/credentials`` file or in environment variables ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` or passed as arguments.

.. warning::

    This is a basic utility used by the other scripts (but you could also use it directly). 
    Do not use this to push the results of AQUA analysis to LUMI-O for aqua-web but rather 
    use ``push-analysis.py`` described above. 

Basic usage
^^^^^^^^^^^

.. code-block:: bash

    python push_s3.py <bucket_name> <source> [-d <destination>] [--aws_access_key_id <aws_access_key_id>] [--aws_secret_access_key <aws_secret_access_key>] [--endpoint_url <endpoint_url>]

Options
^^^^^^^

.. option:: <bucket_name>

    The name of the S3 bucket.

.. option:: <source>

    The path to the directory or file to upload.

.. option:: -d <destination>, --destination <destination>

    Optional destination path.

.. option:: -g, --get

    Flag to download a single file from the S3 bucket instead of uploading.
    When this option is used, the ``-d`` flag is meant as the path on the destination 
    bucket and the source is the name of the local file to write to.

.. option:: -k <aws_access_key_id>, --aws_access_key_id <aws_access_key_id>

    AWS access key ID.

.. option:: -s <aws_secret_access_key>, --aws_secret_access_key <aws_secret_access_key>

    AWS secret access key.

.. option:: --endpoint_url <endpoint_url>

    Custom endpoint URL for S3. Default is https://lumidata.eu.


.. _submit-aqua-web:

Multiple experiment analysis submitter
--------------------------------------

A wrapper containing to facilitate automatic submission of analysis of multiple experiments
in parallel and possible pushing to AQUA Explorer. This is used to implement overnight updates to AQUA Explorer.

Basic usage
^^^^^^^^^^^

.. code-block:: bash

    python ./submit-aqua-web.py EXPLIST

This will read a text file EXPLIST containing a list of models/experiments in the format

.. code-block:: rst

    # List of experiments to analyze in the format
    # catalog model exp [source]

    climatedt-phase1 IFS-NEMO  ssp370  lra-r100-monthly
    climatedt-phase1 IFS-NEMO historical-1990
    climatedt-phase1 ICON historical-1990
    nextgems4 IFS-FESOM ssp370

A sample file ``aqua-web.experiment.list`` is provided in the source code of AQUA.
Specifying the source is optional ('lra-r100-monthly' is the default).

Before using the script you will need to specify details for SLURM and other options
in the configuration file ``config.aqua-web.yaml``. This file is searched in the same directories as 
other AQUA configuration files or in the current directory as last resort.

It is possible to run the analysis on a single experiment specifying model, experiment and source
with the arguments ``-m``, ``-e`` and ``-s`` respectively.

If run without arguments, the script will run the analysis on the default 
experiments specified in the list.

Adding the ``-p`` or ``--push`` flag will push the results to the AQUA Explorer.

The extra ``-w``, ``-f`` and ``-n`` flags are used for maintenance and debugging 
and can be used to wipe the destination directory before pushing the images to aqua-web,
use a fresh temporary output directory for the analysis generation and use the
native (local) AQUA version respectively.

Options
^^^^^^^

.. option:: -c <config>, --config <config>

    The configuration file to use. Default is ``config.aqua-web.yaml``.

.. option:: -m <model>, --model <model>

    Specify a single model to be processed (alternative to specifying the experiment list).

.. option:: -e <exp>, --exp <exp>

    Experiment to be processed.

.. option:: -s <source>, --source <source>

    Source to be processed.

.. option:: -r, --serial

    Run in serial mode (only one core). This is passed to the ``aqua-analysis.py`` script.

.. option:: -x <max>, --max <max>

    Maximum number of jobs to submit without dependency.

.. option:: -t <template>, --template <template>

    Template jinja file for slurm job. Default is ``aqua-web.job.j2``.

.. option:: -d, --dry

    Perform a dry run for debugging (no job submission). Sets also ``loglevel`` to 'debug'.

.. option:: -l <loglevel>, --loglevel <loglevel>

    Logging level.

.. option:: -p, --push
    
    Flag to push to aqua-web. This uses the ``make_push_figures.py`` script.

.. option:: -w, --wipe
    
    Flag to wipe the destination directory before pushing the images to aqua-web.

.. option:: -f, --fresh
    
    Flag to use a fresh temporary output directory for the analysis generation.

.. option:: -n, --native
    
    Flag to use the native (local) AQUA version (default is the container version).

.. option:: -j, --jobname
    
    Alternative prefix for the job name (the default is specified in the config file)



