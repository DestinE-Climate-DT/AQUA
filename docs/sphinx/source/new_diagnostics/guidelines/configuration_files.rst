Configuration Files
===================

Configuration files structure
-----------------------------

When developing a new diagnostic, the configuration file is a mandatory component needed to 
expose the settings and parameters that the diagnostic requires and which can be modified by the user.

In order to ensure consistency and ease of use, here we provide some guidelines for the structure of the configuration files.

Models and reference datasets
+++++++++++++++++++++++++++++

Models and reference dataset to be used in the diagnostic should be defined in the configuration file,
as:

.. code-block:: yaml

    datasets:
        - catalog: null # mandatory as null
          model: null # mandatory
          exp: null # mandatory
          source: null # mandatory
          regrid: null
          freq: null # if the diagnostic supports it
        # Possible second/third/fourth datasets here, no present by default
        # - catalog: null
        #   model: null
        #   exp: null
        #   source: null
        #   regrid: null
        #   freq: null # if the diagnostic supports it

    # Block if the diagnostics needs a reference dataset
    references:
        - catalog: 'obs' # mandatory
          model: 'ERA5' # mandatory
          exp: 'era5' # mandatory
          source: 'monthly' # mandatory
          regrid: null
          freq: null # if the diagnostic supports it

When possible, the datasets should be not limited to a single model, but the default should be.
Some diagnostics may not work with multiple references, it is better to specify it in the documentation
and in the configuration file.

Common parameters
+++++++++++++++++

The configuration file should also include a section for common parameters that are used across different diagnostics.

.. code-block:: yaml

    setup:
        loglevel: 'WARNING'

    output:
        outputdir: "./" # mandatory
        rebuild: true
        save_netcdf: true # mandatory if produced
        save_pdf: true # mandatory
        save_png: true # mandatory
        dpi: 300

These are mandatory parameters, others can be added as needed by the diagnostic.
If you feel that some parameters are missing, please open an issue on the AQUA GitHub repository.

Diagnostic specific parameters
++++++++++++++++++++++++++++++

In order to be able to eventually run multiple diagnostics with a composite configuration file,
a standard strycture for the diagnostic specific parameters is suggested.

.. code-block:: yaml

    diagnostics:
        diagnostic_name:
            run: true # mandatory, if false the diagnostic will not run
            diagnostic_name: diagnostic_name # mandatory, may override the diagnostic name
            variables: ['variable1', 'variable2'] # example for diagnostics running on multiple variables
            regions: ['region1', 'region2'] # example for diagnostics running on multiple regions
            parameter1: default_value1
            plot_params: # example for diagnostics with specific plot parameters
                param1: value1
                param2: value2
            # Other diagnostic specific parameters here

The block may vary depending on the diagnostic, but it should always include the ``run`` parameter
to indicate whether the diagnostic should be executed or not. This allows users to enable or disable
specific diagnostics without modifying the code.

The ``diagnostic_name`` is present to override the diagnostic name if needed.
Imagine for example to run the timeseries diagnostic in an analysis about precipitation.
This will allow the files to be named ``precipitation.timeseries.png`` instead of ``timeseries.timeseries.png``,
which would be less informative.

Configuration Files and AQUA console
------------------------------------

In the section :ref:`aqua-install`, the tool to expose configuration files for the diagnostic or
its CLI is described.
This section provides more details on how to update the code if you want to expose a new configuration file or
you are developing a new diagnostic.

The structure is defined in the ``aqua/cli/diagnostic_config.py`` file. Each diagnostic is associated 
with multiple configuration files and their corresponding source and target paths.

Example ``diagnostic_config.py`` structure:

.. code-block:: python

    diagnostic_config = {
        'global_biases': [
        {
            'config_file': 'config_global_biases.yaml',
            'source_path': 'config/diagnostics/global_biases',
            'target_path': 'diagnostics/global_biases/cli'
        },
        ]
    }

During the installation process, the configuration and CLI files for each diagnostics type are copied or linked 
from the source path to the target path specified in the ``diagnostic_config.py``.

.. note::
    This method will be update in the future in order to allow the copy or link of the entire ``config/diagnostics``
    folder, instead of individual files. This will simplify the process of adding new diagnostics.
    This also means that the source and target paths will not be defined in the
    ``diagnostic_config.py`` file, but will be assumed to be the same for all the files.

The folder structure should follow this pattern:

.. code-block:: text

    $HOME/.aqua/
        ├── diagnostics/
        │   ├── diagnostic_name/
        │   │   ├── definitions/
        │   │   │   └── definitions.yaml
        │   │   └── config_diagnostic_name.yaml

The ``diagnostics/`` folder contains a subfolder for each diagnostic, which in turn may contain a
``definitions/`` folder with possible files defining options for the diagnostic, such as available
regions for the diagnostic or default variable names to be used.
The file used to run the diagnostic are contained in the main diagnostic folder, and should be 
used by default when running the diagnostic individually or through the ``aqua-analysis`` CLI.

.. note::
    After the implementation of the diagnostic in the aqua console, be sure that the configuration files are
    correctly found in the installation folder when running the diagnostic and its CLI.