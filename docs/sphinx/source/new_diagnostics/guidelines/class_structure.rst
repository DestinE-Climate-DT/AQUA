Core Diagnostic: ``Diagnostic``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``Diagnostic`` class serves as the foundation for all diagnostics.

It provides essential functionalities such as:

- A unified ``__init__`` method for consistent initialization. Extra argument for the ``__init__`` should be added only if strictly necessary.
- Initialization of the ``Reader`` class for data access as ``self.reader`` attribute.
- A standardized data retrieval method: ``retrieve()``. This method stores the retrieved data in the ``self.data`` attribute.
  It also populates the ``self.catalog`` and ``self.realization`` attributes if empty by deducing them.
- Built-in saving function ``save_netcdf()`` for NetCDF output. This includes the possibility to generate a catalog entry to be used in further analyses.

Diagnostic Classes
^^^^^^^^^^^^^^^^^^

Each specific diagnostic inherits from ``Diagnostic`` and extends its capabilities.

This is done with the class inheritance structure, which allows for the creation of new diagnostics with minimal code duplication.

.. code-block:: python

    class MyDiagnostic(Diagnostic):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            # Additional initialization code

        def run(self):
            # Diagnostic-specific evaluation code

The purpose of this first class is to perform the data retrieval and the evaluations necessary on a single model.
At the end of the class execution, the results should be saved using the `save_netcdf()` method.
Additional metadata needed for plot or documentation of the results should be added to the xarray attributes,
using ``AQUA_`` as prefix of the metadata. Some metadata are added automatically when using the ``Reader``, such as:

- ``AQUA_catalog``
- ``AQUA_model``
- ``AQUA_exp``
- ``AQUA_source``
- ``AQUA_region`` when a region is selected

If multiple models (e.g. model and observational dataset) are needed, two different instances of the diagnostic should be created.

Each diagnostic class must:

- Implement an ``__init__`` method that includes diagnostic-specific parameters.
- Use the ``retrieve()`` from ``Diagnostic`` for acquiring necessary data.
- If an operation is implemented in the ``Reader`` class, that method should be used (``self.reader.method()``).
- Implement a ``run()`` method or a clear order of methods to be called for the diagnostic evaluation.
- Specific substep should be called ``evaluate_<substep>()``.
- The computed results should be stored as class attributes.
- Implement a ``save_netcdf()`` method to save the results in NetCDF format, if an expansion of the ``Diagnostic.save_netcdf()`` method is needed.

Comparison and Plot Classes
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Each diagnostic module should also include a dedicated class for eventually comparing results between different models and plot the final figure.

.. code-block:: python

    class MyDiagnosticPlot():
        def __init__(self, *args, **kwargs):

In this case, it may not fit the usage of the ``Diagnostic`` class, as it does not support multiple models.
It should provide methods for dataset comparison and plotting.
It should as much as possible rely on the available plotting functions (See :ref:`graphic-tools`).
Details about the plot should be deduced from the xarray attributes, if available.

Command-Line Interface (CLI)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A CLI is available to streamline the execution of diagnostics and comparisons.
It should have a minimal mandatory set of arguments and be able to parse additional arguments if necessary (See :ref:`diagnostics-cli-arguments`).

.. _configuration-file-guidelines:

Configuration file
^^^^^^^^^^^^^^^^^^

When developing a new diagnostic, the configuration file is a mandatory component needed to expose the settings and parameters that the diagnostic requires and which can be modified by the user.
In order to ensure consistency and ease of use, some guidelines for the structure of the configuration files are provided.
The generic blocks, which should be consistent among diagnostics, are described in :ref:`diagnostics-configuration-files`,
while an example of the specific block for a diagnostic is shown below.

.. code-block:: YAML

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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