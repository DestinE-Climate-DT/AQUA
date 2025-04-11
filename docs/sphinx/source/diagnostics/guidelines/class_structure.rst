Core Diagnostic: ``Diagnostic``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``Diagnostic`` class serves as the foundation for all diagnostics.

It provides essential functionalities such as:

- A unified ``__init__`` method for consistent initialization. Extra argument for the ``__init__`` should be added only if strictly necessary.
- Initialization of the ``Reader`` class for data access.
- A standardized data retrieval method: ``retrieve()``.
- Built-in saving function ``save_netcdf()`` for NetCDF output.

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
If multiple models (e.g. model and observational dataset) are needed, two different instances of the diagnostic should be created.

Each diagnostic class must:

- Implement an ``__init__`` method that includes diagnostic-specific parameters.
- Use the ``retrieve()`` from ``Diagnostic`` for acquiring necessary data.
- If an operation is implemented in the ``Reader`` class, that method should be used (``self.reader.method()``).
- Implement a ``run()`` method or a clear order of methods to be called for the diagnostic evaluation.
- Specific substep should be called ``evaluate_<substep>()``.
- The computed results should be stored as class attributes.
- Implement a ``save_netcdf()`` method to save the results in NetCDF format, if an expansion of the ``Diagnostic.save_netcdf()`` method is needed.

Comparison Classes
^^^^^^^^^^^^^^^^^^

Each diagnostic module should also include a dedicated class for comparing results between different models.

.. code-block:: python

    class MyComparison():
        def __init__(self, *args, **kwargs):

In this case, it may not fit the usage of the ``Diagnostic`` class, as it does not support multiple models.
It should provide methods for dataset comparison and plotting.
It should as much as possible rely on the available plotting functions (See :ref:`graphic-tools`).

Command-Line Interface (CLI)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A CLI is available to streamline the execution of diagnostics and comparisons.
It should have a minimal mandatory set of arguments and be able to parse additional arguments if necessary (See :ref:`default_parser`).
It should also take a configuration file (YAML format) as input.

An example of the structure is the following:

.. code-block:: YAML

    datasets:
        - catalog: null # mandatory as null
          model: 'IFS-NEMO' # mandatory
          exp: 'historical-1990' # mandatory
          source: 'lra-r100-monthly' # mandatory
          regrid: null # if the diagnostic supports it
          freq: null # if the diagnostic supports it
        # Possible second/third/fourth datasets here, no present by default
        # - catalog: 'obs'
        #   model: 'ERA5'
        #   exp: 'era5'
        #   source: 'monthly'
        #   regrid: r100

    # Block if the diagnostics needs a reference dataset
    references:
        - catalog: 'obs' # mandatory
            model: 'ERA5' # mandatory
            exp: 'era5' # mandatory
            source: 'monthly' # mandatory
            regrid: null # if the diagnostic supports it
            freq: null # if the diagnostic supports it

    setup:
        loglevel: 'WARNING'

    output:
        outputdir: "./" # mandatory
        rebuild: true
        save_netcdf: true # mandatory if produced
        save_pdf: true # mandatory
        save_png: true # mandatory
        dpi: 300

    diagnostic:
        teleconnections:
            NAO:
            run: true
            months_window: 3
            full_year: false
            seasons: ['DJF', 'JJA']
            cbar_range: [-5, 5]