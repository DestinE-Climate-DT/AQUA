Automatic Standardized File Naming
==================================

Class Overview
--------------

The ``OutputSaver`` class is designed to manage output file naming conventions for scientific data.
It supports generating filenames for various file types (e.g., NetCDF, PDF, PNG) with metadata integration to enhance data management and traceability.
The class ensures consistent and descriptive filenames, facilitating better data organization and reproducibility.
It also allows for the generation of a catalog entry, to store the resulting NetCDF files in the AQUA catalog.

Attributes
----------

- **diagnostic** (*str*): Name of the diagnostic.
- **catalog** (*str* or *list*): Catalog name (e.g., ``lumi-phase2``).
- **model** (*str* or *list*): Model name (e.g., ``IFS-NEMO``).
- **exp** (*str* or *list*): Experiment name (e.g., ``historical``).
- **realization** (*str* or *list*): Realization name (default is ``r1``).
- **catalog_ref** (*str* or *list*, optional): Reference catalog name.
- **model_ref** (*str* or *list*, optional): Reference model name.
- **exp_ref** (*str* or *list*, optional): Reference experiment name.
- **outdir** (*str*, optional): Output directory where files will be saved. Defaults to the current directory.
- **loglevel** (*str*, optional): Logging level for the class's logger. Defaults to ``WARNING``.

.. note::
    The ``OutputSaver`` class automatically includes the current date and time when saving files as metadata.
    This ensures each file has a timestamp indicating when it was generated.
    The version of the AQUA package is also included in the metadata for traceability.

.. note::
    The ``OutputSaver`` should be initialized with the single-model info when storing NetCDF files,
    while with all the involved models and reference when storing plots.

Example Usage
-------------

Initializing the OutputSaver Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following example demonstrates how to initialize the ``OutputSaver`` class:

.. code-block:: python

    from aqua.diagnostics.core import OutputSaver

    # Initializing with the system-defined default catalog
    outputsaver = OutputSaver(diagnostic='dummy', 
                              catalog='climatedt-phase1', model='IFS-NEMO', exp='historical-1990', 
                              catalog_ref='obs', model_ref='ERA5', exp_ref='era5',
                              outdir='.', loglevel='DEBUG')

Generating a Filename
^^^^^^^^^^^^^^^^^^^^^

This example shows how to generate a filename with the 'mean' diagnostic product for the previously initialized class.

.. code-block:: python

    filename = outputsaver.generate_name(diagnostic_product='mean')
    # Output: 'dummy.mean.climatedt-phase1.IFS-NEMO.historical-1990.obs.ERA5.era5'

.. note::
    The generated filename includes the diagnostic name, diagnostic product, catalog, model, and experiment.
    If the reference dataset is specified in the ``OutputSaver`` constructor, it will also be included in the filename.
    Alternatively, the catalog-model-experiment triplets for the main and reference datasets 
    can be specified directly in the ``generate_name`` method.

Generating a Filename with Extra Keys
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The user can also specify extra parameters that will be added to the filename, such as ``variable``, ``region``, ``period``, ``pressure level``, etc.
Extra keys are not mandatory, but if specified, they will be appended to the filename.
They are entirely flexible and can include any relevant information the user wishes to capture.

.. code-block:: python

    extra_keys = {'variable': '2t', 'region': 'global', 'period': '1990-2000'}

    filename = outputsaver.generate_name(diagnostic_product='mean', 
                                         extra_keys=extra_keys)

    # Output: 'dummy.mean.climatedt-phase1.IFS-NEMO.historical-1990.obs.ERA5.era5.2t.global.1990-2000'

Saving a NetCDF File with Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Here is an example of saving a NetCDF file with metadata. The metadata includes the title, author, and description of the file.

.. code-block:: python

    import xarray as xr

    # Example dataset
    dataset = xr.Dataset()

    # Define metadata for the NetCDF file
    metadata = {
        'title': 'Testing the saving of NetCDF files',
        'author': 'OutputSaver',
        'description': 'Demonstrating netCDF Metadata Addition'
    }

    outputsaver.save_netcdf(dataset, 'test', extra_keys=extra_keys, metadata=metadata)

.. note::
    If the ``history`` metadata field is provided, the ``OutputSaver`` class will append
    the current message to the existing history.

Saving a PDF or PNG Plot with Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example demonstrates saving a PDF and PNG plot with metadata. The metadata includes the title, author, subject, and keywords of the file.

.. code-block:: python

    import matplotlib.pyplot as plt

    # Create a sample figure
    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])

    # Define metadata for the PDF file
    metadata = {
        '/Title': 'Sample PDF',
        '/Author': 'OutputSaver',
        '/Subject': 'Demonstrating PDF Metadata Addition',
        '/Keywords': 'PDF, OutputSaver, Metadata'
    }

    # Save the PDF and PNG with metadata
    outputsaver.save_pdf(fig, 'test', extra_keys=extra_keys, metadata=metadata)
    outputsaver.save_png(fig, 'test', extra_keys=extra_keys, metadata=metadata, dpi=300)

.. note::
    We suggest using the metadata field ``/Caption`` to store the plot description.
    This is currently used by the AQUA dashboard to generate plot descriptions.

Opening a PDF File and Displaying Metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To open a PDF file and display its metadata:

.. code-block:: python

    from aqua.core.util import open_image

    open_image("/path/to/my/file/dummy.mean.climatedt-phase1.IFS-NEMO.historical-1990.obs.ERA5.era5.pdf")

Generating a Filename for Multimodel or Multireference Comparisons
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In some diagnostics, multimodel or multireference comparisons may be required.
In this case, the user can specify a list of catalog-model-experiment triplets for the main and/or the reference dataset.
To avoid overly long filenames, the keyword ``multimodel`` or ``multiref`` will be used to indicate that the dataset is a list.
Complete information about the datasets is preserved in the output file's metadata.

.. code-block:: python

    outputsaver = OutputSaver(diagnostic='dummy',
                              catalog=['climatedt-phase1', 'climatedt-phase1'],
                              model=['IFS-NEMO', 'ICON'],
                              exp=['historical-1990', 'historical-1990'],
                              catalog_ref='obs', model_ref='ERA5', exp_ref='era5',
                              outdir='.', loglevel='DEBUG')

    filename = outputsaver.generate_name(diagnostic_product='test')
    # Output: 'dummy.test.multimodel.obs.ERA5.era5'

Creating a catalog entry
^^^^^^^^^^^^^^^^^^^^^^^^

The ``save_netcdf`` method allows for the creation of a catalog entry for the saved NetCDF file.
This entry is created in the same experiment file of the input dataset.

In order to enable the catalog entry creation, the method should include as arguments:

- ``create_catalog_entry`` (*bool*, optional): Set as ``True`` to create a catalog entry for the saved NetCDF file.
- ``dict_catalog_entry`` (*dict*, optional): A dictionary containing the catalog entry information.
  The catalog can specify a ``jinjalist`` (by default ``['freq', 'stat', 'region', 'realization']``) and a ``wildcardlist`` (by default ``['var']``).
  For each matching ``extra_keys``, the code will create an intake parameter for the first list and a wildcard for the second.
  This allows for catalog entry which can access the relevant NetCDF files when used with the ``Reader``.