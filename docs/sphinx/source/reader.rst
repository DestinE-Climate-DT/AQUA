The Reader class
================

Here we describe the main component of the AQUA core, the ``Reader`` class.
The other main core components are described in the :ref:`regrid`, :ref:`fixer` and :ref:`other-components` sections.
Some extra functionalities can be found in the :ref:`advanced-topics` section.

The Reader
^^^^^^^^^^

The ``Reader`` class provides AQUA access to data, developed to offer a centralized common data access point.
AQUA ``Reader`` can, in fact, access different file formats and data from the FDB or intake catalogs, 
and delivers xarray objects.
On top of data access, the ``Reader`` is also able to perform multiple operations on the data:
interpolation and regridding (see :ref:`regrid`), spatial and temporal averaging and metadata correction (see :ref:`fixer`).
These operations can be both embedded in the ``Reader`` class or used by initializating separate components.
The ``Reader`` class is also able to perform parallel processing and to stream data,
since high-resolution data can be too large to be loaded in memory all at once
and it may be necessary to process data in chunks or even step by step.

Input and Output formats
^^^^^^^^^^^^^^^^^^^^^^^^

AQUA supports a variety of climate data file input formats:

- **NetCDF**
- **GRIB** files
- **Zarr**
- **FDB** GRIB
- **ARCO** files
- **Parquet**

After the data are retrieved, the ``Reader`` class returns an xarray object,
specifically an ``xarray.Dataset``, where only the metadata are loaded in memory.

.. note::
    Since metadata are the minimum information needed to load the data and prepare the processing,
    large sets of numerous NetCDF files are easy to read, but they may require
    to open a large amount of data to be able to check all the metadata.
    We then suggest, if low performance is experienced, to use the Zarr format
    on top of the NetCDF format, to `significantly improve the performance <https://ui.adsabs.harvard.edu/abs/2021AGUFMIN15A..08P/abstract>`_
    of the data access.

Catalog exploration
^^^^^^^^^^^^^^^^^^^^^

For an extensive catalog exploration, you can use the ``show_catalog_content()`` function.
This function scans catalog(s) by reading YAML files directly and displays the model/exp/source structure.

The simplest way to use it is:

.. code-block:: python

    from aqua import show_catalog_content
    results = show_catalog_content()

This will scan all available catalogs and output at the info level a dictionary with catalog names and nested values.
You can also filter by specific catalog(s), model, experiment, or source:

.. code-block:: python

    # Scan specific catalog(s)
    results = show_catalog_content(catalog=['ci','obs'])
    
    # Filter by model
    results = show_catalog_content(model='IFS-NEMO')

By default, ``show_catalog_content()`` displays sources in a compact 3-column format for quick overview.
You can enable the ``show_descriptions=True`` parameter to display each source on its own line with its description
from the catalog entry, providing additional information:

.. code-block:: python

    # Show catalog with detailed descriptions
    catalog_content = show_catalog_content(model='IFS-NEMO', show_descriptions=True)

.. note::
    The ``show_catalog_content()`` function is a convenience wrapper that handles ``ConfigPath`` initialization internally.
    If you need more control over the configuration, you can still use the method directly from the ``ConfigPath`` class:
    
    .. code-block:: python
    
        from aqua.core.configurer import ConfigPath
        config = ConfigPath(loglevel='info')
        results = config.show_catalog_content()

Reader basic usage
^^^^^^^^^^^^^^^^^^

Once you know which dataset you want to access, a call to the ``Reader`` can be done.
The basic call to the ``Reader`` is:

.. code-block:: python

    from aqua import Reader
    reader = Reader(model='IFS-NEMO', exp='historical-1990', source='lra-r100-monthly')
    data = reader.retrieve()

This will return a ``Reader`` object that can be used to access the data.
The ``retrieve()`` method will return an ``xarray.Dataset`` to be used for further processing.
The ``catalog``, differently from the ``model``, ``exp`` and ``source`` arguments, is optional.
However, if the triplet is not unique across catalogs, the ``Reader`` will guess the correct catalog,
so it is suggested to always specify it when possible.

The basic call enables fixer, area and time average functionalities, but no regridding or streaming.
To have a complete overview of the available options, please check the :doc:`api_reference`.

If some information about the data is needed, it is possible to use the ``info()`` method of the ``Reader`` class.

.. warning::
    Every ``Reader`` instance carries information about the grids and fixes of the retrieved data.
    If you're retrieving data from many sources, please instantiate a new ``Reader`` for each source.

.. note::
    The ``Reader`` class will try to convert the time coordinate to a Gregorian calendar if time is present,
    keeping the time units to microsecond precision in order to keep the axis as ``datetime64[us]``.

Reader and Intake parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sources can have additional parameters defined in the intake catalog.
For example the same experiment could have multiple ensemble members.
A parameter ``realization`` could be then defined in the catalog to distinguish between different ensemble members.

To pass these additional parameters to the ``Reader``, you can use the ``**kwargs`` functionality.
For example, to select a specific ensemble member, you can do:

.. code-block:: python

    from aqua import Reader
    reader = Reader(catalog='ci', model='CMCC', exp='historical', source='monthly-atm', realization=1)
    data = reader.retrieve()

See `Intake documentation <https://intake.readthedocs.io/en/stable/catalog.html#source-parameters>`_ for more details about Intake source parameters.

Dask and streaming capabilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The standard usage of the ``Reader`` class will load metadata in memory and
make the data available for processing.
This is the standard behaviour of the ``Reader`` class, where ``xarray`` and ``dask``
capabilities are used to retrieve the data.

This allows to fully process also large datasets using dask lazy and parallel processing capabilities.
However, for specific testing or development needs,
the ``Reader`` class is also able to allow a streaming of data, 
where the data are loaded in chunks and processed step by step.
Please check the :ref:`streaming` section for more details.

.. note::
    Dask access to data is available also for FDB data.
    Since a specific intake driver has been developed, if you're adding new FDB sources to the catalog,
    we suggest to read the :ref:`FDB_dask` section.