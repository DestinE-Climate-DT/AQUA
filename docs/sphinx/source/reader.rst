The Reader class
================

Here we describe the main component of the AQUA library, the ``Reader`` class.
It is part of the core components of AQUA, which is the code contained in the folder ``src/aqua``.
The core component consists of all the tools that are used to read, process and visualize data, not specific of a single diagnostic.
The other main core components are described in the :ref:`regrid`, :ref:`fixer` and :ref:`other-components` sections.
Some extra functionalities can be found in the :ref:`advanced-topics` section.

The Reader
^^^^^^^^^^

The ``Reader`` class provides AQUA access to data, developed to offer a centralized common data access point.
AQUA ``Reader`` can, in fact, access different file formats and data from the FDB or intake catalogs, 
and delivers xarray objects.
On top of data access, the ``Reader`` is also able to perform multiple operations on the data:
interpolation and regridding (see :ref:`regrid`), spatial and temporal averaging and metadata correction (see :ref:`fixer`).
These are described in the other sections of the documentation.
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

To check what is available in the catalog, we can use the ``inspect_catalog()`` function.
Three hierarchical layer structures (e.g AQUA triplet) describe each dataset.
At the top level, there are *models* (keyword ``model``) (e.g., ICON, IFS-NEMO, IFS-FESOM, etc.). 
Each model has different *experiments* (keyword ``exp``) and each experiment can have different *sources* (keyword ``source``).

Calling, for example:

.. code-block:: python

    from aqua import inspect_catalog
    inspect_catalog(model='CERES')

will return experiments available in the catalog for model CERES.

.. warning::
    The ``inspect_catalog()`` and the ``Reader`` are based on the catalog and AQUA path configuration.
    If you don't find a source you're expecting, please check these are correctly set (see :ref:`getting_started`).

If you want to have a complete overview of the sources available in the catalog, you can use the ``catalog()`` function.
This will return a list of all the sources available in the catalog, listed by model and experiment.

.. note::
    Both the ``inspect_catalog()`` and the ``catalog()`` functions will scan automatically the last catalog installed.
    If you want to target a specific catalog, you can pass the ``catalog_name`` keyword.

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

.. note::
    The basic call enables fixer, area and time average functionalities, but no regridding or streaming.
    To have a complete overview of the available options, please check the :doc:`api_reference`.

If some information about the data is needed, it is possible to use the ``info()`` method of the ``Reader`` class.

.. warning::
    Every ``Reader`` instance carries information about the grids and fixes of the retrieved data.
    If you're retrieving data from many sources, please instantiate a new ``Reader`` for each source.


Since version v0.10, multiple catalogs are supported. AQUA is designed to browse all the sources to match the triplet requested
by the users, but things can be speed up if we target a specific catalog. This can be done by passing the ``catalog`` kwargs. 

.. code-block:: python

    from aqua import Reader
    reader = Reader(model='IFS-NEMO', exp='historical-1990', source='lra-r100-monthly', catalog='climatedt-phase1')
    data = reader.retrieve()

Dask and Iterator access
^^^^^^^^^^^^^^^^^^^^^^^^

The standard usage of the ``Reader`` class will load metadata in memory and
make the data available for processing.
This is the standard behaviour of the ``Reader`` class, where ``xarray`` and ``dask``
capabilities are used to retrieve the data.

This allows to fully process also large datasets using dask lazy and parallel processing capabilities.
However, for specific testing or development needs,
the ``Reader`` class is also able to allow a streaming of data, 
where the data are loaded in chunks and processed step by step.
Please check the :ref:`iterators` section for more details.

.. note::
    Dask access to data is available also for FDB data.
    Since a specific intake driver has been developed, if you're adding new FDB sources to the catalog,
    we suggest to read the :ref:`FDB_dask` section.