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
    The ``show_catalog_content()`` function is a convenience wrapper that handles ``ConfigCatalog`` initialization internally.
    If you need more control over the configuration, you can still use the method directly from the ``ConfigCatalog`` class:

    .. code-block:: python

        from aqua.core.configurer import ConfigCatalog
        config = ConfigCatalog(loglevel='info')
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

The basic call enables fixer, area and time average functionalities, but no regridding.
To have a complete overview of the available options, please check the :doc:`api_reference`.
For STAC-backed sources, see :ref:`stac-based-sources` for the Intake catalog
layout and examples of selecting collections, items and assets.
The names ``collection``, ``item`` and ``asset`` are aliases for ``model``,
``exp`` and ``source``, respectively.

If some information about the data is needed, it is possible to use the ``info()`` method of the ``Reader`` class.

The ``retrieve()`` method accepts further arguments to select only specific variables or vertical levels as in this example:

.. code-block:: python

    from aqua import Reader
    reader = Reader(model="ERA5", exp="era5-hpz3", source="monthly")
    data = reader.retrieve(var=["t", "2t"], level=[50000, 70000])

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


Reader xarray access
^^^^^^^^^^^^^^^^^^^^

The ``Reader`` class is designed to provide a main inteface via intake, but can also be used to access data directly via xarray.
The ``Reader`` class can be initialized with a ``path`` to a dataset, and the ``retrieve()`` method will return an xarray object.
This is useful when the data is not available in the intake catalog, but is available in a local path, and the user want to inspect them quickly.

.. code-block:: python

    from aqua import Reader
    reader = Reader(path="/path/to/data.nc", areas=False)
    data = reader.retrieve()

``model``, ``exp``, ``source`` and ``catalog`` can be given together with ``path`` as labels:
they are stored in the ``AQUA_model``, ``AQUA_exp``, ``AQUA_source`` and ``AQUA_catalog`` attributes,
used for example in plot titles.

.. code-block:: python

    reader = Reader(path="/path/to/data.nc", model="IFS-NEMO", exp="historical", source="lra-r100-monthly", areas=False)


.. note::
    The xarray access is experimental and may not support all the functionalities of the intake backed.
    For example, regridding, fixer and other functionalities are not available, since they are based on information available in the intake catalog.

Reader stac access
^^^^^^^^^^^^^^^^^^

The ``Reader`` class can also be used to access data from a STAC object (catalog, collection or item) with ``pystac``.
The reader is initialized with a ``url`` pointing to the STAC JSON file. The asset to open is selected in one of two ways:

- with ``collection``, ``item`` and ``asset`` (synonyms of ``model``, ``exp`` and ``source``), giving only the levels
  below the ``url``: e.g. ``collection`` and ``item`` for a catalog, ``item`` for a collection, nothing for an item;
- with ``stac_kwargs``, for any catalog depth: just the asset key (a string) for a single item, or a nested dictionary
  with one entry per level, made of the child collection and item ids and ending with the asset key.
  In this case ``collection``, ``item`` and ``asset`` are only labels.

Without ``stac_kwargs``, ``collection``, ``item`` and ``asset`` always select the STAC objects: to label the data
(e.g. ``model="ERA5"`` for an item whose collection is ``era5``), pass ``stac_kwargs`` as well, for instance just the asset key.

If no asset is given, the ``data`` asset (or the only asset with role ``data``, or the first asset) is opened,
as in the Intake STAC driver. An asset can also be selected by its role.

The catalog, collection, item and asset ids are stored in the ``AQUA_catalog``, ``AQUA_model``, ``AQUA_exp`` and ``AQUA_source``
attributes of the data, unless the user provides other labels. ``AQUA_catalog`` is set only when ``url`` points to a catalog.

The asset is opened lazily with xarray, using the ``xarray:open_kwargs`` and ``xarray:storage_options`` defined in the asset, if any.
The ``chunks`` given to the ``Reader`` override the asset ones (default ``"auto"``). Entries of ``xarray:open_kwargs`` not accepted by
``xarray.open_dataset`` (e.g. ``url`` or ``metadata`` in the DKRZ EERIE catalogs) are ignored with a warning. Storage options are
taken only from ``xarray:storage_options``, and are ignored with a warning when the engine does not accept them (e.g. ``netcdf4``).
The STAC tree is read once per ``Reader``: create a new ``Reader`` to see changes in the catalog.
Asset ``href`` paths relative to the STAC file are resolved, so that local catalogs work as well.
This below is a working example of an item, building on the DKRZ EERIE cloud STAC catalog.

.. code-block:: python

    from aqua import Reader
    reader = Reader(
        url="https://eerie.cloud.dkrz.de/datasets/HadGEM3-GC5E-HH.historical.v20250409.gr1.Amon/stac",
        stac_kwargs="eerie-cloud", areas=False,
    )
    data = reader.retrieve()

A nested catalog is accessed by following the structure of the catalog down to the asset,
here a collection, one of its items and the asset of the item (this example requires ``gribscan``):

.. code-block:: python

    from aqua import Reader
    reader = Reader(
        url="https://km-scale-cloud.dkrz.de/stac-catalog-all.json",
        stac_kwargs={"km-scale-cloud-dkrz-era5-collection": {"era5-dkrz.surface_analysis_monthly": "eerie-cloud"}},
        areas=False,
    )
    data = reader.retrieve()

The AQUA test suite generates a local STAC catalog from the test data (see ``tests/stac_catalogue/README.md``),
which can be used to try the feature offline:

.. code-block:: python

    from aqua import Reader
    reader = Reader(
        url="tests/stac_catalogue/catalog.json",
        stac_kwargs={"ifs": {"ifs-short-ifs2d-tco79": "data"}},
        areas=False,
    )
    data = reader.retrieve()

    # same asset, selected with collection and item (default asset 'data')
    reader = Reader(url="tests/stac_catalogue/catalog.json", collection="ifs", item="ifs-short-ifs2d-tco79", areas=False)

.. note::
    The stac access is experimental and may not support all the functionalities of the intake backed. it might break if the catalog structure is not as expected.
    For example, regridding, fixer and other functionalities are not available, since they are based on information available in the intake catalog.
