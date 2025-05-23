.. _regrid:

Regrid and interpolation capabilities
-------------------------------------

AQUA provides functions to interpolate and regrid data to match the spatial resolution of different datasets. 
AQUA regridding functionalities are based on the external tool `smmregrid <https://github.com/jhardenberg/smmregrid>`_ which 
operates sparse matrix computation based on pre-computed weights. They are wrapper within a `Regridder()` class
that can be used in a modular way to regrid data to a target grid, or seamlessy within the `Reader()`

Basic usage within the Reader()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is the reccomended usage. 
When the ``Reader`` is called, if regrid functionalities are needed, the target grid has to be specified
during the class initialization:

.. code-block:: python

    reader = Reader(model='IFS-NEMO', exp='historical-1990', source='hourly-native-atm2d',
                    regrid='r100')
    data = reader.retrieve()
    data_r = reader.regrid(data)

This will return an ``xarray.Dataset`` with the data lazily regridded to the target grid.
We can then use the ``data_r`` object for further processing and the data
will be loaded in memory only when necessary, allowing for further subsetting and processing.


Basic usage of the Regridder()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Alternatively - although not recommended - the regridding functionalities can be used in a standalone way.

When using the ``Regridder()`` in this way, users can provide a dataset (xr.Dataset or xr.DataArray) 
and then regrid it to a target grid. The class can also initialized with a dictionary containing a set of 
AQUA grids: however, in this case it might be preferrer to go through the ``Reader()``. 
The target grid has to be specified when generating the weights (which is a mandatory step). 
Please notice that the regridder will write the data provided to the disk to initialize the regridding process, 
so it might be a long operation if data are not sampled in the right way. 

.. code-block:: python

    regridder = Regridder(data=data.isel(time=0), loglevel='debug')
    regridder.weights(tgt_grid_name='r144x72', regrid_method="bil")
    data_r = regridder.regrid(data)

As in the previous case, this will return an ``xarray.Dataset`` with the data lazily regridded to the target grid.
The default regrid method is ``ycon`` which is a conservative regrid method.
If you want to use a different regrid method, you can specify it in the ``regrid_method`` keyword,
following the CDO convention.

The ``Regridder()`` class can also be used to retrieve the areas of the source and target grids.

.. code-block:: python

    src_area = regridder.areas()
    tgt_area = regridder.areas(tgt_grid_name='n64')

This - as before - will use the ``smmregrid`` engine based on CDO to compute the areas of the source and target grids.

Concept
^^^^^^^

The idea of the regridder is first to generate the weights for the interpolation and
then to use them for each regridding operation. 
The reader generates the regridding weights automatically (with CDO) if not already
existent and stored in a directory specified in the ``config/catalogs/<catalog-name>/machine.yaml`` file. 
This can have a ``default`` argument but can also specific for each machine you are working on. 

In other words, weights are computed externally by CDO (an operation that needs to be done only once) and 
then stored on the machine so that further operations are considerably fast. 

Such an approach has two main advantages:

1. All operations are done in memory so that no I/O is required, and the operations are faster than with CDO
2. Operations can be easily parallelized with Dask, bringing further speedup.

.. note::
    If you're using AQUA on a shared machine, please check if the regridding weights
    are already available.
    On the other hand, if you use a personal machine, you may want to follow the :ref:`new-machine-regrid` guide.

.. note::
    CDO requires the ``--force`` flag in order to be able to regrid to HealPix grids since version 2.4.0.
    This has been added to the HealPix grids definitions in the ``config/grids`` files.

.. note::
    In the long term, it will be possible to support also pre-computed weights from other interpolation software,
    such as `ESMF <https://earthsystemmodeling.org/>`_ or `MIR <https://github.com/ecmwf/mir>`_.

Available target grids
^^^^^^^^^^^^^^^^^^^^^^

.. note::

    From AQUA version v0.14, all CDO grids are supported natively by AQUA, so it is possible to target `r360x180` without the need to specify `r100`

The "predefined" target grids are:

.. code-block:: yaml

  r005s: r7200x3601
  r005: r7200x3600
  r010s: r3600x1801
  r010: r3600x1800
  r020s: r1800x901
  r020: r1800x900
  r025s: r1440x721
  r025: r1440x720
  r050s: r720x361
  r050: r720x360
  r100s: r360x181
  r100: r360x180
  r200s: r180x91
  r200: r180x90
  r250s: r144x73
  r250: r144x72

For example, ``r100`` is a regular grid at 1° resolution, ``r005`` at 0.05°, etc.
The list is available in the ``config/grids/default.yaml`` file.

.. note::
    The currently defined target grids follow the convention that for example a 1° grid (``r100``) has 360x180 points centered 
    in latitude between 89.5 and -89.5 degrees. Notice that an alternative grid definition with 360x181 points,
    centered between 90 and -90 degrees is sometimes used in the field and it is available in AQUA with the convention of adding
    an s to the corresponding convention defined above (e.g. ``r100s`` ).

.. note::
    Inside the ``config/grids`` directory, it is possible to define custom grids that can be used in the regridding process.
    Currently grids supported by CDO, which do not require extra CDO options, are supported and can be used directly as target grids.
    We are planning to be able to support also more complex irregular grids as target grids in the future (e.g. allowing to regrid everything to
    HealPix grids).

Oceanic grid files naming scheme
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The oceanic grid naming scheme is quite complex and here is reported for internal usage and future memory. 
Unfortunately, every small change in land sea mask requires a new oceanic grids since interpolation relies on pre-computed weights.

Elements Description
====================
- **model**: The model used, e.g., `fesom`, `icon`, `nemo`.
- **resolution**: The horizontal resolution or specific configuration of the model, e.g., `D3`, `NG5`, `R02B08`, `eORCA025`.
- **configuration**: Specific configuration details such as HealPix level or grid type, e.g., `hpz7`, `hpz10`.
- **grid_type**: Type of grid or nested grid structure, e.g., `nested`, `ring`.
- **domain**: The variable or data type in the file, e.g., `oce` (for 2d) or `oce_{vertical_coordinate}` for 3d data.
- **version**: The version of the file, indicated by `v1`, `v2`, etc. Missing version is used for single version files

Examples
========
1. `fesom-D3_hpz7_nested_oce.nc`
    - **Model**: FESOM
    - **Resolution**: D3
    - **Configuration**: hpz7
    - **Grid Type**: Nested
    - **Variable**: Ocean data
    - **Version**: Not specified

2. `icon-R02B08_hpz6_nested_oce_depth_full_v1.nc`
    - **Model**: ICON
    - **Resolution**: R02B08
    - **Configuration**: hpz6
    - **Grid Type**: Nested
    - **Variable**: 3d ocean data with depth as vertical coordinate and full levels
    - **Version**: v1

3. `nemo-eORCA12_hpz10_nested_oce_level.nc`
    - **Model**: NEMO
    - **Resolution**: eORCA12
    - **Configuration**: hpz10
    - **Grid Type**: Nested
    - **Variable**: 3d ocean data with level as vertical coordinate
    - **Version**: Not specified


Vertical interpolation
^^^^^^^^^^^^^^^^^^^^^^

Aside from the horizontal regridding, AQUA offers also the possibility to perform
a simple linear vertical interpolation building  on the capabilities of Xarray.
This is done with the ``vertinterp`` method of the ``Reader`` class.
This can of course be use in the combination of the ``regrid`` method so that it is possible to operate 
both interpolations in a few steps.
Users can also change the unit of the vertical coordinate.

.. code-block:: python

    reader = Reader(model="IFS", exp="tco2559-ng5", source="ICMU_atm3d", regrid='r100')
    data = reader.retrieve()
    field = data['u'].isel(time=slice(0,5)).aqua.regrid()
    interp = field.aqua.vertinterp(levels=[830, 835], units='hPa', method='linear')