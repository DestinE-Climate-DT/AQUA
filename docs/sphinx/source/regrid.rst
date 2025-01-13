.. _regrid:

Regrid and interpolation capabilities
-------------------------------------

AQUA provides functions to interpolate and regrid data to match the spatial resolution of different datasets. 
AQUA regridding functionalities are based on the external tool `smmregrid <https://github.com/jhardenberg/smmregrid>`_ which 
operates sparse matrix computation based on pre-computed weights.

Basic usage
^^^^^^^^^^^

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

The default regrid method is ``ycon`` which is a conservative regrid method.
If you want to use a different regrid method, you can specify it in the ``regrid_method`` keyword,
following the CDO convention.

Concept
^^^^^^^

The idea of the regridder is first to generate the weights for the interpolation and
then to use them for each regridding operation. 
The reader generates the regridding weights automatically (with CDO) if not already
existent and stored in a directory specified in the ``config/catalogs/<catalog-name>/machine.yaml`` file. 
This can have a `default` argument but can also specific for each machine you are working on. 

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

At the current stage, AQUA supports only regular lon-lat grids as target grids.
The available target grids are:

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
    We are planning to be able to support also irregular grids as target grids in the future (e.g. allowing to regrid everything to
    HealPix grids).

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