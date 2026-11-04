.. _polytope:

Climate-DT data access
======================

It is possible to access ClimateDT data available on the Databridge for the DestinE ClimateDT also remotely, from other machines,
using the polytope access.
AQUA can use the polytope engine. Here we describe the necessary steps to set it up.

Obtain the credentials
----------------------

To access Destination Earth ClimateDT data you will need to be registered on the `Destine Service Platform  <https://platform.destine.eu/>`_
and have requested "upgraded access" to the data (follow the link "Access policy upgrade" under your username at the top left corner of the page).

Once the upgraded access has been granted, you can create the key to access the data.
Follow the instructions in the `Polytope documentation <https://github.com/destination-earth-digital-twins/polytope-examples>`_
and the username and password which you defined for the Destine Service Platform to download the credentials into this file.

Once the key is generated, you can create the file ``~/.polytopeapirc`` in your home directory.

A sample ``~/.polytopeapirc`` file will look like this:

.. code-block:: text

    {
        "user_key" : "<your.token>"
    }

Use Polytope engine in AQUA
----------------------------

In order to use Polytope as data access engine in AQUA, you need to specify it when instantiating the `Reader` class.
To this end you will need to specify ``engine="polytope"`` when instantiating the `Reader` or permanently, adding
the argument ``engine: polytope`` as an additional argument in the intake catalog source entry in the corresponding yaml file, under `args:`.
``engine="polytope-gsv"`` is an alternative which at the moment is functionally identical to ``polytope``.
The two will be separated in the future.

.. code-block:: python

    reader = Reader(model="IFS-NEMO", exp="ssp370", source="hourly-hpz7-atm2d", engine="polytope")
    data = reader.retrieve(var='2t')

This allows accessing ClimateDT data on the Databridge also remotely from other machines.

Lumi Databridge and MN5 Databridge endpoints are supported.
Lumi Databridge is the default endpoint, but you can specify the MN5 Databridge endpoint by adding the argument
``machine='mn5'`` in the catalog source entry in the corresponding `main.yaml` file, under `metadata:`.

.. code-block:: yaml

    metadata:
      expid: "0001"
      forcing: historical
      start: '1990'
      machine: mn5

Use z3fdb engine in AQUA
------------------------

Alternatively, you can use the ``z3fdb`` engine for data access, which allows querying/opening the FDB database via a remote or local catalogue routing and wraps the FDB GRIB messages into a lazy zarr-like interface.

In order to use ``z3fdb`` as the data access engine in AQUA, you need to specify it when instantiating the ``Reader`` class.
To this end, you will need to specify ``engine="z3fdb"`` when instantiating the ``Reader`` or permanently, by adding the argument ``engine: z3fdb`` in the intake catalog source entry under ``args:``.

.. code-block:: python

    reader = Reader(model="IFS-NEMO", exp="ssp370", source="hourly-hpz7-atm2d", engine="z3fdb")
    data = reader.retrieve(var='2t')

If you want to use a different configuration file, you can pass its path using the ``config_fdb`` argument when instantiating the ``Reader`` class (e.g. ``Reader(..., config_fdb="/path/to/config.yaml")``).

For split HPC/bridge reads, ``hpc_expver`` overrides the request's ``expver``
only for HPC periods. For example, ``expver: "0001"`` with
``hpc_expver: a0h3`` uses ``a0h3`` outside the bridge interval and ``0001``
inside it, even when both periods share an FDB configuration. The catalog
request is preserved across reads.

.. _z3fdb-metadata:

Output metadata for z3fdb
^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    With ``engine="z3fdb"``, using ``fix=False`` produces a loss of variable
    metadata compared with GSV. Numeric parameters are named ``var167``, for
    example, and do not carry GRIB-derived attributes such as ``units``,
    ``long_name``, ``GRIB_paramId`` or ``GRIB_stepType``.

    Keep the default ``fix=True`` to recover the names, units and attributes
    supplied by AQUA's configured fixes. This does not restore every original
    GRIB message attribute. Dataset-level AQUA metadata and history are retained.

Availability discovery for z3fdb
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before routing requests, z3fdb resolves availability using the same helpers as GSV:

* ``metadata.fdb_info_file`` reads the workflow YAML. Its data dates and HPC
  experiment version override the catalog values; its bridge block, when present,
  supplies bridge dates and the bridge experiment version.
* ``data_start_date: auto`` or ``data_end_date: auto`` scans the configured local
  FDB directories using the DestinE schema. As in GSV, automatic end-date discovery
  excludes the newest directory, whose day may be incomplete. Automatic discovery
  is unsupported for ``timestyle: yearmonth``.
* ``bridge_start_date: stac`` or ``bridge_end_date: stac`` queries the LUMI bridge
  STAC service for both bridge boundaries.
* Bridge boundaries can also be text-file paths or ``complete``, with the same
  defaults for omitted boundaries as GSV.

``config_fdb`` overrides the catalog FDB location for both discovery and retrieval.
The resolved bounds and requested window are normalized to ``savefreq``; requests
outside the declared availability raise an error before retrieval. Discovery is
repeated on each read so workflow updates are picked up without changing the
catalog request or metadata. Field retrieval still uses the lazy z3fdb Zarr path
and does not require GSV.

Time settings for z3fdb
^^^^^^^^^^^^^^^^^^^^^^^

The ``z3fdb`` engine honors the same catalog time settings as GSV:

* ``timestyle: date`` varies ``date`` and ``time``, keeping ``step`` fixed.
* ``timestyle: step`` keeps the forecast reference ``date``/``time`` fixed
  at ``data_start_date`` and varies ``step`` in units of ``timestep``.
* ``timestyle: yearmonth`` varies ``year`` and ``month`` for monthly/yearly data.
* ``timestep`` defines the native time increment (default ``h``).
  ``savefreq`` selects the saved samples, taking the first native timestamp
  in each frequency interval, as GSV does.

For example, ``timestyle: step``, ``timestep: h`` and ``savefreq: 6h``
request steps 0, 6, 12, ... from the forecast reference. A subwindow or an
HPC/bridge boundary keeps that original reference. The optional initial
``step`` in the request sets the earliest available native step.

For daily or monthly ``date`` data, a single request ``time`` is preserved
in both the request and output coordinates: ``time: "1200"`` produces noon
labels. An omitted ``time``, YAML ``null`` or the legacy string ``"None"``
defaults to midnight. Other nonnumeric times are rejected. Output coordinates
come from the planned selection; a different number of returned time entries
raises an error.

Native timesteps must be positive whole minutes. Saved frequencies can be
fixed minute/hour/day intervals that are integer multiples of ``timestep``,
or ``MS``/``YS`` (also accepting ``M``/``Y``). Unsupported layouts, calendar
native timesteps and multiple initial steps raise an error. Legacy
``timeshift`` works only with GSV, not z3fdb. With ``engine="z3fdb"``, leave
it unset or false; a true value raises an error.

Grid coordinates for z3fdb
^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``z3fdb`` engine identifies its grid from one representative GRIB message
per HPC/bridge period, using the same FDB configuration as the data read.
It reads metadata with ecCodes and does not require GSV. Supported grid families:

* HEALPix: resolution and ring/nested ordering come from GRIB metadata.
  The spatial dimension is ``ncells``, matching GSV, without a synthetic cell index coordinate.
* Global regular latitude/longitude: dimensions come from GRIB metadata;
  south-to-north fields are flipped to a north-to-south layout.

Only these two grid families are supported. Unstructured, reduced Gaussian,
rotated and other GRIB grid types raise an error, as do grid-size mismatches.
Each decoded selection must contain a single grid. No external grid files or
``GRID_DEFINITION_PATH`` setting are required for z3fdb coordinate reconstruction.
The field data remains Dask-backed. Identifying the grid retrieves one GRIB
message eagerly, without decoding its field values in Python.

The catalog's ``source_grid_name`` remains available for AQUA regridding and
cell areas, but is not used to reconstruct z3fdb coordinates. No grid override
or additional catalog configuration is required.

.. _z3fdb-chunking:

Chunking Logic for z3fdb
^^^^^^^^^^^^^^^^^^^^^^^^

With ``engine="z3fdb"``, time chunks can be specified as a string/integer,
or with the ``time`` key in a dictionary:

* ``chunks="S"`` (the default) uses one saved timestamp per time chunk.
* ``chunks="D"`` or ``chunks={"time": "D"}`` groups samples by calendar day.
  Full hourly days have 24 samples, six-hourly days have four, and partial days
  have fewer. A day spanning HPC and bridge periods is combined into one output
  chunk. Data stays lazy; no averaging or eager field computation is performed.
* ``chunks="MS"`` groups samples by calendar month, respecting different month
  lengths and leap years. Monthly saved data has one sample per month.
* ``chunks="6h"`` groups samples into six-hour bins starting at midnight,
  06:00, 12:00 and 18:00. An hourly window starting at 03:00 has three samples
  in its first chunk. Both forms also work with ``chunks={"time": ...}``.
* ``chunks={"time": 24}`` groups a fixed number of 24 samples, independent of
  calendar-day boundaries. Fixed counts must be compatible with each native
  request part's time axis, rather than just the total selected window. Partial
  requests and storage splits can violate this restriction.

For example, hourly data requested from 1 January at 12:00 through 3 January at
06:00 (inclusive) has daily time-chunk lengths ``(12, 24, 7)`` with ``chunks="D"``.
No timestamps are added or dropped to fill the partial days. Daily saved data
keeps one timestamp per day; ``chunks="D"`` never changes the sampling frequency.

Frequency-based chunks do not require complete bins or an exactly divisible
total window. The backend uses regular read groups sized to divide every full
or partial time bin in each store. Xarray lazily groups them into the requested
bins, including bins split across HPC and bridge storage. Other positive pandas
resampling frequencies are parsed, but their native read groups may still be
incompatible with the request axes (see the warning below). Invalid or
nonpositive frequencies raise an error. Use ``MS``/``YS`` for month/year-start bins.

By default all selected levels share a chunk. Set ``vertical`` (or its alias
``level``) to a positive integer to group levels, for example:

.. code-block:: python

    Reader(..., engine="z3fdb", chunks={"time": "D", "vertical": 2})

A vertical size of one gives individual levels; a size equal to the selected
level count keeps them together.

.. warning::

    Fixed time counts must fit native request axes. Vertical sizes must divide
    the selected level count; otherwise the driver warns and uses single-level
    chunks. These native constraints come from `z3fdb's API
    <https://sites.ecmwf.int/docs/fdb/develop/z3fdb/api.html>`_.

    For time chunking, compatibility is required for each native MARS request
    part and each HPC/bridge period, not just the total selected window. Partial
    windows and storage splits can therefore make a fixed count invalid.
    Frequency-based chunks can encounter the same restriction: for example,
    50 hourly samples with ``chunks="10h"`` would produce five output chunks,
    but native request parts of 48 and two samples cannot use read groups of 10.

    Incompatible time chunking raises a backend error. The driver does not
    automatically retry with single-timestep output chunks. Choose compatible
    chunking explicitly; requesting a valid pandas frequency alone does not
    guarantee a valid native layout. Single-level fallback applies only to
    incompatible vertical sizes, as described above.

.. seealso::

   * The `ClimateDT external user guide
     <https://platform.destine.eu/services/documents-and-api/doc/?service_name=climate-dt-user-guide>`_
     on the DestinE platform, which includes a "Data and Access" section covering Polytope and other access methods.
   * `aqua_access.ipynb <https://github.com/DestinE-Climate-DT/climatedt-community-resources/blob/main/example_aqua/aqua_access.ipynb>`_
     in the
     `climatedt-community-resources <https://github.com/DestinE-Climate-DT/climatedt-community-resources>`_
     repository: example of ClimateDT data access with the AQUA ``Reader`` using the ``polytope`` engine.
