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

For this engine, a configuration file ``config-z3fdb.yaml`` is used (which is copied to your configuration folder during installation).
If you want to use a different configuration file, you can pass its path using the ``config_fdb`` argument when instantiating the ``Reader`` class (e.g. ``Reader(..., config_fdb="/path/to/config.yaml")``).

Chunking Logic for z3fdb
^^^^^^^^^^^^^^^^^^^^^^^^

When using ``engine="z3fdb"``, chunking is configured as follows:

* **Time direction**: Chunking is always by single time steps.
* **Level direction**: By default, level chunking is not performed.
* **Level chunking override**: If ``chunks`` is defined and it is a dictionary with a ``'level'`` key, then chunking is also done in the level direction.

For example, specifying ``chunks={"level": 1}`` provides single-level chunking (along with time chunking).
If you pass something like ``chunks={"level": 3}``, the integer value (e.g. 3) is ignored, and the level chunking is still performed as single-value.

.. seealso::

   * The `ClimateDT external user guide
     <https://platform.destine.eu/services/documents-and-api/doc/?service_name=climate-dt-user-guide>`_
     on the DestinE platform, which includes a "Data and Access" section covering Polytope and other access methods.
   * `aqua_access.ipynb <https://github.com/DestinE-Climate-DT/climatedt-community-resources/blob/main/example_aqua/aqua_access.ipynb>`_
     in the
     `climatedt-community-resources <https://github.com/DestinE-Climate-DT/climatedt-community-resources>`_
     repository: example of ClimateDT data access with the AQUA ``Reader`` using the ``polytope`` engine.
