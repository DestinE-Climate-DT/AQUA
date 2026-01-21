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