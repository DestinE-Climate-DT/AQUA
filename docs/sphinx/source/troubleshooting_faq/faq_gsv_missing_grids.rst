I am getting an error MissingGridDefinitionPathError
====================================================

It is possible that, when trying to access data on FDB, you get the following error:

.. code-block::

    MissingGridDefinitionPathError: Environment variable `GRID_DEFINITION_PATH` is not set.
    This variable must point to the location of grid-defining netCDF files.
    Without this path Unstructured Grids cannot be decoded.

The GSV package that AQUA uses to access the data on FDB requires the environment variable ``GRID_DEFINITION_PATH`` to be set.
This variable must point to the location of grid-defining netCDF files.

To solve this issue, you can set the environment variable ``GRID_DEFINITION_PATH`` to the location of the grid-defining netCDF files.
On Lumi and Levante (and possible other HPCs supported on DestinE-Climate-DT) you can rely on shared grid definition files that are already available on the system.
Please refer to the corresponding installation guidelines for more information: :ref:`installation-lumi` or :ref:`installation-levante`.