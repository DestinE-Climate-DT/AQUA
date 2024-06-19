.. _aqua-console:

Configuration and catalog manager
===================================

Since ``v0.9`` the possibility to manage where the configuration and catalog files are stored has been added.
This is based on a command line interface which can also handle fixes and grids files. 
Here we give a brief overview of the features.
If you are a developer, you may want to read the :ref:`dev-notes` section.

The entry point for the console is the command ``aqua``.
It has the following subcommands:

- :ref:`aqua-install`
- :ref:`aqua-add`
- :ref:`aqua-list`
- :ref:`aqua-update`
- :ref:`aqua-remove`
- :ref:`aqua-set`
- :ref:`aqua-uninstall`
- :ref:`aqua-fixes`
- :ref:`aqua-grids`

To show the AQUA version, you can use the command:

.. code-block:: bash

    aqua --version

while a brief help is available with:

.. code-block:: bash

    aqua --help, -h

It is possible to set the level of verbosity with two options:

.. option:: --verbose, -v

    It increases the verbosity level, setting it to INFO.

.. option:: --very-verbose, -vv

    It increases the verbosity level, setting it to DEBUG.

In both cases the level of verbosity has to be specified before the command.

.. _aqua-install:

aqua install
------------

With this command the configuration file and the default data models, grids and fixes are copied to the destination folder.
By default, this will be ``$HOME/.aqua``. It is possible to specify from where to copy and where to store.
It is also possible to ask for an editable installation, so that only links are created, ideal for developers, 
which can keep their catalog or fixes files under version control.

.. option:: machine

    The name of the machine where you are installing. It is an optional argument to simplify analysis on specific system as levante or lumi.

.. option:: --path, -p <path>

    The folder where the configuration file is copied to. Default is ``$HOME/.aqua``.
    If this option is used, the tool will ask the user if they want a link in the default folder ``$HOME/.aqua``.
    If this link is not created, the environment variable ``AQUA_CONFIG`` has to be set to the folder specified.

.. option:: --editable, -e <path>

    It installs the configuration file from the path given.
    It will create a symbolic link to the configuration folder.
    This is very recommended for developers. Please read the :ref:`dev-notes` section.

.. warning::
    The editable mode requires a path to the ``AQUA/config`` folder, not to the main AQUA folder.

.. _aqua-add:

aqua add <catalog>
--------------------

This command adds a catalog to the list of available catalogs.
It will copy the catalog folder and files to the destination folder.
As before, it is possible to specify if symbolic links have to be created
and it is possible to install extra catalogs not present in the AQUA release.
Multiple catalogs can be installed with multiple calls to `aqua add`

.. option:: catalog

    The name of the catalog to be added.
    It can be also a path pointing to a specific folder where an AQUA compatible catalog can be found
    This is a mandatory field.

.. option:: --editable, -e <path>

    It installs the catalog based on the path given.
    It will create a symbolic link to the catalog folder.
    This is very recommended for developers. Please read the :ref:`dev-notes` section.

.. _aqua-list:

aqua list
---------

This command lists the available catalogs in the installation folder.
It will show also if a catalog is installed in editable mode.

.. option:: --all, -a

    This will show also all the fixes, grids and data models installed

.. _aqua-update:

aqua update <catalog>
-----------------------

This command will check if there is a new version of the catalog available and update it by overwriting the current installation.

.. warning::

    This will work smoothly only for default AQUA catalogs unless the full path is specified.

.. _aqua-remove:

aqua remove <catalog>
-----------------------

It removes a catalog from the list of available catalogs.
This means that the catalog folder will be removed from the installation folder or the link will be deleted
if the catalog is installed in editable mode.

.. _aqua-set:

aqua set <catalog>
--------------------

This command sets the default main catalog to be used. 

.. _aqua-uninstall:

aqua uninstall
--------------

This command removes the configuration and catalog files from the installation folder.
If the installation was done in editable mode, only the links will be removed.

.. note::
    If you need to reinstall aqua, the command ``aqua install`` will ask if you want to overwrite the existing files.

.. _aqua-fixes:

aqua fixes {add,remove} <fixes-file>
-------------------------------------

This submcommand is able to add or remove a fixes YAML file to the list of available installed fixes.
It will copy the fix file to the destination folder, or create a symbolic link if the editable mode is used.
This is useful if a new external fix is created and needs to be added to the list of available fixes.

.. option:: <fix-file>

    The path of the file to be added.
    This is a mandatory field.

.. option:: -e, --editable

    It will create a symbolic link to the fix folder. Valid only for ``aqua fixes add``

.. _aqua-grids:

aqua grids {add,remove} <grid-file>
-----------------------------------

This submcommand is able to add or remove a grids YAML file to the list of available installed grids.
It will copy the grids file to the destination folder, or create a symbolic link if the editable mode is used.
This is useful if new external grids are created and need to be added to the list of available grids.

.. option:: <grid-file>

    The path of the file to be added.
    This is a mandatory field.

.. option:: -e, --editable

    It will create a symbolic link to the grid folder. Valid only for ``aqua grids add``

.. _tropical_rainfall-console:

Tropical Rainfall enabling and setup
====================================

- :ref:`tropical_rainfall-enable`
- :ref:`tropical_rainfall-add_config`
- :ref:`tropical_rainfall-run_cli`
- :ref:`tropical_rainfall-test`

.. _tropical_rainfall-enable:

aqua enable tropical_rainfall
-----------------------------

This command enables the Tropical Rainfall package.

.. option:: --editable <source_path>

    The path to the Tropical Rainfall source directory.
    This is a mandatory field for editable mode.

.. _tropical_rainfall-add_config:

tropical_rainfall add_config [config_file_path]
-----------------------------------------------

This command adds a new configuration file to the Tropical Rainfall package.

.. option:: [config_file_path]

    The path to the configuration file to be added.
    This is not a mandatory field. By default, the command will use the default configuration 
    file located in the Tropical Rainfall package directory.

.. _tropical_rainfall-run_cli:

tropical_rainfall run_cli --config_file <config_file_path>
----------------------------------------------------------

This command runs the Tropical Rainfall CLI with the specified configuration file and number of processes.

.. option:: --config_file <config_file_path>

    The path to the CLI configuration file.

.. option:: --nproc <number_of_processes>

    The number of processes to use for analysis.
    If not provided, the default is 1.

.. _tropical_rainfall-test:

tropical_rainfall test [test_folder]
------------------------------------

This command runs the tests for the Tropical Rainfall package.

.. option:: [test_folder]

    The folder containing the tests.
    If not provided, the default `.aqua/tests` folder will be used.
