.. _aqua-console:

Configuration and Catalogue manager
===================================

Since ``v0.8.2`` the possibility to manage where the configuration and catalogue files are stored has been added.
Here we give a brief overview of the features.
If you are a developer, you may want to read the :ref:`dev-notes` section.

The entry point for the console is the command ``aqua``.
It has the following subcommands:

- :ref:`aqua-init`
- :ref:`aqua-add`
- :ref:`aqua-list`
- :ref:`aqua-update`
- :ref:`aqua-remove`
- :ref:`aqua-uninstall`
- :ref:`aqua-fixes`
- :ref:`aqua-grids`
- :ref:`aqua-set`

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

.. _aqua-init:

aqua install
------------

With this command the configuration file and the default data models, grids and fixes are copied to the destination folder.
It is possible to specify from where to copy and where to store.
It is also possible to ask for an editable installation, so that only links are created, ideal for developers.

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

aqua add <catalogue>
--------------------

This command adds a catalogue to the list of available catalogues.
It will copy the catalogue file to the destination folder.
Also in this case it is possible to specify if symbolic links have to be created
and it is possible to install a catalogue normally not present in the package.

.. option:: catalog

    The name of the catalogue to be added.
    This is a mandatory field.

.. warning::
    At the moment only catalogues present in the package can be added without the editable mode.
    This will change in the future, but for now the only way to add a custom catalogue is in the editable mode.

.. option:: --editable, -e <path>

    It installs the catalogue based on the path given.
    It will create a symbolic link to the catalogue folder.
    This is very recommended for developers. Please read the :ref:`dev-notes` section.

.. _aqua-list:

aqua list
---------

This command lists the available catalogues in the installation folder.
It will show also if a catalogue is installed in editable mode.

.. _aqua-update:

aqua update <catalogue>
-----------------------

This command will check if there is a new version of the catalogue available and update it.

.. warning::
    This command is not yet implemented.

.. _aqua-remove:

aqua remove <catalogue>
-----------------------

It removes a catalogue from the list of available catalogues.
This means that the catalogue folder will be removed from the installation folder or the link will be deleted
if the catalogue is installed in editable mode.

.. _aqua-uninstall:

aqua uninstall
--------------

This command removes the configuration and catalogue files from the installation folder.
If the installation was done in editable mode, only the links will be removed.

.. note::
    If you need to reinstall aqua, the command ``aqua install`` will ask if you want to overwrite the existing files.

.. _aqua-fixes:

aqua fixes-add <fix-file>
-------------------------

This command adds a fix to the list of available fixes.
It will copy the fix file to the destination folder, or create a symbolic link if the editable mode is used.
This is useful if a new external fix is created and needs to be added to the list of available fixes.

.. option:: <fix-file>

    The path of the file to be added.
    This is a mandatory field.

.. option:: -e, --editable

    It will create a symbolic link to the fix folder.

.. _aqua-grids:

aqua grids-add <grid-file>
--------------------------

This command adds a grid to the list of available grids.
It will copy the grid file to the destination folder, or create a symbolic link if the editable mode is used.
This is useful if a new external grid is created and needs to be added to the list of available grids.

.. option:: <grid-file>

    The path of the file to be added.
    This is a mandatory field.

.. option:: -e, --editable

    It will create a symbolic link to the grid folder.

.. _aqua-set:

aqua set <catalogue>
--------------------

This command sets the default catalogue to be used.

.. warning::
    At the actual stage of development, the catalogue coincide with a machine.
    This command is then setting the machine name to be used in the configuration file.