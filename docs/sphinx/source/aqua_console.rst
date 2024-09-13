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
- :ref:`aqua-remove`
- :ref:`aqua-set`
- :ref:`aqua-uninstall`
- :ref:`aqua-list`
- :ref:`aqua-update`
- :ref:`aqua-fixes`
- :ref:`aqua-grids`
- :ref:`aqua-lra`

The main command has some options listed below:

.. option:: --version

    To show the AQUA version.

.. option:: --path

    To show the path where the source code is installed.
    This is particularly useful if you're running a script that uses AQUA.

.. warning::
    Many of the CLI commands (see :ref:`cli`) are still relying on the existance
    of an environment variable ``AQUA`` pointing to the main AQUA folder.
    This will be soon deprecated in favor of the new console command.

.. option:: --help, -h

    To show the help message.

It is possible to set the level of verbosity with two options:

.. option:: --verbose, -v

    It increases the verbosity level, setting it to INFO.

.. option:: --very-verbose, -vv

    It increases the verbosity level, setting it to DEBUG.

In both cases the level of verbosity has to be specified before the subcommand.

.. _aqua-install:

aqua install
------------

With this command the configuration file and the default data models, grids and fixes are copied to the destination folder.
By default, this will be ``$HOME/.aqua``. It is possible to specify from where to copy and where to store.
It is also possible to ask for an editable installation, so that only links are created, ideal for developers, 
which can keep their catalog or fixes files under version control.

.. note::
    Since version ``v0.10`` the configuration file provided in the AQUA release is a template.
    Even if the ``aqua install`` is done in editable mode, the configuration file will be copied to the destination folder.

Optional arguments are:

.. option:: machine-name

    The name of the machine where you are installing.
    It is an optional argument that will set the machine name of the configuration file.

.. warning::
    If not provided, the machine name will be left to ``auto``, where each time the 
    configuration file is loaded, the machine name will be set trying to guess the machine name.
    This can bring to some issues if the machine name is not correctly guessed.

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

.. _aqua-install-diagnostics:

aqua install of diagnostics
^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to the general configuration file, ``aqua install`` supports copying and linking configuration files 
for different diagnostics.
Each diagnostic has its own set of configuration files that are copied or linked to specific folders.

After running ``aqua install``, the configuration files for each diagnostic will be organized in the target directories 
specified in the ``diagnostic_config.py``. For example, the structure might look like this:

.. code-block:: text

    $HOME/.aqua/
        ├── diagnostics/
        │   ├── atmglobalmean/
        │   │   └── cli/
        │   │       └── atm_mean_bias_config.yaml
        │   ├── ecmean/
        │   │   ├── config/
        │   │   │   ├── ecmean_config_destine-v1-levante.yml
        │   │   │   ├── ecmean_config_destine-v1.yml
        │   │   │   ├── interface_AQUA_destine-v1.yml
        │   │   └── cli/
        │   │       └── config_ecmean_cli.yaml

This structure ensures that all configuration files are neatly organized and easily accessible for each diagnostic type.

.. _aqua-add:

aqua add <catalog>
--------------------

This command adds a catalog to the list of available catalogs.
It will copy the catalog folder and files to the destination folder.
As before, it is possible to specify if symbolic links have to be created
and it is possible to install extra catalogs not present in the AQUA release.

.. note::
    Since version ``v0.10`` the catalog is detached from the AQUA repository and
    it is available `here <https://github.com/DestinE-Climate-DT/Climate-DT-catalog>`_.

Multiple catalogs can be installed with multiple calls to `aqua add`.
By default the catalog will be downloaded from the external Climate-DT catalog repository,
if a matching catalog is found. As shown below, it is possible to specify a local path
and install the catalog from there.

.. option:: catalog

    The name of the catalog to be added.
    **It is a mandatory argument.**
    If the installation is done in editable mode, this name can be customized.

.. option:: --editable, -e <path>

    It installs the catalog based on the path given.
    It will create a symbolic link to the catalog folder.
    This is very recommended for developers. Please read the :ref:`dev-notes` section.

.. _aqua-remove:

aqua remove <catalog>
-----------------------

It removes a catalog from the list of available catalogs.
This means that the catalog folder will be removed from the installation folder or the link will be deleted
if the catalog is installed in editable mode.

.. option:: catalog

    The name of the catalog to be removed.
    **It is a mandatory argument.**

.. _aqua-set:

aqua set <catalog>
--------------------

This command sets the default main catalog to be used.
Since it is possible to have multiple catalogs installed and accessible at the same time, 
if more than one catalog is present it will move the selected catalog to the top of the list.
The ``Reader`` behaviour will be then, if multiple triplets of ``model``, ``exp``, ``source`` are found in multiple
catalogs, to use the first one found in the selected catalog.

.. option:: catalog

    The name of the catalog to be set as default.
    **It is a mandatory argument.**

.. _aqua-uninstall:

aqua uninstall
--------------

This command removes the configuration and catalog files from the installation folder.
If the installation was done in editable mode, only the links will be removed.

.. note::
    If you need to reinstall aqua, the command ``aqua install`` will ask if you want to overwrite the existing files.

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

    This will work only for catalogs installed from the Climate-DT repository.
    If the catalog is installed in editable mode, it will be enough to update the linked folders.


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

aqua lra -c <config_file> <lra-options>
-----------------------------------

This subcommand launch the LRA generation based on the LRA tool
For full description of the LRA generator functionalities, please refer to the :ref:`_lra` section.
In most of cases, it is better to embed this tool within a batch job

.. _fdb-catalog-generator:

aqua catgen -c <config_file> -p <portfolio>
--------------------------------------------------------

This subcommand launch the source catalog entry generator, specifically for FDB sources part of the ClimateDT experiments.
This simplifies the process of adding new experiments to the catalog, based on the data-portfolio structure of the Destination Earth ClimateDT. 
It exploits the capabilities of the Jinja2 package to obtain a cleaner and more flexible code.


.. option:: -c <config>, --config <config>

    The configuration file to use. A ``config.tmpl`` is available to be copied and edited.

.. option:: -p <portfolio>, --portfolio <portfolio>  

    The data portfolio to be used. At moment `production` and `reduced` are supported.

.. option:: -l <loglevel>, --loglevel <loglevel>

    The logging level, following the python standards
    
Basic usage
^^^^^^^^^^^

To add a new experiment to the catalog, follow these steps:

1. Clone the two repositories, `DestinE-ClimateDT-catalog <https://github.com/DestinE-Climate-DT/Climate-DT-catalog/tree/main>`_ and `data-portfolio <https://earth.bsc.es/gitlab/digital-twins/de_340-2/data-portfolio>`_, to your preferred location.
2. Create your own ``config.yaml`` file with the details of your simulation, including the paths of the cloned repositories. A template is provided in ``.aqua/templates/catgen``
3. Run the command ``aqua catgen -p production -c config.yaml``, where the ``-p`` argument can be either ``production`` or ``reduced`` to specify the Jinja2 template to be used.
4. The catalog entry will be created in the appropriate location in the DestinE-ClimateDT-catalog folder as defined by the configuration file.

