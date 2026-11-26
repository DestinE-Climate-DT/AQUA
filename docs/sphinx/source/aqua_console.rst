.. _aqua-console:

The AQUA console
================

What is the AQUA console?
-------------------------

The AQUA console is a command line interface that has two main purposes:

- A central access to install and to manage where the configuration (also fixes and grids) and catalog files are stored has been added.
- A tool for more complex operations:

    - DROP (see :ref:`aqua-drop` and :ref:`drop`)
    - FDB catalog generator (see :ref:`catalog_generator`).
    - Diagnostics wrapper for a complete experiment analysis (see :ref:`aqua_analysis`).

Here we give a brief overview of the features.
If you are a developer, you may want to read the :ref:`dev-notes` section.

The entry point for the console is the command ``aqua``.
Commands follow the grammar ``aqua <noun> <verb> [arguments]``, e.g. ``aqua catalog add ci``,
plus a few top-level commands acting on the whole installation or running a tool.

Top-level commands:

- :ref:`aqua-install`, :ref:`aqua-uninstall`, :ref:`aqua-update`, :ref:`aqua-list`
- :ref:`aqua-drop`, :ref:`aqua-analysis`, :ref:`aqua_push` and ``aqua catgen`` (see :ref:`catalog_generator`)

Nouns and their verbs:

.. list-table::
   :header-rows: 1

   * - Noun
     - Verbs
   * - :ref:`catalog <aqua-catalog>`
     - ``add <name> [-e path] [-r user/repo]``, ``remove <name>``, ``set <name>``, ``update <name|all>``, ``list``, ``avail [-r user/repo]``
   * - :ref:`project <aqua-project>`
     - ``set <name>``, ``list`` (``install <name> [-e path]``, ``update <name>``, ``remove <name>`` not available yet)
   * - :ref:`grids <aqua-grids>`
     - ``add <file> [-e]``, ``remove <file>``, ``list``, ``path <path>``, ``build ...``, ``deploy <name>``
   * - :ref:`fixes <aqua-fixes>`
     - ``add <file> [-e]``, ``remove <file>``, ``list``
   * - :ref:`styles <aqua-fixes>`
     - ``add <file> [-e]``, ``remove <file>``, ``list``

``aqua <noun>`` without a verb, or ``aqua <noun> -h``, prints the verbs of that noun.
Some commands of previous AQUA versions have been replaced (see :ref:`aqua-deprecated`).

The main command has some options listed below:

.. option:: --version

    To show the AQUA version.

.. option:: --path

    To show the path where the source code is installed.
    This is particularly useful if you're running a script that uses AQUA.

.. warning::
    Some of the CLI commands (see :ref:`cli`) are still relying on the existance
    of an environment variable ``AQUA`` pointing to the main AQUA folder.
    This is deprecated in favor of the new console command.

.. option:: --help, -h

    To show the help message.

It is possible to set the level of verbosity with two options:

.. option:: --verbose, -v

    It increases the verbosity level, setting it to INFO.

.. option:: --very-verbose, -vv

    It increases the verbosity level, setting it to DEBUG.

.. warning::
    In both cases the level of verbosity has to be specified before the subcommand.

.. _aqua-install:

aqua install
------------

With this command the configuration file and the default data models, grids and fixes are copied to the destination folder.
A bare ``aqua install`` installs AQUA core and, if ``aqua-diagnostics`` is installed in the current environment,
the ``diagnostics`` project: its project-scoped folders go in a subfolder named after it (e.g. ``analysis/diagnostics``)
and it is the active project, written in the ``project`` key of ``config-aqua.yaml``.
Other projects (see :ref:`aqua-console-plugins`) are added with :ref:`aqua project install <aqua-project-install>`.

By default, the destination folder will be ``$HOME/.aqua``.
It is possible to specify from where to copy and where to store.
It is also possible to ask for an editable installation, so that only links are created, ideal for developers,
which can keep their catalogs or fixes files under version control.

.. note::
    A configuration file is necessary to run AQUA.
    In the AQUA release a template is provided.
    Even if the ``aqua install`` is done in editable mode, this configuration file will be copied and customized in the destination folder.

Mandatory arguments are:

.. option:: machine-name

    The name of the machine where you are installing. **It is a mandatory argument.**
    Even if you are working on your local machine, always define it (even a random name would suffice!)
    Setting machine to `lumi`, `levante` or `MN5` is fundamental to use AQUA on these platforms.

Optional arguments are:

.. option:: -c, --core, --core <path/to/aqua-core/repo>

    If used without specifying a path, it will copy the configuration files from the AQUA core package installed in the current environment.
    If a path is specified, the folders containing the configuration files will be linked from the specified path,
    allowing developers to work on their local copy of AQUA core.

.. option:: -d, --diagnostics, --diagnostics <path/to/aqua-diagnostics/repo>

    If used without specifying a path, it will copy the configuration files from the AQUA diagnostics package installed in the current environment.
    If a path is specified, the folders containing the configuration files will be linked from the specified path,
    allowing developers to work on their local copy of AQUA diagnostics.

.. note::
    ``-c`` alone installs only core; ``-d`` alone adds diagnostics to an existing installation,
    as ``aqua project install diagnostics``. Without ``-c`` and ``-d``, both are installed in standard mode.

.. option:: --path, -p <path>

    The folder where the configuration file is copied to. Default is ``$HOME/.aqua``.
    If this option is used, the tool will ask the user if they want a link in the default folder ``$HOME/.aqua``.
    If this link is not created, the environment variable ``AQUA_CONFIG`` has to be set to the folder specified
    in order to expose it to AQUA.

.. _aqua-console-plugins:

Plugins and projects: the plugin contract
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``aqua install`` does not hardcode the list of components it knows how to install: besides ``core``,
any Python package can advertise itself as an installable AQUA component through a standard
`entry point <https://packaging.python.org/en/latest/specifications/entry-points/>`_
in the ``aqua.plugins`` group. This is how ``aqua-diagnostics`` is discovered, and it is the mechanism
a project package (e.g. ``aqua-claima``) uses to plug into ``aqua project install``, ``aqua update`` and ``aqua list``
without any change in AQUA core.

Every plugin is a **project**: its name is the entry point name (``diagnostics``, ``claima``, ...),
which is also the name of its subfolder in the installation, the value of the ``project`` key of
``config-aqua.yaml`` and the argument of ``aqua project install``.
AQUA core is not a plugin and not a project.

A plugin package needs two things:

1. An entry point declared in its ``pyproject.toml``:

   .. code-block:: toml

       [project.entry-points."aqua.plugins"]
       claima = "aqua.claima:get_install_dirs"

2. A ``get_install_dirs()`` function, importable from that module, returning a dictionary that follows
   the **plugin contract version 2**:

   .. code-block:: python

       def get_install_dirs():
           return {
               "contract": 2,
               "config": ["analysis", "collections", "tools"],
               "flat": [],
               "templates": [],
               "files": ["grids"],
           }

The folder names are relative to the ``aqua/<name>/config`` folder of the package
(``aqua/<name>/templates`` for ``templates``), and they are installed as follows:

.. list-table::
   :header-rows: 1

   * - Key
     - Installed as
     - Example (``aqua-diagnostics``)
   * - ``config``
     - project-scoped folders: ``<config>/<dir>/<name>``. The project-scoped folders are ``analysis``,
       ``collections`` and ``tools``.
     - ``analysis``, ``collections``, ``tools``
   * - ``flat``
     - whole folders owned by the project: ``<config>/<dir>``
     - ``definitions``
   * - ``templates``
     - whole folders: ``<config>/templates/<dir>``
     - ``collections``
   * - ``files``
     - single files merged into the flat ``<config>/<dir>`` (e.g. an extra grid file); the file names must not
       collide with those of AQUA core or of other projects
     - none

Rules:

- ``"contract": 2`` is mandatory. A plugin returning a dictionary without it (the version 1 contract of
  ``aqua-diagnostics`` < 0.27.0) is rejected and ``aqua install`` stops with an error asking to update that plugin.
- Missing keys default to an empty list and a single folder name is read as a one-item list.
  Unknown keys are rejected, with an error naming the package.
- In editable mode (``aqua project install <name> -e <path>``, ``<path>`` being the repository containing ``aqua/<name>/config``)
  the links are made at the ``<config>/<dir>/<name>`` level,
  so that different projects can be editable or not independently.
- ``get_install_dirs()`` may check the version of AQUA core and raise an exception if it is too old:
  the message is shown by ``aqua install``.
- The ``files`` of a project are copied (linked in editable mode) one by one into the flat folder:
  the installation stops if a file with the same name already exists. ``aqua project update`` and
  ``aqua project remove`` find them by listing the installed package again.
- A ``flat`` folder belongs to the project that declares it: the installation stops if it already exists,
  and ``aqua project remove`` deletes it. A project cannot declare as ``flat`` a folder of AQUA core or of the
  project layout (``catgen``, ``data_model``, ``fixes``, ``grids``, ``styles``, ``analysis``, ``collections``,
  ``tools``, ``templates``, ``catalogs``): to add files to these, use ``files``.
- ``templates`` folders are shared: ``aqua project remove`` keeps them.
- AQUA core installs ``catgen``, ``data_model``, ``fixes``, ``grids`` and ``styles`` flat: ``catgen`` is a core tool
  and it is not project-scoped.

A package that is registered but cannot be imported (e.g. installed metadata left over after an incomplete
removal) is skipped with a warning, rather than breaking the install.

.. _aqua-list:

aqua list
---------

This command lists some information about the current AQUA installation (path, machine, available components),
the installed projects with their installation mode (standard or editable) and package version,
marking the active one with ``*``, and the available catalogs in the installation folder.
It will show also if a catalog is installed in editable mode.

.. option:: -a, --all

    This will show also all the fixes, grids and data models installed

.. _aqua-uninstall:

aqua uninstall
--------------

This command removes the configuration and catalog files from the installation folder.
If the installation was done in editable mode, only the links will be removed.

.. note::
    If you need to reinstall aqua, the command ``aqua install`` will ask if you want to overwrite the existing files.

.. _aqua-update:

aqua update
-----------

This command will update the configuration files of AQUA core and of every project installed
(see :ref:`aqua-console-plugins`), except those installed in editable mode, which are left untouched
since they already point to the developer's own working copy.
Each project is refreshed as by ``aqua project update``, so other projects and user files are kept;
the extra files of the projects are copied again after core has replaced its flat folders.
An installation that predates project support is refused (see :ref:`aqua-pre-project`).
To update the catalogs, use :ref:`aqua-catalog-update`.

.. _aqua-check:

aqua check
----------

This command validates the current installation and exits with a non-zero code if a check fails:

- the ``project`` key is present in ``config-aqua.yaml`` (see :ref:`aqua-pre-project`);
- the active project is installed (a missing ``analysis``, ``collections`` or ``tools`` folder of the active
  project is only reported as a warning);
- the ``catalog.yaml`` of every installed catalog can be loaded.

.. _aqua-catalog:

aqua catalog
------------

The ``catalog`` noun manages the catalogs installed in the configuration folder.

.. note::
    The default catalogs are detached from the AQUA repository and
    they are available in the `Climate-DT-catalog <https://github.com/DestinE-Climate-DT/Climate-DT-catalog>`_ repository.
    It is possible to use other catalogs as well. The folder structure has to be the same
    as the default catalog.

.. warning::
    Adding, updating or listing catalogs from GitHub makes use of the GitHub API.
    These are limited to 60 requests per hour for unauthenticated users and it may easily hit the limit.
    If you encounter this issue, you can generate a personal access token and set it as an environment variable
    ``GITHUB_TOKEN``, together with a ``GITHUB_USER`` variable with your GitHub username.

.. _aqua-avail:

aqua catalog avail
^^^^^^^^^^^^^^^^^^

This simple command will print all the available catalogs to be installed from a repository.
By default this will be the `Climate-DT-catalog <https://github.com/DestinE-Climate-DT/Climate-DT-catalog>`_.

.. option:: -r, --repository <user/repo>

    It is possible to specify a different repository to explore.
    The format is ``user/repo``. For example, ``DestinE-Climate-DT/Climate-DT-catalog``.
    If this option is not specified, the default repository will be used.

.. _aqua-add:

aqua catalog add <catalog>
^^^^^^^^^^^^^^^^^^^^^^^^^^

This command adds a catalog to the list of available catalogs and sets it as the default one.
It will copy the catalog folder and files to the destination folder.
Multiple catalogs can be installed with multiple calls to ``aqua catalog add``.
By default the catalog will be downloaded from the external Climate-DT catalog repository,
if a matching catalog is found. It is possible to specify a different repository.
As shown below, it is also possible to specify a local path and install the catalog from there.
Similarly to the installation command, this will create symbolic links to the local folder,
ideal for developers.

.. option:: catalog

    The name of the catalog to be added.
    **It is a mandatory argument.**
    If the installation is done in editable mode, this name can be customized.

.. option:: --editable, -e <path>

    It installs the catalog based on the path given.
    It will create a symbolic link to the catalog folder.
    This is very recommended for developers. Please read the :ref:`dev-notes` section.

.. option:: --repository, -r <user/repo>

    It is possible to specify a different repository to explore.
    The format is ``user/repo``. For example, ``DestinE-Climate-DT/Climate-DT-catalog``.
    If this option is not specified, the default repository will be used.

.. _aqua-remove:

aqua catalog remove <catalog>
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It removes a catalog from the list of available catalogs.
This means that the catalog folder will be removed from the installation folder or the link will be deleted
if the catalog is installed in editable mode.

.. option:: catalog

    The name of the catalog to be removed.
    **It is a mandatory argument.**

.. _aqua-set:

aqua catalog set <catalog>
^^^^^^^^^^^^^^^^^^^^^^^^^^

This command sets the default main catalog to be used.
Since it is possible to have multiple catalogs installed and accessible at the same time,
if more than one catalog is present it will move the selected catalog to the top of the list.
The ``Reader`` behaviour will be then, if multiple triplets of ``model``, ``exp``, ``source`` are found in multiple
catalogs, to use the first one found in the selected catalog.

.. option:: catalog

    The name of the catalog to be set as default.
    **It is a mandatory argument.**

.. _aqua-catalog-update:

aqua catalog update <catalog>
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This command downloads the catalog again from the Climate-DT repository, overwriting the current installation.
If the catalog is installed in editable mode, this command will not work.

.. option:: catalog

    The name of the catalog to be updated, or ``all`` to update all the catalogs installed not in editable mode.
    **It is a mandatory argument.**

aqua catalog list
^^^^^^^^^^^^^^^^^

This command lists the installed catalogs, showing which ones are installed in editable mode.

.. _aqua-project:

aqua project
------------

The ``project`` noun selects the AQUA project, i.e. the package shipping configuration (and optionally diagnostics)
that AQUA uses: for example ``diagnostics``. Each project has its own subfolder in the project-scoped
configuration folders ``analysis``, ``collections`` and ``tools`` (e.g. ``$HOME/.aqua/analysis/diagnostics``),
while all the other folders (catalogs, grids, fixes, ``catgen``, ...) are shared.
The active project is stored in the ``project`` key of ``config-aqua.yaml`` and can be overridden
with the ``AQUA_PROJECT`` environment variable (see :ref:`aqua-project-context`).

.. _aqua-pre-project:

.. note::
    Installations that predate project support (no ``project`` key, flat layout such as ``$HOME/.aqua/analysis``)
    are not supported: the ``aqua project`` verbs, ``aqua update`` and the project folders stop with an error
    asking to upgrade the installation with ``aqua install``.

aqua project set <project>
^^^^^^^^^^^^^^^^^^^^^^^^^^

This command sets the active project in ``config-aqua.yaml``.
The project must be installed, i.e. at least one of ``analysis``, ``collections`` and ``tools``
must have a subfolder with its name. ``core`` is not a project and is refused.

.. option:: project

    The name of the project to be set as active.
    **It is a mandatory argument.**

aqua project list
^^^^^^^^^^^^^^^^^

This command lists the installed projects with their installation mode (standard or editable)
and package version, marking the active one with ``*``.

.. _aqua-project-install:

aqua project install <project> [-e path]
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This command adds a project to an existing installation and makes it the active one.
Its folders and extra files (see :ref:`aqua-console-plugins`) are copied from the package installed
in the current environment.

.. option:: -e, --editable <path>

    Link the folders and the extra files from the repository of the project at ``<path>``
    (the link is made at the ``<dir>/<project>`` level), as for ``aqua catalog add``.

aqua project update <project>
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This command copies again the folders and extra files of an installed project from its package:
shipped files are overwritten, user files with other names are kept, editable (linked) entries are skipped.

aqua project remove <project>
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This command removes the ``analysis``, ``collections`` and ``tools`` folders of a project, its ``flat`` folders
and its extra files (only the links, in editable mode); shared ``templates`` folders are kept.
If it was the active project, another installed project becomes active; if none is left, the ``project`` key
stays empty until a project is installed.

.. _aqua-fixes:

aqua {grids,fixes,styles} {add,remove,list}
-------------------------------------------

The ``grids``, ``fixes`` and ``styles`` nouns manage the files of the configuration folder with the same name:
grids and fixes YAML files, and matplotlib style files (``.mplstyle``) used by the AQUA graphics.
``add`` copies a file to the configuration folder, or creates a symbolic link if the editable mode is used.
This is useful if a new external file is created and needs to be added to the available ones.
A grids or fixes file is added only if it can be merged with the files already installed.
``remove`` removes a file (or its link), ``list`` lists the installed files.

.. option:: <file>

    The path of the file to be added, or the name of the file to be removed.
    This is a mandatory field for ``add`` and ``remove``.

.. option:: -e, --editable

    It will create a symbolic link to the file. Valid only for ``add``.

.. _aqua-grids:

aqua grids
----------

Besides ``add``, ``remove`` and ``list``, the ``grids`` noun has three verbs to set up the grids of a machine.

.. _aqua-grids-set:

aqua grids path <path>
^^^^^^^^^^^^^^^^^^^^^^

This subcommand sets in the configuration file the path to the grids, areas and weights folders.
If you need to deploy grids in this path, you can use the ``aqua grids deploy`` command (see :ref:`aqua-grids-deploy`).

.. option:: <path>

    The path to the grids, areas and weights folders.
    This is a mandatory field.
    The code will create the subfolders ``grids``, ``areas`` and ``weights`` in the specified path.

.. note::
    By default, it is not needed to set the path to the grids, areas and weights folders.
    AQUA will determine the path automatically based on the machine in the configuration file.
    This command is useful in new machines or if you don't have access to the default folders.
    In a locall installation for example, catalogs will not be able to find the grids, areas and weights
    unless this command is used to set the correct path.

.. _aqua-grids-deploy:

aqua grids deploy <grid-name>
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This subcommand is used to deploy a grid from a bucket to the local file system.
It is useful to set up AQUA in a new machine, where the grids are not available yet.
A match for the grid name (wildcard are supported) will be searched in the grids file in the configuration folder.
If a match is found, the grid will be deployed from the bucket to the local file system.

.. note::

    In order to avoid unwanted overriding of existing grids in a shared system,
    the command will work only if the user has set a custom path for the grids, areas and weights folders with the ``aqua grids path`` command.
    See the :ref:`aqua-grids-set` section for more details.

.. option:: <grid-name>

    The name of the grid to be deployed.
    This is a mandatory field.

Example usage:

    To deploy the atmospheric healpix10 grid, you can run the following command:

    .. code-block:: bash

        aqua grids deploy hpz10-nested

.. _aqua-grids-build:

aqua grids build
^^^^^^^^^^^^^^^^

This subcommand is used to build grids from sources. Given a specific ``Reader()`` source, it tries to build a grid file based on the data available.
This is available for regular, healpix, curvilinear grids. Partial support for unstructured grids is also available, while gaussian grids are not supported yet.
It also create the correspondent grid entry in the grid file in the ``config/grids`` folder.

The following options are available for ``aqua grids build``:

.. option:: -c, --config <file>

    YAML configuration file for the builder. If not specified, options must be provided via CLI.

.. option:: --catalog <catalog>

    Catalog identifying the source for the Reader() call.

.. option:: -m, --model <model>

    Model name (e.g. "IFS") for the Reader() call. **(Required)**

.. option:: -e, --exp <experiment>

    Experiment name for the Reader() call. **(Required)**

.. option:: -s, --source <source>

    Data source for the Reader() call. **(Required)**

.. option:: -l, --loglevel <level>

    Log level for the builder. Default is WARNING.

.. option:: --rebuild

    Rebuild the grid even if it already exists.

.. option:: --version <version>

    Version number for the grid file. Currently integer versioning is supported. Useful for multiple versions of the same grid.

.. option:: --outdir <directory>

    Output directory for the grid file. Default is the current directory.

.. option:: --original <resolution>

    Original resolution of the grid. Useful for masked grids which have been remapped to a different resolution.

.. option:: --modelname <name>

    Alternative name for the model for grid naming. Useful for coupled models sources.

.. option:: --gridname <name>

    Alternative name for the grid for grid naming. Required for Curvilinear and Unstructured grids, where the CDO grids cannot be guessed.

.. option:: --vert_coord <name>

    Name of the vertical coordinate in the dataset. Useful for 3D datasets where the vertical coordinate is not automatically detected by ``GridInspector``.

.. option:: --fix

    Apply fixes and data model to the original source before building the grid. Useful for models with very specific coordinates/dimensions.
    Suggested as default setting, disabled if issues arise.

.. option:: --datamodel

    Apply data model to the original source before building the grid. Useful for models with very specific coordinates/dimensions.
    Suggested as default setting, disabled if issues arise.

.. option:: --verify

    Verify the grid file after creation. This is done by calling CDO via ``smmregrid`` to check if the weights generation is valid.

.. option:: --yaml

    Create the grid entry in the grid file after building. This has to be added to catalog `source_grid_name` manually to be used by the Reader.
    Please keep in mind that this is not verified yet.

.. option:: --force_unstructured

    Force the grid detection to use unstructured grid type. Useful for datasets with ambiguous grid types (e.g. gaussian regular with inverted lon/lat dimensions).

.. option:: --reader_kwargs <json>

    Pass additional Reader parameters using a JSON object. For example, to select the Polytope engine and set time chunks:

    .. code-block:: bash

        aqua grids build --catalog CATALOG --model MODEL --exp EXP --source SOURCE \
            --reader_kwargs '{"engine": "polytope", "chunks": {"time": 12}}'

    Quote the entire object with single quotes in the shell and use double quotes for JSON keys and strings.
    JSON preserves numbers, booleans (``true`` and ``false``), lists, nested objects and ``null`` (Python ``None``).
    The selected Reader/backend determines which additional parameters it supports.

.. _aqua-drop:

aqua drop -c <config_file> <drop-options>
-----------------------------------------

This subcommand launch DROP (Data Reduction OPerator).
For full description of the DROP functionalities, please refer to the :ref:`drop` section.
In most of cases, it is better to embed this tool within a batch job.

.. _aqua-analysis:

aqua analysis <analysis-options>
--------------------------------

This subcommand launch the analysis tool, which is a flexible wrapper for the diagnostics.
It allows to run a set of diagnostics on a specific experiment.
For a complete description of the analysis tool, please refer to the :ref:`aqua_analysis` section.

.. _aqua-deprecated:

Commands changed in AQUA 1.3
----------------------------

The following commands of previous AQUA versions have been removed and give a usage error.
Please update your scripts to the new syntax.

.. list-table::
   :header-rows: 1

   * - Removed command
     - New command
   * - ``aqua add <catalog> [-e path] [-r user/repo]``
     - ``aqua catalog add <catalog> [-e path] [-r user/repo]``
   * - ``aqua remove <catalog>``
     - ``aqua catalog remove <catalog>``
   * - ``aqua set <catalog>``
     - ``aqua catalog set <catalog>``
   * - ``aqua avail [-r user/repo]``
     - ``aqua catalog avail [-r user/repo]``
   * - ``aqua update -c <catalog|all>``
     - ``aqua catalog update <catalog|all>``
   * - ``aqua grids set <path>``
     - ``aqua grids path <path>``
   * - ``aqua install <machine> --<project> [path]``
     - ``aqua project install <project> [-e path]``
