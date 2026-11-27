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
     - ``add <name> [-e path | -r user/repo]``, ``remove <name>``, ``set <name>``, ``update <name|all> [-r user/repo]``, ``list``, ``avail [-r user/repo]``
   * - :ref:`project <aqua-project>`
     - ``add <name> [-e path]``, ``remove <name>``, ``set <name>``, ``update <name>``, ``list``
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
the ``diagnostics`` project: its project folders go in a subfolder named after it (e.g. ``analysis/diagnostics``)
and it is the active project, written in the ``project`` key of ``config-aqua.yaml``.
Other projects (see :ref:`aqua-console-plugins`) are added with :ref:`aqua project add <aqua-project-add>`.

.. note::
    ``diagnostics`` is not a project like the others: it is the default configuration of AQUA, shipped with
    ``aqua-diagnostics``, used by :ref:`aqua analysis <aqua_analysis>` to run the analysis and evaluation of an experiment.
    It is installed and handled as a project only to share the same mechanism: other projects
    (e.g. ``claima``) add their own configuration on top of AQUA core in the same way.

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
    ``-c`` alone installs only core; ``-d`` alone adds diagnostics to an existing installation, with the same steps
    as ``aqua project add`` for the other projects. It is the only way to install diagnostics:
    ``aqua project add diagnostics`` is refused. Without ``-c`` and ``-d``, both are installed in standard mode.

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
a project package (e.g. ``aqua-claima``) uses to plug into ``aqua project add``, ``aqua update`` and ``aqua list``
without any change in AQUA core.

Every plugin is a **project**: its name is the entry point name (``diagnostics``, ``claima``, ...),
which is also the name of its subfolder in the installation, the value of the ``project`` key of
``config-aqua.yaml`` and the argument of ``aqua project add``.
AQUA core is not a plugin and not a project; ``diagnostics`` is the default project, installed by ``aqua install``.

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
               "project": ["analysis", "collections"],
               "exclusive": [],
               "shared": ["grids", "tools"],
           }

The three keys say **who owns** what is installed. The folder names are relative to the ``aqua/<name>/config``
folder of the package (``aqua/<name>/templates`` for the shared ``templates``), and they are installed as follows:

.. list-table::
   :header-rows: 1

   * - Key
     - Installed as
     - Owner
     - Example (``aqua-diagnostics``)
   * - ``project``
     - a subfolder named after the project: ``<config>/<dir>/<name>``. Only ``analysis`` and ``collections``.
     - the project, in its own subfolder
     - ``analysis``, ``collections``
   * - ``exclusive``
     - a whole folder: ``<config>/<dir>``
     - this project alone: no other package can write there
     - ``definitions``
   * - ``shared``
     - one entry (file or folder) at a time: ``<config>/<dir>/<entry>``. Only ``grids``, ``fixes``, ``styles``,
       ``tools`` and ``templates``.
     - each package owns its entries, which sit next to those of core and of the other projects
     - ``tools`` (``tools/ecmean``, ...), ``templates`` (``templates/collections``)

Rules:

- ``"contract": 2`` is mandatory. A plugin returning a dictionary without it (the version 1 contract of
  older ``aqua-diagnostics`` releases) is rejected and ``aqua install`` stops with an error asking to update that plugin.
- Missing keys default to an empty list and a single folder name is read as a one-item list.
  Unknown keys are rejected, with an error naming the package: among them ``config``, ``flat``, ``files`` and
  ``templates``, the names used before ``project``, ``exclusive`` and ``shared``.
- ``project`` accepts only ``analysis`` and ``collections``, and at least one of them is mandatory:
  these folders are what tells that a project is on disk. ``shared`` accepts only ``grids``, ``fixes``, ``styles``,
  ``tools`` and ``templates``. Any other folder name is rejected.
- In editable mode (``aqua project add <name> -e <path>``, ``<path>`` being the repository containing ``aqua/<name>/config``)
  the links are made at the ``<config>/<dir>/<name>`` level for the project folders, on the whole folder for the
  exclusive ones and on each entry for the shared ones,
  so that different projects can be editable or not independently.
- ``get_install_dirs()`` may check the version of AQUA core and raise an exception if it is too old:
  the message is shown by ``aqua install``.
- An ``exclusive`` folder belongs to the project that declares it: the installation stops if it already exists,
  ``aqua project update`` replaces it and ``aqua project remove`` deletes it. A project cannot declare as
  ``exclusive`` a folder of AQUA core, of the project layout or among the shared ones (``catgen``, ``data_model``,
  ``fixes``, ``grids``, ``styles``, ``tools``, ``templates``, ``analysis``, ``collections``, ``catalogs``):
  to add something to these, use ``shared``.
- The ``shared`` entries of a project are copied (linked in editable mode) one by one, at the first level of the
  shared folder: a file such as ``grids/claima-grid.yaml``, or a folder such as ``tools/ecmean``, which is then
  replaced as a whole by an update. Two packages cannot ship an entry with the same name: ``aqua project add`` stops
  if the entry already exists, ``aqua project update`` and ``aqua update`` if it belongs to core or to another
  installed project. So a project can add ``tools/<its tool>`` or ``templates/<its folder>``, but not a file inside
  the ``tools/ecmean`` or ``templates/collections`` of another package.
  ``aqua project update`` and ``aqua project remove`` find the entries by listing the installed package again:
  an entry that a new version of the package does not ship any more is left in the installation.
- ``tools`` holds the static files of a tool (e.g. ``tools/ecmean``) and ``templates`` the blank configuration files
  to start from (e.g. ``templates/collections`` of ``aqua-diagnostics``): both are read whatever the active project.

AQUA core is not a project, but its folders follow the same contract, with these exceptions:

- it has no ``project`` folders, it is not listed by the ``project`` key and it is installed, updated and removed
  only with the whole installation;
- its ``exclusive`` folders are ``catgen``, ``data_model``, ``fixes``, ``grids`` and ``styles``. The last three are
  at the same time the shared folders where the projects, and the user with ``aqua grids|fixes|styles add``,
  add their entries: ``aqua update`` replaces them, then every project copies its entries again;
- in the shared ``templates`` it installs only ``catgen``, ``drop`` and ``gridbuilder``, not every folder of its package;
- it ships nothing in ``tools``, which exists only once a project adds an entry to it.

A package that is registered but cannot be imported (e.g. installed metadata left over after an incomplete
removal) is skipped with a warning, rather than breaking the install.

.. _aqua-list:

aqua list
---------

This command lists some information about the current AQUA installation (path, machine, available components),
the installed projects with their installation mode (standard or editable) and package version,
and the available catalogs in the installation folder.
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
The installed projects are those listed by the ``project`` key of ``config-aqua.yaml``: a package available in the
environment but not installed with ``aqua project add`` is not touched.
Each project is refreshed as by ``aqua project update``, so the other projects are kept;
the shared entries of the projects are copied again after core has replaced its folders.
The rule is the same for core and for the projects: an installed folder belongs to its package and is replaced.
A file which the package does not ship is lost: one copied with e.g. ``aqua grids add``, one written by hand
in a project folder, or one that the new version of the package removes or renames.
Only links are kept: the shared entries of an editable project and the files added with ``-e``.
Before a folder is replaced, a warning lists what is about to be removed and is not copied again.
The checks are those of ``aqua project update``, done for every installed project before any change:
if the package of one of them is missing from the environment or breaks the plugin contract, nothing is updated.
A broken package which is not installed as a project does not stop the update.
An installation that predates project support is refused (see :ref:`aqua-pre-project`).
To update the catalogs, use :ref:`aqua-catalog-update`.

.. _aqua-check:

aqua check
----------

This command validates the current installation and exits with a non-zero code if a check fails:

- the ``project`` key is present in ``config-aqua.yaml`` (see :ref:`aqua-pre-project`);
- the active project is installed, i.e. listed by the ``project`` key;
- every project listed by the ``project`` key has at least one of its project folders
  (a single missing ``analysis`` or ``collections`` folder is only reported as a warning,
  as a project folder that does not belong to a listed project);
- every catalog listed by the ``catalog`` key has its folder, and its ``catalog.yaml`` can be loaded
  (a folder in ``catalogs`` which is not listed is only reported as a warning, and not loaded).

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
As for ``aqua project add``, a catalog already installed (listed by the ``catalog`` key) is refused:
use ``aqua catalog update``. A folder with the same name in ``catalogs`` which is not listed is refused too
and never deleted: remove it by hand first.

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
    It cannot be used together with ``--editable``.

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
As for ``aqua catalog update``, ``remove`` and ``list``, the catalog must be installed,
i.e. listed by the ``catalog`` key of ``config-aqua.yaml``: a folder copied by hand in ``catalogs`` is not.
If a listed catalog has no folder, these commands stop with an error: as for a project without its folders,
``aqua catalog remove`` followed by ``aqua catalog add`` restores it.
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
The active catalog does not change: use ``aqua catalog set`` for that.
As for ``aqua catalog add``, the downloaded catalog must load: if it does not, or the download fails
or is interrupted, the installed catalog is kept as it was.
A catalog installed in editable mode follows its repository: it is not touched, with a warning,
and ``aqua catalog update all`` goes on with the other catalogs.

.. option:: catalog

    The name of the catalog to be updated, or ``all`` to update all the catalogs installed not in editable mode.
    **It is a mandatory argument.**

.. option:: --repository, -r <user/repo>

    The repository to download the catalog from, as for ``aqua catalog add``.
    AQUA does not record where a catalog was added from: without this option the default repository is used,
    also for a catalog added with ``-r``.

aqua catalog list
^^^^^^^^^^^^^^^^^

This command lists the installed catalogs by name, as ``aqua project list`` does for the projects:
each one is ``standard`` or ``editable``, with the path it links to.

.. _aqua-project:

aqua project
------------

The ``project`` noun selects the AQUA project, i.e. the package shipping configuration (and optionally diagnostics)
that AQUA uses: for example ``claima``, or ``diagnostics``, the default configuration installed by ``aqua install``
(see :ref:`aqua-install`). Each project has its own subfolder in the project
configuration folders ``analysis`` and ``collections`` (e.g. ``$HOME/.aqua/analysis/diagnostics``),
while all the other folders (catalogs, grids, fixes, ``catgen``, ...) are shared.
The installed projects are listed by the ``project`` key of ``config-aqua.yaml``, as the catalogs are by the
``catalog`` key: the first one is the active project (a single project is written as a plain name).
Another installed project can be selected with the ``AQUA_PROJECT`` environment variable
(see :ref:`aqua-project-context`).

.. _aqua-pre-project:

.. note::
    Installations that predate project support (no ``project`` key, flat layout such as ``$HOME/.aqua/analysis``)
    are not supported: the ``aqua project`` verbs, ``aqua update`` and the project folders stop with an error
    asking to upgrade the installation with ``aqua install``.

aqua project set <project>
^^^^^^^^^^^^^^^^^^^^^^^^^^

This command sets the active project, moving it to the top of the ``project`` key of ``config-aqua.yaml``.
The project must be installed, i.e. listed by that key (see :ref:`aqua-project-add`).
As for the catalogs, ``aqua project set``, ``update`` and ``list``, and ``aqua update``, stop with an error
if a listed project has none of its folders: ``aqua project remove`` followed by ``aqua project add`` restores it.
``core`` is not a project and is refused.

.. option:: project

    The name of the project to be set as active.
    **It is a mandatory argument.**

aqua project list
^^^^^^^^^^^^^^^^^

This command lists the installed projects with their installation mode (standard or editable)
and package version, in alphabetical order. As for the catalogs, the active project is not marked:
it is the first one of the ``project`` key of ``config-aqua.yaml``.

.. _aqua-project-add:

aqua project add <project> [-e path]
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This command adds a project to an existing installation and makes it the active one:
once its files are installed, it is added at the top of the ``project`` key of ``config-aqua.yaml``.
Its folders and shared entries (see :ref:`aqua-console-plugins`) are copied from the package installed
in the current environment.
As for ``aqua catalog add``, a project already installed (listed by the ``project`` key) is refused:
use ``aqua project update``. A project folder with its name which is not listed is refused too and never deleted:
remove it by hand first.
If the project cannot be written in ``config-aqua.yaml``, what was installed is removed.
``diagnostics`` is refused: it is installed with ``aqua install -d``, also in an existing installation.
Once installed, ``aqua project set``, ``update``, ``remove`` and ``list`` handle it as any other project.

.. option:: -e, --editable <path>

    Link the folders and the shared entries from the repository of the project at ``<path>``
    (the link is made at the ``<dir>/<project>`` level), as for ``aqua catalog add``.

aqua project update <project>
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This command copies again the folders and shared entries of an installed project from its package:
the folders are replaced, as ``aqua update`` does for core, and editable (linked) entries are skipped.
A project installed entirely in editable mode follows its repository and is not touched.
There is no ``aqua project update all``: ``aqua update`` updates core and every installed project.

.. warning::
    The folders of a project belong to its package: a file written there by hand, or one that the new version of
    the package removes or renames, is deleted by the update, which lists it in a warning first.
    Keep your own files outside the project folders, or install the project in editable mode.

aqua project remove <project>
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This command removes the ``analysis`` and ``collections`` folders of a project, its ``exclusive`` folders
and its entries in the shared folders (only the links, in editable mode).
These are found from the project package: if it is not installed in the current environment, the command stops
without removing anything.
The project is removed from the ``project`` key: if it was the active project, the next installed one becomes active;
if none is left, the key stays empty until a project is installed.

.. _aqua-fixes:

aqua {grids,fixes,styles} {add,remove,list}
-------------------------------------------

The ``grids``, ``fixes`` and ``styles`` nouns manage the files of the configuration folder with the same name:
grids and fixes YAML files, and matplotlib style files (``.mplstyle``) used by the AQUA graphics.
``add`` copies a file to the configuration folder, or creates a symbolic link if the editable mode is used.
A copied file lasts until the next ``aqua update``, which replaces the folder and lists it in a warning.
Use the editable mode to keep a file across updates.
This is useful if a new external file is created and needs to be added to the available ones.
A grids or fixes file is added only if it can be merged with the files already installed.
A file which cannot be added (missing, not mergeable, not a ``.mplstyle`` for styles, or with a name already installed)
stops the command with an error and exit code 1.
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
     - ``aqua catalog add <catalog> [-e path | -r user/repo]``
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
     - ``aqua project add <project> [-e path]``
