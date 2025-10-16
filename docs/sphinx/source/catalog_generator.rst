.. _catalog_generator:

Catalog Generator
=====================

aqua catgen -c <config_file> -p <portfolio>
-------------------------------------------

This subcommand launch the source catalog entry generator, specifically for FDB sources part of the ClimateDT experiments.
This simplifies the process of adding new experiments to the catalog, based on the data-portfolio structure of the Destination Earth ClimateDT. 
It exploits the capabilities of the Jinja2 package to obtain a cleaner and more flexible code.


.. option:: -c <config>, --config <config>

    The configuration file to use. A ``config.tmpl`` is available to be copied and edited.

.. option:: -p <portfolio>, --portfolio <portfolio>  

    The data portfolio to be used. At moment ``production`` and ``reduced`` are supported.

.. option:: -l <loglevel>, --loglevel <loglevel>

    The logging level, following the python standards.
    
Basic usage
^^^^^^^^^^^

To add a new experiment to the catalog, follow these steps:

1. Clone the two repositories, `DestinE-ClimateDT-catalog <https://github.com/DestinE-Climate-DT/Climate-DT-catalog/tree/main>`_ and `data-portfolio <https://gitlab.earth.bsc.es/digital-twins/de_340-2/data-portfolio>`_, to your preferred location.
2. Create your own ``config.yaml`` file with the details of your simulation, including the paths of the cloned repositories. A template is provided in ``.aqua/templates/catgen``
3. Run the command ``aqua catgen -p production -c config.yaml``, where the ``-p`` argument can be ``production``, ``reduced`` or ``minimal``.
4. The catalog entry will be created in the appropriate location in the ``DestinE-ClimateDT-catalog`` folder as defined by the configuration file.

Configuration file
------------------

The configuration file ``config.tmpl`` contains the following keys:

- ``author``: the author of the experiment. This field is mandatory.
- ``maintainer``: the maintainer of the experiment. 
- ``machine``: the machine where the experiment is running. This field is mandatory. Supported machine names so far are ``lumi``, ``MN5``, ``levante``, ``hpc2020``.
- ``repos``: the paths to the data-portfolio and Climate-DT-catalog repositories.
- ``resolution``: the resolution of the experiment, which can be ``production``, ``develop``, ``lowres`` (if production portfolio), or ``intermediate`` (if reduced portfolio).
- ``catalog_dir``: the folder in the Climate-DT-catalog where the catalog entry will be stored.
- ``model``: model name
- ``exp``: experiment name
- ``fixer_name``: the fixer to use. Default is ``climatedt-phase2-production`` (or ``climatedt-phase2-reduced`` for reduced portfolio)

FDB Request definitions (to be filled by the user/workflow):

- ``activity``: Default is ``highresmip``
- ``experiment``:  Default is ``cont``
- ``generation``: the generation to use. Default is ``1``
- ``expver: 0001``:  Default is ``0001``
- ``num_of_realizations``: number of realizations in case of ensembles. Default is ``1``.
- ``default_realization``: in case of ensembles, the first realization to be loaded by default. Default is ``1``.

Info on the experiment:

- ``data_start_date``: data start date (format: 'YYYYMMDD')
- ``data_end_date``: data end date (format: 'YYYYMMDD')
- ``bridge_end_date``: if a transfer between the HPC and the data bridge is ongoing, this field should specify the last date when data was saved on the bridge. The default value is ``Null``, but it can be ``complete`` or a date.
- ``atm_grid``: the native atmospheric grid. This field is not mandatory. If not specified, the atm_grid will be automatically set according to what is set in ``config/catgen/matching_grids.yaml``.
- ``ocean_grid``: the ocean grid to use. This field is not mandatory. If not specified, the ocean_grid will be automatically set according to what is set in ``config/catgen/matching_grids.yaml``.
- ``description``: a description of the experiment. This field is not mandatory. If not specified, it will be automatically generated.

Info for the dashboard:
- ``menu``: the name that will appear in the dashboard. This field is not mandatory. If not specified, the ``exp`` will be used.
- ``note``: a note to complement experiment description, if needed. This field is not mandatory. 

Paths:
- ``fdb_home``: the path to the FDB home
- ``fdb_home_bridge``: the path to the FDB home bridge. Default is ``Null``.







