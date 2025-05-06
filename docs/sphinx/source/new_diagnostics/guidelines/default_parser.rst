.. _default_parser:

Default parser
==============

All the state of the art diagnostics (See :ref:`stateoftheart_diagnostics` for a description) needs to have a
command line interface (CLI) to be able to run them together with the other diagnostics in the AQUA framework.

In order to facilitate the usage of different diagnostics, a set of common arguments that the CLI should be able
to parse has been defined. A utility parser function is present in the ``aqua.diagnostics.core`` module as ``template_parser``.
The function will take any parser and add the mandatory arguments.

These are:
    * ``--loglevel, -l``: Set logging level (str)
    * ``--catalog``: Catalog name (str) 
    * ``--model``: Model name (str)
    * ``--exp``: Experiment name (str)
    * ``--source``: Source name (str)
    * ``--config, -c``: YAML configuration file (str)
    * ``--nworkers, -n``: Number of workers (int)
    * ``--cluster``: Cluster address (str) 
    * ``--regrid``: Regrid data to the target grid (str)
    * ``--outputdir``: Output directory path (str)