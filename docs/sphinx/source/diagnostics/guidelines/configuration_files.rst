Configuration Files and AQUA console
====================================

In the section :ref:`aqua-install-diagnostics`, the tool to expose configuration files for the diagnostic or
its CLI is described.
This section provides more details on how to update the code if you want to expose a new configuration file or
you are developing a new diagnostic.

The structure is defined in the ``aqua/cli/diagnostic_config.py`` file. Each diagnostic is associated 
with multiple configuration files and their corresponding source and target paths.

Example ``diagnostic_config.py`` structure:

.. code-block:: python

    diagnostic_config = {
        'atmglobalmean': [
            {
                'config_file': 'atm_mean_bias_config.yaml',
                'source_path': 'diagnostics/atmglobalmean/cli/config',
                'target_path': 'diagnostics/atmglobalmean/cli'
            }
        ],
        'ecmean': [
            {
                'config_file': 'ecmean_config_destine-v1-levante.yml',
                'source_path': 'diagnostics/ecmean/config',
                'target_path': 'diagnostics/ecmean/config'
            },
            {
                'config_file': 'ecmean_config_destine-v1.yml',
                'source_path': 'diagnostics/ecmean/config',
                'target_path': 'diagnostics/ecmean/config'
            },
            {
                'config_file': 'interface_AQUA_destine-v1.yml',
                'source_path': 'diagnostics/ecmean/config',
                'target_path': 'diagnostics/ecmean/config'
            },
            {
                'config_file': 'config_ecmean_cli.yaml',
                'source_path': 'diagnostics/ecmean/cli',
                'target_path': 'diagnostics/ecmean/cli'
            }
        ]
    }

During the installation process, the configuration and CLI files for each diagnostics type are copied or linked 
from the source path to the target path specified in the ``diagnostic_config.py``.

The target folder structure should follow this pattern:

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

.. note::
    After the implementation of the diagnostic in the aqua console, be sure that the configuration files are
    correctly found in the installation folder when running the diagnostic and its CLI.