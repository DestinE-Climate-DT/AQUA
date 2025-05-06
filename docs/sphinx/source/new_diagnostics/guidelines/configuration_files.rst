Configuration Files and AQUA console
====================================

In the section :ref:`aqua-install`, the tool to expose configuration files for the diagnostic or
its CLI is described.
This section provides more details on how to update the code if you want to expose a new configuration file or
you are developing a new diagnostic.

The structure is defined in the ``aqua/cli/diagnostic_config.py`` file. Each diagnostic is associated 
with multiple configuration files and their corresponding source and target paths.

Example ``diagnostic_config.py`` structure:

.. code-block:: python

    diagnostic_config = {
        'global_biases': [
        {
            'config_file': 'config_global_biases.yaml',
            'source_path': 'config/diagnostics/global_biases',
            'target_path': 'diagnostics/global_biases/cli'
        },
        ]
    }

During the installation process, the configuration and CLI files for each diagnostics type are copied or linked 
from the source path to the target path specified in the ``diagnostic_config.py``.

The target folder structure should follow this pattern:

.. code-block:: text

    $HOME/.aqua/
        ├── diagnostics/
        │   ├── global_biases/
        │   │   ├── config/
        │   │   └── cli/
        │   │       └── config_global_biases.yaml

.. note::
    After the implementation of the diagnostic in the aqua console, be sure that the configuration files are
    correctly found in the installation folder when running the diagnostic and its CLI.