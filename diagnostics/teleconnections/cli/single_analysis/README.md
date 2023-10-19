# Teleconnections CLI

This folder contains the code to perform the analysis of a single experiment with the teleconnections diagnostic.

## Usage

The CLI script is called `cli_teleconnections.py` and can be used as follows:

```bash
mamba activate aqua_common
python cli_teleconnections.py --config <path_to_config_file>
```

Configuration files can be found in this folder.
Please refer to the notes in the configuration files for more information on the options.
`cli_config_atm.yaml` is an example configuration file for the atmosphere part of the teleconnections diagnostic.
`cli_config_oce.yaml` is an example configuration file for the ocean part of the teleconnections diagnostic.

A more detailed description of additional options can be found by running `python cli_teleconnections.py --help`.
If a configuration is specified both in the configuration file and as a command line argument, the command line argument takes precedence.

## Computational requirements

The diagnostic require to have the correct environment loaded.
It requires in order to produce all the figures the ERA5 reanalysis data in the catalogue, as `model=ERA5`, `exp=era5`, `source=monthly`.
We provide no slurm script to run the diagnostic because the execution on a low resolution archive is very fast and an interactive session is sufficient.

Please note that a wrapper to execute all the diagnostics is provided in the `cli/aqua-analysis` folder.