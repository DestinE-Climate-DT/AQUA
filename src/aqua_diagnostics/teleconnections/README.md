# Teleconnections diagnostic

Main authors: 
- Matteo Nurisso (m.nurisso@isac.cnr.it)

## Description

The folder contains jupyter-notebooks and python scripts in order to evaluate teleconnections in the DE_340 AQUA model evaluation framework.
The script are based on the `AQUA` framework.

At the moment the following teleconnections are available:
- [NAO](https://github.com/DestinE-Climate-DT/AQUA/tree/main/notebooks/diagnostics/teleconnections/NAO.ipynb)
- [ENSO](https://github.com/DestinE-Climate-DT/AQUA/tree/main/notebooks/diagnostics/teleconnections/ENSO.ipynb)

See the documentation for more details on the teleconnections.

## Table of contents

- [Teleconnections diagnostic](#teleconnections-diagnostic)
  - [Description](#description)
  - [Table of contents](#table-of-contents)
  - [Installation instructions](#installation-instructions)
  - [Data requirements](#data-requirements)
- [Teleconnections CLI](#teleconnections-cli)
  - [Usage](#usage)
    - [Configuration](#configuration)
  - [Computational requirements](#computational-requirements)
  - [Bootstrap CLI](#bootstrap-cli)
- [Contributing](#contributing)

## Installation instructions

The diagnostic is based on the `AQUA` framework, and requires the installation of the `AQUA` package.
Follow the instructions in the `AQUA` documentation to install it.

## Data requirements

The diagnostic requires the following data:
- 'msl' (Mean sea level pressure) for NAO
- 'tos' (Sea surface temperature) for ENSO

These are the minimum requirements, but the diagnostic can be easily extended to other variables, since the regression and correlation maps can be done with every variable available in the dataset.
The names of the variables refers to the DestinE data governance, but the diagnostic can be easily adapted to other standards with the usage of the interface files.

Data should be preferably in the form of monthly means and it would be optimal for efficiency to have data on a grid with a resolution of 1°x1° (LRA format).
It is possible to initialize the class with different regridding and time aggregation options, so that the diagnostic can deal with different resolutions and time frequencies.

Comparisons with observations are also available, and require to have access to ERA5 data.
Data are already available on Levante and LUMI.

Additionally, NCAR data with monthly values of NAO and ENSO indices are available in the `notebooks/diagnostics/teleconnections/data` folder.
These are used to compare the teleconnections in the DE_340 simulations with the observations in the notebooks.

# Teleconnections CLI

This folder contains the code to perform the analysis with the teleconnections diagnostic.

## Usage

The CLI script is called `cli_teleconnections.py` and can be used as follows:

```bash
mamba activate aqua # if you are using the conda environment
python cli_teleconnections.py
```

### Configuration

Additional options are:
-  `-c` or `--config`: path to the configuration file
-  `-d` or `--dry`: if True, run is dry, no files are written
-  `-l` or `--loglevel`: log level [default: WARNING]
-  `--ref`: if True, analysis is performed against a reference
-  `--catalog`: catalog name
-  `--model`: model name
-  `--exp`: experiment name
-  `--source`: source name
-  `--outputdir`: output directory
-  `--interface`: interface file

Configuration files can be found in this folder and are named `cli_config_*.yaml`:
- `cli_config_atm.yaml` is an example configuration file for the atmosphere part of the teleconnections diagnostic. It is the default configuration file.
- `cli_config_oce.yaml` is an example configuration file for the ocean part of the teleconnections diagnostic.

The configuration file is a YAML file with the following structure:

```yaml
# you can turn on/off the atmosferic teleconnections you want to compute
teleconnections:
  NAO: true

# The first is overwritten if the script with the flags--catalog, --model, --exp, --source
# Extra keyword arguments for the models are:
# regrid: null
# freq: null
# zoom: null
# These are all null (None) by default because we're assuming
# data are monthly and already regridded to a 1deg grid.
# If you want to use native data, you have to set these parameters
# for each model.
models:
  - catalog: 'lumi-phase1'
    model: 'IFS'
    exp: 'tco2559-ng5-cycle3'
    source: 'lra-r100-monthly'

# Reference is analyzed if --ref is passed to the script
# The same extra keyword arguments as for the models can be used.
reference:
  - catalog: 'obs'
    model: 'ERA5'
    exp: 'era5'
    source: 'monthly'

# This is overwritten if the script with the flags --configdir
# Common output directory for all teleconnections
# Structure of the output directory:
# outputdir
# ├── pdf
# └── netcdf
outputdir: './output' # common output directory for figures and netcdf files

# Configdir for the teleconnections
configdir: null

# List of teleconnections specific configuration parameters
NAO:
  months_window: 3
```

## Computational requirements

The diagnostic require to have the correct environment loaded.
It requires in order to produce all the figures if `--ref` is set the ERA5 reanalysis data in the catalog, as `model=ERA5`, `exp=era5`, `source=monthly`.
We provide a slurm script to run the diagnostic but the execution on a low resolution archive is very fast and an interactive session is sufficient.

Please note that a wrapper to execute all the diagnostics is provided in the `cli/aqua-analysis` folder.

## Bootstrap CLI

An additional CLI not included in the wrapper is the `cli_bootstrap.py` script.
This script will not produce any plot but it will evaluate with 1000 bootstrap the significance of the teleconnections regression map.
This is available only for full year regression maps and not for the seasonal ones.
The script can be run as follows:

```bash
mamba activate aqua # if you are using the conda environment
python cli_bootstrap.py -c config_bootstrap.yaml
```

The configuration file has the same structure of the `cli_config_*.yaml` files.

# Contributing

Contributions are welcome, please open an issue or a pull request.
If you have any doubt or suggestion, please contact the AQUA team or Matteo Nurisso (@mnurisso, m.nurisso@isac.cnr.it).