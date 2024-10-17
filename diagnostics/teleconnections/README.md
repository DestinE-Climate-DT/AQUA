# Teleconnections diagnostic

Main authors: 
- Matteo Nurisso (m.nurisso@isac.cnr.it)

## Description

The folder contains jupyter-notebooks and python scripts in order to evaluate teleconnections in the DE_340 AQUA model evaluation framework.
The script are based on the `AQUA` framework.

At the moment the following teleconnections are available:
- [NAO](https://github.com/oloapinivad/DestinE-Climate-DT/blob/main/diagnostics/teleconnections/notebooks/NAO.ipynb)
- [ENSO](https://github.com/oloapinivad/DestinE-Climate-DT/blob/main/diagnostics/teleconnections/notebooks/ENSO.ipynb)

See the documentation for more details on the teleconnections.

## Table of contents

- [Teleconnections diagnostic](#teleconnections-diagnostic)
  - [Description](#description)
  - [Table of contents](#table-of-contents)
  - [Installation instructions](#installation-instructions)
  - [Data requirements](#data-requirements)
  - [Contributing](#contributing)

## Installation instructions

The diagnostic is based on the `AQUA` framework, and requires the installation of the `AQUA` package.
Follow the instructions in the `AQUA` documentation to install the framework.
A `pyproject.toml` file is provided in this folder to install the diagnostic in the AQUA environment.
It is not tought to be used as a standalone package.

## Data requirements

The diagnostic requires the following data:
- 'msl' (Mean sea level pressure, paramid 151) for NAO
- 'avg_tos' (Time-mean sea surface temperature, paramid 263101) for ENSO

These are the minimum requirements, but the diagnostic can be easily extended to other variables, since the regression and 
correlation maps can be done with every variable available in the dataset.
The names of the variables refers to the DestinE data governance, but the diagnostic can be easily adapted to other standards with the usage of the interface files.

Data should be preferably in the form of monthly means and it would be optimal for efficiency to have data on a grid with a resolution of 1°x1° (LRA format).
It is possible to initialize the class with different regridding and time aggregation options, so that the diagnostic can deal with different resolutions and time frequencies.

Comparisons with observations are also available, and require to have access to ERA5 data.
Data are already available on Levante and LUMI.

Additionally, NCAR data with monthly values of NAO and ENSO indices are available in the `data` folder.
These are used to compare the teleconnections in the DE_340 simulations with the observations in the notebooks.

## Contributing

Contributions are welcome, please open an issue or a pull request.
If you have any doubt or suggestion, please contact the AQUA team or Matteo Nurisso (@mnurisso, m.nurisso@isac.cnr.it).