# Ensemble statistics

Author: 
- Maqsood Mubarak Rajput (AWI,maqsoodmubarak.rajput@awi.de)

## Description

The ensemble module computes mean and standard deviation of climate model data.

## Table of Contents

- [Ensemble statistics](#Ensemble-statistics)
  - [Description](#description)
  - [Table of Contents](#table-of-contents)
  - [Installation Instructions](#installation-instructions)
  - [Data requirements](#data-requirements)
  - [Examples](#examples)
  - [Contributing](#contributing)

## Installation Instructions

To install this diagnostic you can use conda.

No more environments than the regular AQUA ones (located in `./environment.yaml`) are needed.
Refer to the AQUA documentation for more information.

## Data requirements

The ensemble members with `1D`,`2D` in `lon-lat` or `lev-lat` dimensions needs merged along the default dimension `Ensembles` to create an `xarray.Dataset`. This `xarray.Dataset` is then given to the `ensemble` module. 

## Examples

The `notebooks` folder contains notebooks with examples of how to use the ensemble module and its main functions.
Please note that notebooks may load data from the DKRZ cluster, so they may not work outside of levante.

- [Example of how to use the 1D ensemble of timeseries](https://github.com/DestinE-Climate-DT/AQUA/blob/main/notebooks/diagnostics/ensemble/ensemble_timeseries.ipynb)

- [Example of how to use the 2D lon-lat ensemble of atmglobalmean](https://github.com/DestinE-Climate-DT/AQUA/blob/main/notebooks/diagnostics/ensemble/ensemble_global_2D.ipynb)

- [Example of how to use the 2D Zonal (lev-lat) ensemble of temperature](https://github.com/DestinE-Climate-DT/AQUA/blob/main/notebooks/diagnostics/ensemble/ensemble_zonalaverage.ipynb)


## Contributing

Contributions are welcome, please open an issue or a pull request. 
If you have any doubt or suggestion, please contact the AQUA team or Maqsood Mubarak Rajput (maqsoodmubarak.rajput@awi.de)
