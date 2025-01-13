# Ensemble statistics

Main authors: 
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
  - [TODO](#TODO)
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

- [Example of how to use the 1D ensemble of timeseries](https://github.com/DestinE-Climate-DT/AQUA/blob/dev-ensemble/notebooks/diagnostics/ensemble/ensemble_timeseries.ipynb)

- [Example of how to use the 2D lon-lat ensemble of atmglobalmean](https://github.com/DestinE-Climate-DT/AQUA/blob/dev-ensemble/notebooks/diagnostics/ensemble/ensemble_atmglobalmean.ipynb)

- [Example of how to use the 2D Zonal (lev-lat) ensemble of temperature](https://github.com/DestinE-Climate-DT/AQUA/blob/dev-ensemble/notebooks/diagnostics/ensemble/ensemble_zonalaverage.ipynb)

## TODO

- option to change the `cmap` in the `lon-lat` and `lev-lat` plots.
- option to change the `zorder` in the timeseries plots (It matters while presenting via a projector).
- Saving outputs to netcdf.
- Implementation of the normalized standard deviation.
- Implementation of vertical interpolation in the (ensemble) Zonal mean and standard deviation.
- (preprocessing) checks or flags for the grids/time-frequency, e.g., the ensemble members have same `lon-lat`, `lev-lat` and/or time frequency before computing the ensemble computations.
- checks for dropping `nan`s.
- functions should be added in case of unequal data.
- A combined CLI for all three ensemble sub-classes.
- Implement a class to handle an ensemble of 3D data.
- Integrated in the github workflow.
- Include the Reader class in the notebooks in addition to the specific paths. 
- Include the catalogs whereever the Reader class is used.

## Contributing

Contributions are welcome, please open an issue or a pull request. 
If you have any doubt or suggestion, please contact the AQUA team or Maqsood Mubarak Rajput (maqsoodmubarak.rajput@awi.de)
