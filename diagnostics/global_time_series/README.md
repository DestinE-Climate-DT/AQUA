# Global mean time series

Main authors:
- Lukas Kluft (MPI, lukas.kluft@mpimet.mpg.de)
- Matteo Nurisso (CNR, m.nurisso@isac.crn.it)

## Description

This diagnostic computes and plots:

- Global mean time series of various variables
- Gregory-like analysis of radiation imbalance to diagnose model drift
- Seasonal cycle of global mean of various variables

## Table of Contents

- [Global mean time series](#global-mean-time-series)
  - [Description](#description)
  - [Table of Contents](#table-of-contents)
  - [Installation Instructions](#installation-instructions)
  - [Data requirements](#data-requirements)
  - [Examples](#examples)
  - [Contributing](#contributing)

## Installation Instructions

This diagnostic does not rely on any additional dependencies.
It is installed automatically when installing the AQUA environment.

## Data requirements

The diagnostic requires the variables that the user wants to analyse.
For the Gregory-like plot, the following variables are required:

- ``2t`` (2 metre temperature, GRIB paramid 167)
- ``mtnlwrf`` (Mean top net long-wave radiation flux, GRIB paramid 235040)
- ``mtnswrf`` (Mean top net short-wave radiation flux, GRIB paramid 235039)

## Examples

The **notebook/** folder contains the following notebooks:

- **global_time_series.ipynb**:
  This notebook provides a brief overview of the time series plotting as well
  as a Gregory-like analysis of radiation imbalance to diagnose model drift.
- **seasonal_cycles.ipynb**:
  This notebook provides a brief overview of the seasonal cycle plotting.

## Contributing

This diagnostic is maintained by Matteo Nurisso (@mnurisso).
