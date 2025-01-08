# Timeseries

Main authors:
- Matteo Nurisso (CNR, m.nurisso@isac.crn.it)
- Lukas Kluft (MPI, lukas.kluft@mpimet.mpg.de)

## Description

This diagnostic computes and plots:

- Global and Regional monthly and annual timeseries of any variable or derived quantity
- Gregory-like analysis of radiation imbalance to diagnose model drift
- Seasonal cycle of global and regional mean of any variable or derived quantity

## Table of Contents

- [Timeseries](#timeseries)
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

The **AQUA/notebook/diagnostics/timeseries** folder contains the following notebooks:

- **timeseries.ipynb**:
  This notebook provides a brief overview of the time series plotting as well
  as a Gregory-like analysis of radiation imbalance to diagnose model drift.
- **seasonalcycles.ipynb**:
  This notebook provides a brief overview of the seasonal cycle plotting.

## Contributing

This diagnostic is maintained by Matteo Nurisso (@mnurisso, m.nurisso@isac.cnr.it).