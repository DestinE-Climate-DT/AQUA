# Global mean time series

Main authors:
- Lukas Kluft (MPI, lukas.kluft@mpimet.mpg.de)
- Matteo Nurisso (CNR, m.nurisso@isac.crn.it)

## Description

This diagnostic computes and plots various global mean time series and Gregory-like
plot of radiation imbalance to diagnose model drift.

## Table of Contents

- [Global mean time series](#global-mean-time-series)
  - [Description](#description)
  - [Table of Contents](#table-of-contents)
  - [Installation Instructions](#installation-instructions)
  - [Data requirements](#data-requirements)
  - [Examples](#examples)
  - [Contributing](#contributing)

## Installation Instructions

This diagnostic does not rely on any additional dependencies. You can use the
common python environment provided by the AQUA framework.

## Data requirements

The diagnostic reads various model runs to compute and plot global mean time series.
In addition, the time series can be
extended to inclued any dataset as reference.

## Examples

The **notebook/** folder contains the following notebooks:

- **global_time_series.ipynb**:
  This notebook provides a brief overview of the time series plotting as well
  as a Gregory-like analysis of radiation imbalance to diagnose model drift.

## Contributing

This diagnostic is maintained by Lukas Kluft (@lkluft) and Matteo Nurisso (@mnurisso).
