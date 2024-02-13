# Radiation Budget Diagnostic

Main authors: 
- Susan Sayed (DWD, susan.sayed@dwd.de)

## Description

This diagnostic computes and plots biases of radiation variables and uses the nextGEMS Cycle 3 data as input.

## Installation Instructions

This diagnostic does not rely on any additional dependencies. You can use the
common python environment provided by the AQUA framework.

## Data requirements

The diagnostic reads various model runs (NextGEMS Cycle 3 at the moment) to
compute and plot the results. In addition, the time series has to be
extended to inclued ERA5 reanalysis data and CERES EBAF as reference.

## Examples

The **notebook/** folder contains the following notebooks:

- **time_series.ipynb**: a notebook that shows how the output of the `plot_model_comparison_timeseries` function;
- **gregory.ipynb**: a notebook that demonstrates how to produce gregory plots of desired models;
- **box_plot.ipynb**: a notebook that demonstrates how to create box plots;
- **bias_maps.ipynb**: a notebook that creates bias maps to localise significant biases in comparison to CERES data. 