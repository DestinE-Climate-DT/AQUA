# Sea ice diagnostics

Main authors: 
- François Massonnet (UCLouvain, francois.massonnet@uclouvain.be)

## Description

The sea ice diagnostic computes the sea ice extent (SIE).
The SIE is defined as the areal integral of all ocean grid cells that contain at least 15% of sea ice concentration (SIC).

## Table of Contents

- [Sea ice diagnostics](#sea-ice-diagnostics)
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

The diagnostic requires the following data:
- `avg_siconc` (Time-mean sea ice area fraction, GRIB paramid 263001)

## Examples

The `notebooks` folder contains notebooks with examples of how to use the diagnostic and its main functions.
Please note that notebooks may load data from the DKRZ cluster, so they may not work outside of levante.

- [Example of how to use the diagnostic](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/seaice/notebooks/seaice.ipynb)

## Contributing

Contributions are welcome, please open an issue or a pull request. 
If you have any doubt or suggestion, please contact the AQUA team or François Massonnet (francois.massonnet@uclouvain.be)
