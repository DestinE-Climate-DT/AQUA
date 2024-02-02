# AQUA

The Application for Quality assessment and Uncertainity quAntification (AQUA) is a is a model evaluation framework designed for running diagnostics on high-resolution climate models, specifically for Climate DT climate simulations being part of Destination Earth activity. The package provides a flexible and efficient framework to process and analyze large volumes of climate data. With its modular design, AQUA offers seamless integration of core functions and a wide range of diagnostic tools that can be run in parallel. AQUA offers:

- Efficient handling of large datasets from high-resolution climate models;
- Support for various data formats, such as NetCDF, GRIB, HDF or FDB;
- Robust and fast regridding functionality;
- Averaging and aggregation tools for temporal and spatial analyses;
- Modular design for easy integration of new diagnostics.

## Installation

AQUA requires python>=3.9,<3.12. Recommended installation through mamba (a package manager for conda-forge).

### Create conda/mamba environment and install packages
```
git clone git@github.com:DestinE-Climate-DT/AQUA.git
cd AQUA
mamba env create -f environment.yml
mamba activate aqua
```

This installation will provide both the AQUA framework and the AQUA diagnostics, which can be found in the `diagnostics` folder.

### Use of AQUA container 

An alternative deployment making use of containers is available. Please refer to the `AQUA Container` chapter in the [AQUA Documentation](https://wiki.eduuni.fi/download/attachments/288474772/aqua.pdf).

## Documentation

Full [AQUA Documentation](https://wiki.eduuni.fi/download/attachments/288474772/aqua.pdf) is available on the Climate DT Wiki.

## Examples

Please look at the `notebook` folder to explore AQUA functionalities. 

## Command lines tools

Please look at the `cli` folder to have access to the AQUA command line tools. 

## Contributing guide

Please refer to the [Guidelines for Contributors](https://github.com/DestinE-Climate-DT/AQUA/blob/main/CONTRIBUTING.md) if you want to join AQUA team!

