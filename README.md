# AQUA

The Application for Quality assessment and Uncertainity quAntification (AQUA) is a model evaluation framework designed for running diagnostics on high-resolution climate models, specifically for Climate DT climate simulations being part of Destination Earth activity. The package provides a flexible and efficient python3 framework to process and analyze large volumes of climate data. With its modular design, AQUA offers seamless integration of core functions and a wide range of diagnostic tools that can be run in parallel. AQUA offers:

- Efficient handling of large datasets from high-resolution climate models;
- Support for various data formats, such as NetCDF, GRIB, Zarr or FDB;
- Robust and fast regridding functionality based on CDO;
- Averaging and aggregation tools for temporal and spatial analyses;
- Modular design for easy integration of new diagnostics. 

## Installation

AQUA requires python>=3.9. Recommended installation should be done through mamba (a package manager for conda-forge).

### Create conda/mamba environment and install packages
```
git clone git@github.com:DestinE-Climate-DT/AQUA.git
cd AQUA
mamba env create -f environment.yml
mamba activate aqua
```

This installation will provide both the AQUA framework and the AQUA diagnostics, which can be found in the `diagnostics` folder.

### Use of AQUA container 

An alternative deployment making use of containers is available. Please refer to the `Container` chapter in the [AQUA Documentation](https://aqua-web-climatedt.2.rahtiapp.fi/documentation/container.html).

## Documentation

Full [AQUA Documentation](https://aqua-web-climatedt.2.rahtiapp.fi/documentation/index.html) is available.
Please notice that the webpage is password protected.
You can find the credentials in the [wiki page](https://wiki.eduuni.fi/display/cscRDIcollaboration/AQUA+-+Meetings) or please contact the AQUA team to get access.

## Examples

Please look at the `notebook` folder to explore AQUA functionalities. 

## Command lines tools

Please look at the `cli` folder to have access to the AQUA command line tools. 

## Contributing guide

Please refer to the [Guidelines for Contributors](https://github.com/DestinE-Climate-DT/AQUA/blob/main/CONTRIBUTING.md) if you want to join AQUA team!

