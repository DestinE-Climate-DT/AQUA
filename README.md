![maintenance-status](https://img.shields.io/badge/maintenance-actively--developed-brightgreen.svg)
[![PyPI - Version](https://img.shields.io/pypi/v/aqua-core?style=flat)](https://pypi.org/project/aqua-core/)
[![AQUA tests](https://github.com/DestinE-Climate-DT/AQUA/actions/workflows/aqua.yml/badge.svg)](https://github.com/DestinE-Climate-DT/AQUA/actions/workflows/aqua.yml)
[![Documentation Status](https://readthedocs.org/projects/aqua/badge/?version=latest)](https://aqua.readthedocs.io/en/latest/)
[![codecov](https://codecov.io/gh/DestinE-Climate-DT/AQUA/graph/badge.svg?token=E9D0A8SWIU)](https://codecov.io/gh/DestinE-Climate-DT/AQUA)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.14906075.svg)](https://doi.org/10.5281/zenodo.14906075)

# AQUA

The Application for QUality Assessment (AQUA) is a model evaluation framework designed for running diagnostics on high-resolution climate models, specifically for Climate DT climate simulations being part of Destination Earth activity. The package provides a flexible and efficient python3 framework to process and analyze large volumes of climate data. With its modular design, AQUA offers seamless integration of core functions and a wide range of diagnostic tools that can be run in parallel. AQUA offers:

- Efficient handling of large datasets from high-resolution climate models;
- Support for various data formats, such as NetCDF, GRIB, Zarr or FDB;
- Robust and fast regridding functionality based on CDO;
- Averaging and aggregation tools for temporal and spatial analyses;
- Modular design for easy integration of new diagnostics. 

## Installation

AQUA requires python>=3.10,<3.13. Recommended installation should be done through a package manager for conda-forge (e.g. [Miniforge](https://github.com/conda-forge/miniforge)).

### Create conda/mamba environment and install packages

```
git clone git@github.com:DestinE-Climate-DT/AQUA.git
cd AQUA
mamba env create -f environment.yml
mamba activate aqua
```

This installation will provide both the AQUA framework and the AQUA diagnostics.

### Use of AQUA container 

An alternative deployment making use of containers is available. Please refer to the `Container` chapter in the [AQUA Documentation](https://aqua.readthedocs.io/en/latest/container.html).

## Documentation

Full [AQUA Documentation](https://aqua.readthedocs.io/en/latest/) is available on ReadTheDocs.

## Examples

Please look at the `notebook` folder to explore AQUA functionalities.

## Command lines tools

Please look at the `cli` folder to have access to the AQUA command line tools. 

## Contributing guide

Please refer to the [Guidelines for Contributors](https://github.com/DestinE-Climate-DT/AQUA/blob/main/CONTRIBUTING.md) if you want to join AQUA team!

## License

AQUA is distributed as open source software under Apache 2.0 License. The copyright owner is the European Union, represented by the European Commission. The development of AQUA has been funded by the European Union through Contract `DE_340_CSC - Destination Earth Programme Climate Adaptation Digital Twin (Climate DT)`. Further info can be found at https://destine.ecmwf.int/ and https://destination-earth.eu/
