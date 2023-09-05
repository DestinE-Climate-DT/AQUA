# AQUA
AQUA model evaluation framework

This repository is thought to host the code development as well as the discussion for the DE_340 AQUA model evaluation framework. In the first phase, please use it as a playground, including code examples or notebook if you want to show some specific configuration. Please use specific branches to host your development.  

AQUA framework will be based on a series of python3 libraries. Those libraries are based on the `xarray+dask` framework so that they will be able to exploit out-of-core computation, fundamental to operate on the large volume of expected DE_340 data. 
Most important module is the `Reader` class which allows for data access through intake catalog - and later on through FDB - as well as regridding (via the smmregrid module), time and spatial averaging as well as changes in data format convention.

Diagnostics can be introduced within the AQUA framework making use of a specific python3 subpackage, listed in the `diagnostics` folder.

## Installation

The code works on both py3.10 and py3.9.  Recommended installation through mamba (a package manager for conda-forge)

### create conda/mamba environment and install packages
```
git clone git@github.com:oloapinivad/AQUA.git
cd AQUA
mamba env create -f environment.yml
conda activate aqua
```

Some diagnostics of aqua require extra conda or pip dependencies and have their environment files (for example, `teleconnections/env-teleconnections.yml` or `diagnostics/tropical_rainfall/env-tropical-rainfall.yml`).
For simplicity, we provide the user with an additional environment file, `environment-common.yml `, which contains all standard dependencies of aqua and extra dependencies for each diagnostic:
``` 
mamba env create -f environment-common.yml 
``` 

## Examples

Please look at the `notebook` folder to explore AQUA functionalities

### Note on adding a kernel for DKRZ jupyterhub

Documentation on adding kernels: https://docs.dkrz.de/doc/software%26services/jupyterhub/kernels.html#use-your-own-kernel

It should come down to:

```
pip install ipykernel
python -m ipykernel install --user --name aqua --display-name="aqua"
```

## Contributing guide

A contribution guide is available [here](CONTRIBUTING.md). Please read it before contributing to the project.

