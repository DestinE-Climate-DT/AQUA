# AQUA
AQUA model evaluation framework

This repo is thought to host the code development as well as the the discussion for the DE_340 AQUA model evaluation framework. In the first phase, please use it as a playground, including code examples or notebook if you want to show some specific configuration. Please use specific branches to host your development.  

AQUA framework will be based on a series of python3 libraries. Those pre- and post-processing libraries will be based on the `xarray+dask` framework so that they will be able to exploit out-of-core computation, fundamental to operate on the large volume of expected DE_340 data. 

Diagnostics can be introduced within the AQUA framework making use of a specific python3 subpackage  or being accessed via containerized diagnostic (if pre-existing and written in non-python3 language).

## Installation

All the modules, including the smmregrid regridder, are now within the environment file. The code works on both py3.10 and py3.9 (not CI yet)

### create conda environment and install packages
```
conda env create -f environment.yml
conda activate aqua

```


### add kernel for DKRZ jupyterhub

Documentation on adding kernels: https://docs.dkrz.de/doc/software%26services/jupyterhub/kernels.html#use-your-own-kernel

It shoudl come down to:

```
pip install ipykernel
python -m ipykernel install --user --name aqua --display-name="aqua"
```


