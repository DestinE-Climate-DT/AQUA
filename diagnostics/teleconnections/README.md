# Teleconnections diagnostic

The folder contains jupyter-notebooks and python scripts in order to evaluate teleconnections in the DE_340 AQUA model evaluation framework.
The script are based on the `xarray+dask` framework, a specific list of the packages used can be found inside the notebooks or in the `env-teleconnections.yml` file, that can be used as well to create the `conda` environment needed to run the diagnostic.

Teleconnections available:

- NAO
- ENSO
- MJO

## Library files

- `cdotesting.py` contains function evaluating teleconnections with cdo bindings, in order to test the python libraries (see test section).
- `index.py` contains functions for the direct evaluation of teleconnection indices.
- `plots.py` contains functions for the visualization of time series and maps for teleconnection diagnostic.
- `tools.py` contains generic functions that may be useful to the whole diagnostic.

## Test

Tests are run with github actions, see `.github/workflows/teleconnections.yml` for details.

## Notebooks

All notebooks are in the `notebooks` folder.

- `cdo_testing` contains an example of usage of the cdo bindings introduced in the `cdotesting.py` file.
- `NAO/ENSO` contain the respective teleconnections evaluated with the library methods from `index.py` and `plots.py`.
- `test_cdovslib` contains examples of the usage of functions contained in `cdotesting.py`.
- `test_hovmoller` contains examples of the usage of functions contained in `plots.py`.

## Create the teleconnections env

The `env-teleconnections.yml` file can be used to create the `conda` environment needed to run the diagnostic.

```bash
mamba env create -f env-teleconnections.yml # or conda
```

### Add kernel for DKRZ jupyterhub

Documentation on adding kernels: https://docs.dkrz.de/doc/software%26services/jupyterhub/kernels.html#use-your-own-kernel

After creating the environment, activate it and install the kernel:

'''bash
python -m ipykernel install --user --name teleconnections --display-name="teleconnections"
'''

### Lumi installation

`conda` is not available on Lumi, so the environment has to be created manually.

An installation tool similar to `lumi_installation.sh` has still to be written.