# Teleconnections diagnostic

The folder contains jupyter-notebooks and python scripts in order to evaluate teleconnections in the DE_340 AQUA model evaluation framework.
The script are based on the `xarray+dask` framework, a specific list of the packages used can be found inside the notebooks or in the `env-teleconnections.yml` file, that can be used as well to create the `conda` environment needed to run the diagnostic.

## Library files (WIP)

- `cdotesting.py` contains function evaluating teleconnections with cdo bindings, in order to test the python libraries (see test section).
- `index.py` contains functions for the direct evaluation of teleconnection indices.
- `plots.py` contains functions for the visualization of time series and maps for teleconnection diagnostic.
- `tools.py` contains generic functions that may be useful to the whole diagnostic.

## Test (WIP)

CI tests are run with github actions, see `.github/workflows/aqua.yml` for details.

### Old test
- `unit_test.py` contains a comparison between the diagnostic developed in `index.py` and the same method applied with `cdo` bindings in order to keep track of possible problems in the development phase.

## Notebooks (WIP)

All notebooks are in the `notebooks` folder.

- `cdo_testing` contains an example of usage of the cdo bindings introduced in the `cdotesting.py` file.
- `NAO/ENSO` contain the respective teleconnections evaluated with the library methods from `index.py` and `plots.py`.
- `test_cdovslib` contains examples of the usage of functions contained in `cdotesting.py`.
- `test_hovmoller` contains examples of the usage of functions contained in `plots.py`.

## Create the teleconnections env and add kernel for DKRZ jupyterhub

Documentation on adding kernels: https://docs.dkrz.de/doc/software%26services/jupyterhub/kernels.html#use-your-own-kernel

It should come down to:

'''bash
mamba env create -f env-teleconnections.yml # or conda 
python -m ipykernel install --user --name teleconnections --display-name="teleconnections"
'''