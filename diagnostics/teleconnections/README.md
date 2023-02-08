# Teleconnections diagnostic

The folder contains jupyter-notebooks and python scripts in order to evaluate teleconnections in the DE_340 AQUA model evaluation framework.
The script are based on the `xarray+dask` framework, a specific list of the packages used can be found inside the notebooks or in the `env-teleconnections.yml` file.

## Libraries file (WIP)

- `cdotesting.py` contains function evaluating teleconnections with cdo bindings, in order to test the python libraries
- `index.py` contains functions for the direct evaluation of teleconnection indices
- `plots.py` contains functions for the visualization of time series and maps for teleconnection diagnostic
- `tools.py` contains generic functions that may be useful to the whole diagnostic

## Notebooks (WIP)

- `NAO_monthly/seasonal_comparison` are two notebooks containing the comparison between cdo and teleconnections diagnostic, together with a comparison with ncar NAO data
- `cdo_testing` contains an example of usage of the cdo bindings introduced in the `cdotesting.py` file
- `test_libraries` contains examples of usage of the functions implemented in the .py files

### Old notebooks

The folder `old_notebooks` contains previous version of notebooks whose code is now in library functions

- `test_NAO_index` contains examples of NAO index evaluation (station based)
- `test_NAO_regression` contains example of correlation or covariance maps obtained from the previously evaluated NAO index
- `test_ENSO_index` contains examples of Nino 3.4 index evaluation
- `test_ENSO_regression` contains example of correlation or covariance maps obtained from the previously evaluated ENSO index
- `test_cdo_bindings` contains an example of NAO index evaluation with cdo bindings