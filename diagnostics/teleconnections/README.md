# Teleconnections diagnostic

The folder contains jupyter-notebooks and python scripts in order to evaluate teleconnections in the DE_340 AQUA model evaluation framework.
The script are based on the `xarray+dask` framework, a specific list of the packages used can be found inside the notebooks or in the `env-teleconnections.yml` file.

## Libraries file (WIP)

- `index.py` contains functions for the direct evaluation of teleconnection indices
- `plots.py` contains functions for the visualization of time series and maps for teleconnection diagnostic
- `tools.py` contains generic functions that may be useful to the whole diagnostic

## Notebooks (WIP)

- `test_NAO_index` notebook contains examples of NAO index evaluation (station based)
- `test_NAO_regression` notebook contains example of correlation or covariance maps obtained from the previously evaluated NAO index
- `test_ENSO_index` notebook contains examples of Nino 3.4 index evaluation
- `test_ENSO_regression` notebook contains example of correlation or covariance maps obtained from the previously evaluated ENSO index