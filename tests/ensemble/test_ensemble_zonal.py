"""Test ensemble Ensemble module"""
import os
import subprocess
import pytest
import xarray as xr
import numpy as np
from aqua import Reader
from aqua.diagnostics import EnsembleZonal
from aqua.diagnostics.ensemble.util import retrieve_merge_ensemble_data

@pytest.mark.ensemble
def test_ensemble_zonal():
    """Initialize variables before the test."""
    catalog_list = ['ci', 'ci']
    models_catalog_list = ['NEMO', 'NEMO']
    exps_catalog_list = ['results', 'results']
    sources_catalog_list = ['zonal_mean-latlev', 'zonal_mean-latlev']
    variable = 'avg_so'

    dataset = retrieve_merge_ensemble_data(
        variable=variable, 
        catalog_list=catalog_list, 
        models_catalog_list=models_catalog_list, 
        exps_catalog_list=exps_catalog_list, 
        sources_catalog_list=sources_catalog_list, 
        log_level = "WARNING",
        ens_dim="ensemble"
    )
    
    assert dataset is not None
    
    zonalmean_ens = EnsembleZonal(var=variable, 
        dataset=dataset, 
        ensemble_dimension_name="ensemble")
    zonalmean_ens.compute()
    
    assert zonalmean_ens.dataset_mean is not None
    assert zonalmean_ens.dataset_std.all() == 0
    
    plot_dict = zonalmean_ens.plot()
    
    assert plot_dict['mean_plot'][0] is not None
    assert plot_dict['std_plot'][0] is not None





