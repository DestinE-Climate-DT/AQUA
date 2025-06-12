"""Test ensemble Ensemble module"""
import os
import subprocess
import pytest
import xarray as xr
import numpy as np
from aqua import Reader
from aqua.diagnostics import EnsembleLatLon
from aqua.diagnostics.ensemble.util import retrieve_merge_ensemble_data

@pytest.mark.ensemble
def test_ensemble_2D_LatLon():
    """Initialize variables before the test."""
    catalog_list = ['ci', 'ci']
    models_catalog_list = ['FESOM', 'FESOM']
    exps_catalog_list = ['results', 'results']
    sources_catalog_list = ['atmglobalmean2D', 'atmglobalmean2D']
    variable = '2t'

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
    
    atmglobalmean_ens = EnsembleLatLon(var=variable, 
        dataset=dataset, 
        ensemble_dimension_name="ensemble")
    atmglobalmean_ens.compute()
    
    assert atmglobalmean_ens.dataset_mean is not None
    assert atmglobalmean_ens.dataset_std.all() == 0
    
    plot_dict = atmglobalmean_ens.plot()
    
    assert plot_dict['mean_plot'][0] is not None




    
