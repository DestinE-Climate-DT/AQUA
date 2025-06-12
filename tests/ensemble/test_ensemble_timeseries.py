"""Test ensemble Ensemble module"""
import os
import subprocess
import pytest
import xarray as xr
import numpy as np
from aqua import Reader
from aqua.diagnostics import EnsembleTimeseries
from aqua.diagnostics.ensemble.util import retrieve_merge_ensemble_data

@pytest.mark.ensemble
def test_ensemble_timeseries():
    """Initialize variables before the test."""
    catalog_list = ['ci', 'ci']
    models_catalog_list = ['FESOM', 'FESOM']
    exps_catalog_list = ['results', 'results']
    sources_catalog_list = ['timeseries1D', 'timeseries1D']
    variable = '2t'
    
    # loading and merging the data
    dataset = retrieve_merge_ensemble_data(
        variable=variable,
        catalog_list=catalog_list,
        models_catalog_list=models_catalog_list,
        exps_catalog_list=exps_catalog_list,
        sources_catalog_list=sources_catalog_list
        )
    print(dataset)
    assert dataset is not None

    ts = EnsembleTimeseries(
    var=variable,
    mon_model_dataset=dataset,
    ann_model_dataset=dataset,
    )
    ts.compute()
    assert ts.mon_dataset_mean is not None
    assert ts.ann_dataset_mean is not None
    assert ts.mon_dataset_std.values.all() == 0
    assert ts.ann_dataset_std.values.all() == 0
    
    #fig, ax = ts.plot()
    #assert fig is not None
   


