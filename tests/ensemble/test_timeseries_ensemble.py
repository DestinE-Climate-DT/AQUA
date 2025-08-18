"""Test ensemble module"""
import os
import subprocess
import pytest
import xarray as xr
import numpy as np

try:
    from aqua.diagnostics import EnsembleTimeseries
except ModuleNotFoundError:
    print("Import error for the ensemble module.")

@pytest.mark.ensemble
def test_timeseries_datasets():
    """
    load aqua-analysis output of timeseries.
    """
    dataset = xr.open_dataset('global_time_series_timeseries_2t_IFS-FESOM_historical-1990_mon.nc')
    dataset_list = [dataset,dataset]
    dataset = xr.concat(dataset_list, "Ensembles")
    del dataset_list
    return dataset

@pytest.mark.ensemble
def test_ensemble_timeseries_compute():
    """Test the compute method of EnsembleTimeseries."""
    sample_dataset = test_timeseries_datasets()
    ts = EnsembleTimeseries(var="2t", mon_model_dataset=sample_dataset)
    ts.run()

    assert ts.mon_dataset_mean is not None
    assert ts.mon_dataset_std.values.all() == 0 

@pytest.mark.ensemble
def test_edit_attributes():
    """Test the edit_attributes method."""
    ts = EnsembleTimeseries(var="2t")
    ts.edit_attributes(loglevel="DEBUG", ensemble_label="Test Ensemble")

    assert ts.loglevel == "DEBUG"
    assert ts.ensemble_label == "Test Ensemble"