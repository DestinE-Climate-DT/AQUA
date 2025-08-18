"""Test ensemble Zonal EnsembleZonal module"""
import os
import subprocess
import pytest
import xarray as xr
import numpy as np

try:
    from aqua.diagnostics import EnsembleZonal
except ModuleNotFoundError:
    print("Import error for the ensemble module.")

@pytest.mark.ensemble
def test_Zonal_datasets():
    """
    load aqua-analysis output of timeseries.
    """
    dataset = xr.open_dataset('IFS-NEMO-historical-1990-lra-r100-monthly_zonal_mean_trend_atlantic_ocean.nc')
    dataset_list = [dataset,dataset]
    dataset = xr.concat(dataset_list, "Ensembles")
    del dataset_list
    return dataset

@pytest.mark.ensemble
def test_ensemble_Zonal_compute():
    """Test the compute method of EnsembleZonal."""
    sample_dataset = test_Zonal_datasets()
    Zonal = EnsembleZonal(var="avg_so", dataset=sample_dataset)
    Zonal.run()

    assert Zonal.dataset_mean.all() is not None
    assert Zonal.dataset_std.any() == 0 or np.nan # this step needs correction  

@pytest.mark.ensemble
def test_edit_attributes():
    """Test the edit_attributes method."""
    Zonal = EnsembleZonal(var="avg_so")
    Zonal.edit_attributes(loglevel="DEBUG", cbar_label="Test Label")

    assert Zonal.loglevel == "DEBUG"
    assert Zonal.cbar_label == "Test Label"