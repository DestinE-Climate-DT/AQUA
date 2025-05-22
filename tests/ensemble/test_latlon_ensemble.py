"""Test ensemble 2D lat-lon EnsembleLatLon module"""
import os
import subprocess
import pytest
import xarray as xr
import numpy as np

try:
    from aqua.diagnostics import EnsembleLatLon
except ModuleNotFoundError:
    print("Import error for the ensemble module.")

@pytest.mark.ensemble
def test_2D_datasets():
    """
    load aqua-analysis output of timeseries.
    """
    dataset = xr.open_dataset('atmglobalmean.statistics_maps.2t.IFS-FESOM_historical-1990.nc')
    dataset_list = [dataset,dataset]
    dataset = xr.concat(dataset_list, "Ensembles")
    del dataset_list
    return dataset

@pytest.mark.ensemble
def test_ensemble_2D_compute():
    """Test the compute method of EnsembleLatLons."""
    sample_dataset = test_2D_datasets()
    latlon = EnsembleLatLon(var="2t", dataset=sample_dataset)
    latlon.run()

    assert latlon.dataset_mean is not None
    assert latlon.dataset_std.all() == 0 

@pytest.mark.ensemble
def test_edit_attributes():
    """Test the edit_attributes method."""
    latlon = EnsembleLatLon(var="2t")
    latlon.edit_attributes(loglevel="DEBUG", cbar_label="Test Label")

    assert latlon.loglevel == "DEBUG"
    assert latlon.cbar_label == "Test Label"
