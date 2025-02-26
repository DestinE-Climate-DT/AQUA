import os
import subprocess
import pytest
import xarray as xr
import numpy as np

try:
    from aqua.diagnostics import EnsembleLatLon
except ModuleNotFoundError:
    print("Import error for the ensemble module.")

# Fixture to load the dataset
@pytest.fixture
def sample_dataset():
    """
    Load aqua-analysis output of timeseries and return the concatenated dataset.
    """
    dataset = xr.open_dataset('atmglobalmean.statistics_maps.2t.IFS-FESOM_historical-1990.nc')
    dataset_list = [dataset, dataset]
    dataset = xr.concat(dataset_list, "Ensembles")
    del dataset_list
    return dataset

# Test to compute ensemble 2D LatLon
@pytest.mark.ensemble
def test_ensemble_2D_compute(sample_dataset):
    """Test the compute method of EnsembleLatLon."""
    latlon = EnsembleLatLon(var="2t", dataset=sample_dataset)
    latlon.run()

    assert latlon.dataset_mean is not None
    assert latlon.dataset_std.all() == 0

# Test to edit attributes of EnsembleLatLon
@pytest.mark.edit
def test_edit_attributes():
    """Test the edit_attributes method."""
    latlon = EnsembleLatLon(var="2t")
    latlon.edit_attributes(loglevel="DEBUG", cbar_label="Test Label")

    assert latlon.loglevel == "DEBUG"
    assert latlon.cbar_label == "Test Label"


