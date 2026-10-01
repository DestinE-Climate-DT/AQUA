"""Tests for the AQUA STAC intake driver."""

import json
import os
import tempfile

import intake
import numpy as np
import pytest
import xarray as xr

from aqua.core.intake_drivers import IntakeSTACSource


@pytest.fixture
def sample_stac_netcdf_path():
    """Path to the IFS STAC item in AQUA_tests."""
    test_dir = os.path.dirname(__file__)
    item_path = os.path.abspath(
        os.path.join(
            test_dir,
            "../AQUA_tests/models/stac-example/items/ifs/ifs-long-regridded-r18x9.json",
        )
    )
    if not os.path.isfile(item_path):
        pytest.skip("STAC example item not found on disk")
    return item_path


@pytest.mark.aqua
def test_stac_driver_registration():
    """Verify that 'stac' driver is registered in intake."""
    registered = intake.registry.drivers.registered()
    assert "stac" in registered
    assert registered["stac"] is IntakeSTACSource


@pytest.mark.aqua
def test_stac_read_netcdf(sample_stac_netcdf_path):
    """Test reading a NetCDF-backed STAC item."""
    source = IntakeSTACSource(urlpath=sample_stac_netcdf_path, asset="data")

    # Check exposed metadata
    assert source.metadata.get("aqua:model") == "IFS"
    assert source.metadata.get("id") == "ifs-long-regridded-r18x9"
    assert source.metadata.get("collection") == "ifs"
    assert "2t" in source.metadata.get("aqua:variables", [])
    assert source.asset_name == "data"
    assert "href" in source.asset_info

    # Read dataset
    ds = source.read()
    assert isinstance(ds, xr.Dataset)
    assert "2t" in ds.data_vars
    assert "ttr" in ds.data_vars

    # Read chunked (to_dask)
    ds_dask = source.to_dask()
    assert isinstance(ds_dask, xr.Dataset)


@pytest.mark.aqua
def test_stac_read_zarr(tmp_path):
    """Test reading a Zarr-backed STAC item with relative href."""
    zarr_dir = tmp_path / "sample.zarr"
    json_path = tmp_path / "item.json"

    # Create dummy dataset and write to zarr
    ds = xr.Dataset({"temperature": (["time", "x"], np.ones((5, 10)))})
    ds.to_zarr(str(zarr_dir))

    # Create STAC item JSON
    stac_item = {
        "id": "synthetic-zarr-item",
        "type": "Feature",
        "stac_version": "1.0.0",
        "properties": {
            "title": "Synthetic Zarr Item",
            "aqua:model": "SyntheticModel",
        },
        "assets": {
            "zarr_store": {
                "href": "sample.zarr",
                "type": "application/vnd+zarr",
                "roles": ["data"],
            }
        },
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(stac_item, f)

    source = IntakeSTACSource(urlpath=str(json_path), asset="zarr_store")

    assert source.metadata.get("title") == "Synthetic Zarr Item"
    assert source.metadata.get("aqua:model") == "SyntheticModel"

    loaded_ds = source.read()
    assert isinstance(loaded_ds, xr.Dataset)
    assert "temperature" in loaded_ds.data_vars
    assert loaded_ds["temperature"].shape == (5, 10)


@pytest.mark.aqua
def test_stac_catalog_yaml(sample_stac_netcdf_path):
    """Test loading a STAC entry from an Intake YAML catalog."""
    catalog_content = f"""
sources:
  stac_ifs:
    description: Test STAC entry
    driver: stac
    args:
      urlpath: "{sample_stac_netcdf_path}"
      asset: "data"
    metadata:
      fixer_name: "IFS-default"
"""
    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as f:
        f.write(catalog_content)
        f.flush()

        cat = intake.open_catalog(f.name)
        assert "stac_ifs" in cat

        entry = cat["stac_ifs"]
        assert entry.metadata.get("fixer_name") == "IFS-default"
        assert entry.metadata.get("aqua:model") == "IFS"

        ds = entry.read()
        assert isinstance(ds, xr.Dataset)
        assert "2t" in ds.data_vars


@pytest.mark.aqua
def test_stac_asset_selection_errors(tmp_path):
    """Test error handling when asset is missing or item has no assets."""
    json_path = tmp_path / "item_no_assets.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"id": "empty-item", "type": "Feature", "assets": {}}, f)

    with pytest.raises(ValueError, match="contains no assets"):
        IntakeSTACSource(urlpath=str(json_path))

    json_path2 = tmp_path / "item_one_asset.json"
    with open(json_path2, "w", encoding="utf-8") as f:
        json.dump(
            {
                "id": "single-asset-item",
                "type": "Feature",
                "assets": {"preview": {"href": "preview.png"}},
            },
            f,
        )

    with pytest.raises(KeyError, match="Asset 'missing' not found"):
        IntakeSTACSource(urlpath=str(json_path2), asset="missing")
