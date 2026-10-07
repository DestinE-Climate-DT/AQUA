"""Tests for the AQUA STAC intake driver."""

import json
import os
import tempfile

import intake
import numpy as np
import pytest
import xarray as xr

from aqua import Reader
from aqua.core.intake_drivers import IntakeSTACSource


@pytest.fixture(scope="session")
def stac_sample_data(tmp_path_factory):
    """Generate STAC sample items and collection on the fly using Reader catalog paths."""
    try:
        reader_long = Reader(model="IFS", exp="test-tco79", source="long", areas=False, fix=False)
        long_nc_path = os.path.abspath(reader_long.backend._all_urls[0])

        reader_short = Reader(model="IFS", exp="test-tco79", source="short", areas=False, fix=False)
        short_nc_path = os.path.abspath(reader_short.backend._all_urls[0])

        reader_tele = Reader(model="IFS", exp="test-tco79", source="teleconnections", areas=False, fix=False)
        tele_urls = [os.path.abspath(u) for u in reader_tele.backend._all_urls]
        enso_nc_path = next((u for u in tele_urls if "enso" in u), None)
    except Exception as e:
        pytest.skip(f"Could not load IFS test catalog entries via Reader: {e}")

    for p in (long_nc_path, short_nc_path, enso_nc_path):
        if not p or not os.path.isfile(p):
            pytest.skip(f"Test NetCDF file not found on disk: {p}")

    tmpdir = tmp_path_factory.mktemp("stac_sample")
    items_dir = tmpdir / "items"
    colls_dir = tmpdir / "collections"
    items_dir.mkdir(parents=True, exist_ok=True)
    colls_dir.mkdir(parents=True, exist_ok=True)

    item_long_path = items_dir / "ifs-long-regridded-r18x9.json"
    item_short_path = items_dir / "ifs-short-ifs2d-tco79.json"
    item_enso_path = items_dir / "ifs-teleconnections-enso-test.json"
    coll_path = colls_dir / "ifs.json"

    item_long = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": "ifs-long-regridded-r18x9",
        "geometry": None,
        "properties": {
            "title": "regridded_r18x9.nc",
            "aqua:model": "IFS",
            "aqua:variables": ["2t", "ttr"],
            "start_datetime": "2020-01-20T00:00:00Z",
            "end_datetime": "2020-08-03T23:00:00Z",
        },
        "links": [
            {"rel": "collection", "href": "../collections/ifs.json", "type": "application/json"},
            {"rel": "parent", "href": "../collections/ifs.json", "type": "application/json"},
        ],
        "assets": {
            "data": {
                "href": long_nc_path,
                "title": "regridded_r18x9.nc",
                "description": "Local NetCDF test data; open with xarray.open_dataset(asset.href).",
                "type": "application/x-netcdf",
                "roles": ["data"],
            }
        },
        "collection": "ifs",
    }
    item_long_path.write_text(json.dumps(item_long, indent=2))

    item_short = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": "ifs-short-ifs2d-tco79",
        "geometry": None,
        "properties": {
            "title": "IFS2d.tco79.nc",
            "aqua:model": "IFS",
            "aqua:variables": ["2t"],
            "start_datetime": "2020-01-20T00:00:00Z",
            "end_datetime": "2020-01-20T01:00:00Z",
        },
        "links": [
            {"rel": "collection", "href": "../collections/ifs.json", "type": "application/json"},
            {"rel": "parent", "href": "../collections/ifs.json", "type": "application/json"},
        ],
        "assets": {
            "data": {
                "href": short_nc_path,
                "title": "IFS2d.tco79.nc",
                "description": "Local NetCDF test data; open with xarray.open_dataset(asset.href).",
                "type": "application/x-netcdf",
                "roles": ["data"],
            }
        },
        "collection": "ifs",
    }
    item_short_path.write_text(json.dumps(item_short, indent=2))

    item_enso = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": "ifs-teleconnections-enso-test",
        "geometry": None,
        "properties": {
            "title": "enso_test.nc",
            "aqua:model": "IFS",
            "aqua:variables": ["skt"],
            "start_datetime": "1989-01-01T00:00:00Z",
            "end_datetime": "1995-12-01T00:00:00Z",
        },
        "links": [
            {"rel": "collection", "href": "../collections/ifs.json", "type": "application/json"},
            {"rel": "parent", "href": "../collections/ifs.json", "type": "application/json"},
        ],
        "assets": {
            "data": {
                "href": enso_nc_path,
                "title": "enso_test.nc",
                "description": "Local NetCDF test data; open with xarray.open_dataset(asset.href).",
                "type": "application/x-netcdf",
                "roles": ["data"],
            }
        },
        "collection": "ifs",
    }
    item_enso_path.write_text(json.dumps(item_enso, indent=2))

    coll = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": "ifs",
        "title": "AQUA IFS test data",
        "description": "IFS native, regridded, and teleconnection test datasets.",
        "license": "proprietary",
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [["1989-01-01T00:00:00Z", "2020-08-03T23:00:00Z"]]},
        },
        "summaries": {
            "aqua:model": ["IFS"],
            "assets:type": ["application/x-netcdf"],
        },
        "links": [
            {"rel": "self", "href": "./ifs.json", "type": "application/json"},
            {"rel": "item", "href": "../items/ifs-long-regridded-r18x9.json", "type": "application/geo+json"},
            {"rel": "item", "href": "../items/ifs-short-ifs2d-tco79.json", "type": "application/geo+json"},
            {"rel": "item", "href": "../items/ifs-teleconnections-enso-test.json", "type": "application/geo+json"},
        ],
    }
    coll_path.write_text(json.dumps(coll, indent=2))

    return {
        "item_path": str(item_long_path),
        "collection_path": str(coll_path),
    }


@pytest.fixture
def sample_stac_netcdf_path(stac_sample_data):
    """Path to the generated IFS STAC item."""
    return stac_sample_data["item_path"]


@pytest.fixture
def sample_stac_collection_path(stac_sample_data):
    """Path to the generated IFS STAC collection."""
    return stac_sample_data["collection_path"]


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


@pytest.mark.aqua
def test_stac_read_collection(sample_stac_collection_path):
    """Test reading a NetCDF-backed STAC item through a STAC collection."""
    source = IntakeSTACSource(
        urlpath=sample_stac_collection_path,
        item="ifs-long-regridded-r18x9",
        asset="data",
    )

    # Check exposed metadata from collection and item
    assert source.metadata.get("aqua:model") == "IFS"
    assert source.metadata.get("id") == "ifs-long-regridded-r18x9"
    assert source.metadata.get("collection_title") == "AQUA IFS test data"
    assert "collection_info" in source.metadata
    assert source.collection_info["id"] == "ifs"
    assert source.item_name == "ifs-long-regridded-r18x9"
    assert source.asset_name == "data"

    # Read dataset
    ds = source.read()
    assert isinstance(ds, xr.Dataset)
    assert "2t" in ds.data_vars
    assert "ttr" in ds.data_vars


@pytest.mark.aqua
def test_stac_collection_missing_item_error(sample_stac_collection_path):
    """Test that reading directly from a collection without selecting an item raises ValueError."""
    source = IntakeSTACSource(urlpath=sample_stac_collection_path)
    assert source.item_name is None
    assert "ifs-short-ifs2d-tco79" in source.keys()
    with pytest.raises(ValueError, match="cannot be read directly"):
        source.read()


@pytest.mark.aqua
def test_stac_collection_unknown_item_error(sample_stac_collection_path):
    """Test that requesting an unknown item from a collection raises KeyError with available items."""
    with pytest.raises(KeyError, match="Item 'non-existent-item' not found in STAC collection"):
        IntakeSTACSource(
            urlpath=sample_stac_collection_path,
            item="non-existent-item",
        )


@pytest.mark.aqua
def test_stac_collection_catalog_yaml(sample_stac_collection_path):
    """Test loading a STAC collection entry with an item argument from an Intake YAML catalog."""
    catalog_content = f"""
sources:
  ifs_collection_item:
    description: Test STAC collection entry with item argument
    driver: stac
    args:
      urlpath: "{sample_stac_collection_path}"
      item: "ifs-long-regridded-r18x9"
      asset: "data"
"""
    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as f:
        f.write(catalog_content)
        f.flush()

        cat = intake.open_catalog(f.name)
        assert "ifs_collection_item" in cat

        entry = cat["ifs_collection_item"]
        assert entry.metadata.get("collection_title") == "AQUA IFS test data"
        assert entry.metadata.get("id") == "ifs-long-regridded-r18x9"

        ds = entry.read()
        assert isinstance(ds, xr.Dataset)
        assert "2t" in ds.data_vars


@pytest.mark.aqua
def test_stac_collection_dynamic_item_selection(sample_stac_collection_path):
    """Test dynamically selecting different items using entry(item=...) in an Intake catalog."""
    catalog_content = f"""
sources:
  ifs_collection:
    description: IFS collection with selectable item
    driver: stac
    parameters:
      item:
        description: Item to load
        type: str
        default: ifs-long-regridded-r18x9
    args:
      urlpath: "{sample_stac_collection_path}"
      item: "{{{{ item }}}}"
      asset: "data"
"""
    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as f:
        f.write(catalog_content)
        f.flush()

        cat = intake.open_catalog(f.name)

        # 1. Default item
        e1 = cat["ifs_collection"]
        assert e1.item_name == "ifs-long-regridded-r18x9"

        # 2. Select another item via entry call
        e2 = cat["ifs_collection"](item="ifs-short-ifs2d-tco79")
        assert e2.item_name == "ifs-short-ifs2d-tco79"
        ds2 = e2.read()
        assert "2t" in ds2.data_vars

        # 3. Select teleconnections item
        e3 = cat.ifs_collection(item="ifs-teleconnections-enso-test")
        assert e3.item_name == "ifs-teleconnections-enso-test"
        ds3 = e3.read()
        assert "skt" in ds3.data_vars


@pytest.mark.aqua
def test_stac_collection_hierarchical_indexing(sample_stac_collection_path):
    """Test hierarchical indexing syntax cat['collection']['item']['asset'].read()."""
    catalog_content = f"""
sources:
  ifs:
    description: IFS collection
    driver: stac
    args:
      urlpath: "{sample_stac_collection_path}"
"""
    with tempfile.NamedTemporaryFile("w", suffix=".yaml") as f:
        f.write(catalog_content)
        f.flush()

        cat = intake.open_catalog(f.name)
        ifs = cat["ifs"]

        # Inspect collection
        assert "ifs-short-ifs2d-tco79" in ifs
        assert "ifs-long-regridded-r18x9" in ifs.keys()

        # Reading directly on collection raises ValueError
        with pytest.raises(ValueError, match="cannot be read directly"):
            ifs.read()

        # Level 1: cat["ifs"]["item"].read()
        item_source = ifs["ifs-short-ifs2d-tco79"]
        assert item_source.item_name == "ifs-short-ifs2d-tco79"
        assert item_source.asset_name == "data"
        assert "data" in item_source.keys()

        ds1 = item_source.read()
        assert isinstance(ds1, xr.Dataset)
        assert "2t" in ds1.data_vars

        # Level 2: cat["ifs"]["item"]["asset"].read()
        ds2 = cat["ifs"]["ifs-short-ifs2d-tco79"]["data"].read()
        assert isinstance(ds2, xr.Dataset)
        assert "2t" in ds2.data_vars

        # Other items
        ds3 = cat["ifs"]["ifs-teleconnections-enso-test"]["data"].read()
        assert isinstance(ds3, xr.Dataset)
        assert "skt" in ds3.data_vars

        # Invalid asset
        with pytest.raises(KeyError, match="not an available item in collection"):
            cat["ifs"]["ifs-short-ifs2d-tco79"]["nonexistent"]
