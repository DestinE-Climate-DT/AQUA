"""Tests for the AQUA STAC intake driver."""

import json
import tempfile

import intake
import numpy as np
import pytest
import xarray as xr
from dask.base import is_dask_collection

from aqua import Reader
from aqua.core.backend import BackendFactory
from aqua.core.backend.catalog_mixin import CatalogMixin
from aqua.core.configurer import ConfigContext
from aqua.core.intake_drivers import IntakeSTACSource
from aqua.core.intake_drivers.stac.stac import _resolve_href

xr.set_options(keep_attrs=True)

pytestmark = pytest.mark.aqua


@pytest.fixture(scope="session")
def sample_stac_netcdf_path(stac_catalogue_dir):
    """Path to the generated IFS STAC item."""
    return str(stac_catalogue_dir / "items" / "ifs" / "ifs-long-regridded-r18x9.json")


@pytest.fixture(scope="session")
def sample_stac_collection_path(stac_catalogue_dir):
    """Path to the generated IFS STAC collection."""
    return str(stac_catalogue_dir / "collections" / "ifs.json")


@pytest.fixture
def selectable_stac_source(tmp_path, sample_stac_netcdf_path, request):
    """Build selectable metadata around an existing NetCDF asset, directly or through YAML."""
    with open(sample_stac_netcdf_path, encoding="utf-8") as handle:
        template = json.load(handle)
    asset = template["assets"]["data"]
    href = _resolve_href(sample_stac_netcdf_path, asset["href"])
    links = []
    for item_id, data_key in (("first", "data"), ("second", "primary"), ("data", "data")):
        assets = {
            data_key: {**asset, "href": href, "roles": ["data"]},
            "shared": {**asset, "href": href, "roles": []},
            f"preview-{item_id}": {**asset, "href": href, "roles": ["thumbnail"]},
        }
        if item_id == "first":
            assets["only-first"] = {**asset, "href": href, "roles": []}
        item_path = tmp_path / f"{item_id}.json"
        item_path.write_text(json.dumps({**template, "id": item_id, "assets": assets}))
        links.append({"rel": "item", "id": item_id, "href": item_path.name})
    collection_path = tmp_path / "collection.json"
    collection_path.write_text(json.dumps({"type": "Collection", "id": "selectable", "links": links}))
    if request.param == "direct":
        return IntakeSTACSource(urlpath=str(collection_path), item="first")
    catalog_path = tmp_path / "catalog.yaml"
    catalog_path.write_text(
        f"sources:\n  selectable:\n    driver: stac\n    args:\n"
        f"      urlpath: {json.dumps(str(collection_path))}\n      item: first\n      asset: shared\n"
    )
    return intake.open_catalog(str(catalog_path))["selectable"](asset=None)


class TestSTACHrefResolution:
    """Resolve STAC links without depending on the remote filesystem."""

    @pytest.mark.parametrize(
        ("base_urlpath", "href", "expected"),
        [
            ("https://example.org/catalog/item.json", "../data.nc", "https://example.org/data.nc"),
            ("s3://bucket/catalog/item.json", "../data.nc", "s3://bucket/data.nc"),
            ("gs://bucket/catalog/item.json", "./data.nc", "gs://bucket/catalog/data.nc"),
            ("memory://bucket/catalog/item.json", "../data.nc", "memory://bucket/data.nc"),
            ("memory:///catalog/item.json", "../data.nc", "memory:///data.nc"),
            ("file:///catalog/item.json", "../data.nc", "file:///data.nc"),
            ("https://example.org/catalog/item.json", "/data/file.nc", "https://example.org/data/file.nc"),
            ("s3://bucket/catalog/item.json", "/data/file.nc", "s3://bucket/data/file.nc"),
            ("https://example.org/catalog/item.json", "//cdn.example.org/file.nc", "https://cdn.example.org/file.nc"),
            ("s3://bucket/catalog/item.json", "//other-bucket/data.nc", "s3://other-bucket/data.nc"),
            ("s3://bucket", "../data.nc", "s3://bucket/data.nc"),
            ("s3://bucket/catalog/item.json", "../store.zarr/", "s3://bucket/store.zarr/"),
            (
                "https://example.org/catalog/item.json?old=1",
                "../data.nc?version=2#part",
                "https://example.org/data.nc?version=2#part",
            ),
            ("s3://bucket/item.json?version=1", "#part", "s3://bucket/item.json?version=1#part"),
            ("s3://bucket/item.json?version=1", "?version=2", "s3://bucket/item.json?version=2"),
            ("s3://bucket/item.json", "https://example.org/data.nc", "https://example.org/data.nc"),
            ("https://example.org/item.json", "s3://bucket/data.nc", "s3://bucket/data.nc"),
            ("https://example.org/item.json", "file:///data/file.nc", "file:///data/file.nc"),
            ("/catalog/item.json", "../data.nc", "/data.nc"),
            ("/catalog/item.json", "/data/file.nc", "/data/file.nc"),
            ("/catalog/item.json", "file:///data/file.nc", "file:///data/file.nc"),
            ("", "../data.nc", "../data.nc"),
        ],
    )
    def test_resolve_href(self, base_urlpath, href, expected):
        """Preserve URL components while resolving local and remote references."""
        assert _resolve_href(base_urlpath, href) == expected

    def test_relative_local_base(self, tmp_path, monkeypatch):
        """Resolve a relative filesystem base against the working directory."""
        monkeypatch.chdir(tmp_path)
        assert _resolve_href("catalog/item.json", "../data.nc") == str(tmp_path / "data.nc")


class TestSTACDriver:
    """Driver registration and individual STAC items."""

    def test_stac_driver_registration(self):
        """Verify that 'stac' driver is registered in intake."""
        registered = intake.registry.drivers.registered()
        assert "stac" in registered
        assert registered["stac"] is IntakeSTACSource

    def test_stac_read_netcdf(self, sample_stac_netcdf_path):
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

        with pytest.raises(KeyError, match="Asset 'missing' not found"):
            source["missing"]

    def test_stac_read_zarr(self, tmp_path):
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

    def test_stac_catalog_yaml(self, sample_stac_netcdf_path):
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

    def test_stac_asset_selection_errors(self, tmp_path):
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

        assert IntakeSTACSource(urlpath=str(json_path2)).asset_name == "preview"

        role_path = tmp_path / "item_with_roles.json"
        role_path.write_text(
            json.dumps(
                {
                    "id": "role-item",
                    "type": "Feature",
                    "assets": {
                        "preview": {"href": "preview.png", "roles": ["thumbnail"]},
                        "primary": {"href": "data.nc", "roles": ["data"]},
                    },
                }
            )
        )
        assert IntakeSTACSource(urlpath=str(role_path)).asset_name == "primary"

        missing_href_path = tmp_path / "item_missing_href.json"
        missing_href_path.write_text(json.dumps({"id": "missing-href", "type": "Feature", "assets": {"data": {}}}))
        with pytest.raises(ValueError, match="has no 'href'"):
            IntakeSTACSource(urlpath=str(missing_href_path))


class TestSTACCollections:
    """Selecting and reading items from collections."""

    @pytest.mark.parametrize("selectable_stac_source", ["direct", "yaml"], indirect=True)
    @pytest.mark.parametrize("asset, expected", [(None, "primary"), ("shared", "shared"), ("thumbnail", "preview-second")])
    def test_item_change_preserves_asset_selector(self, selectable_stac_source, asset, expected):
        """Re-evaluate inferred defaults and roles without losing an explicit asset selector."""
        source = selectable_stac_source(asset=asset)(chunks={"time": 1})
        changed = source(item="second")
        assert changed.item_name == "second"
        assert changed.asset_name == expected
        assert changed.asset_arg == asset
        assert source(item="second", asset="shared").asset_name == "shared"

    @pytest.mark.parametrize("selectable_stac_source", ["direct", "yaml"], indirect=True)
    def test_item_change_rejects_missing_explicit_asset(self, selectable_stac_source):
        """An explicit asset must not silently fall back to the new item's default."""
        source = selectable_stac_source(asset="only-first")
        with pytest.raises(KeyError, match="Asset 'only-first' not found"):
            source(item="second")
        assert source(item="second", asset=None).asset_name == "primary"

    @pytest.mark.parametrize("selectable_stac_source", ["direct", "yaml"], indirect=True)
    @pytest.mark.parametrize("selection", ["index", "call", "entry"])
    def test_item_navigation_exposes_only_assets(self, selectable_stac_source, selection):
        """Item views agree on indexing, iteration, membership, and Intake entries."""
        collection = selectable_stac_source(chunks={"time": 1})
        assert set(collection.keys()) == {"first", "second", "data"}
        assert set(collection._entries) == set(collection.keys())
        assert "shared" not in collection
        with pytest.raises(KeyError, match="Item 'shared' not found"):
            collection["shared"]
        item = {
            "index": lambda: collection["first"],
            "call": lambda: collection(item="first"),
            "entry": lambda: collection._entries["first"](),
        }[selection]()
        item = item(chunks={"time": 1})
        assert set(item.keys()) == {"data", "shared", "preview-first", "only-first"}
        assert set(item._entries) == set(item.keys())
        assert all(key in item for key in item.keys())
        assert "second" not in item
        with pytest.raises(KeyError, match="Asset 'second' not found"):
            item["second"]
        assert item["data"].item_name == "first"
        assert item["data"].asset_arg == "data"
        assert item._entries["data"]().item_name == "first"

    def test_stac_read_collection(self, sample_stac_collection_path):
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

    def test_stac_collection_missing_item_error(self, sample_stac_collection_path):
        """Test that reading directly from a collection without selecting an item raises ValueError."""
        source = IntakeSTACSource(urlpath=sample_stac_collection_path)
        assert source.item_name is None
        assert "ifs-short-ifs2d-tco79" in source.keys()
        with pytest.raises(ValueError, match="cannot be read directly"):
            source.read()
        with pytest.raises(ValueError, match="cannot be read directly"):
            source.to_dask()

    def test_stac_collection_unknown_item_error(self, sample_stac_collection_path):
        """Test that requesting an unknown item from a collection raises KeyError with available items."""
        with pytest.raises(KeyError, match="Item 'non-existent-item' not found in STAC collection"):
            IntakeSTACSource(
                urlpath=sample_stac_collection_path,
                item="non-existent-item",
            )
        with pytest.raises(KeyError, match="Item 'non-existent-item' not found in STAC collection"):
            IntakeSTACSource(urlpath=sample_stac_collection_path)["non-existent-item"]

    def test_stac_collection_catalog_yaml(self, sample_stac_collection_path):
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

    def test_stac_collection_dynamic_item_selection(self, sample_stac_collection_path):
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

    def test_stac_collection_hierarchical_indexing(self, sample_stac_collection_path):
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
            with pytest.raises(KeyError, match="Asset 'nonexistent' not found"):
                cat["ifs"]["ifs-short-ifs2d-tco79"]["nonexistent"]


class TestSTACCollectionDiscovery:
    """Collection links, assets, and Intake entries."""

    def test_stac_collection_child_links_and_roles(self, tmp_path):
        """Test STAC collections with rel='child' links, title-based items, and role-based asset matching."""
        # Create item file
        item_file = tmp_path / "item.json"
        item_data = {
            "id": "my-item-id",
            "type": "Feature",
            "stac_version": "1.0.0",
            "properties": {"title": "My Item"},
            "assets": {
                "dkrz-disk": {
                    "href": "disk_data.nc",
                    "roles": ["data"],
                    "type": "application/x-netcdf",
                },
                "eerie-cloud": {
                    "href": "cloud_data.nc",
                    "roles": ["data"],
                    "type": "application/x-netcdf",
                },
                "preview": {
                    "href": "preview.png",
                    "roles": ["thumbnail"],
                    "type": "image/png",
                },
            },
        }
        with open(item_file, "w", encoding="utf-8") as f:
            json.dump(item_data, f)

        # Create collection with rel='child', href ending in /stac, and title
        coll_file = tmp_path / "coll.json"
        coll_data = {
            "id": "my-collection",
            "type": "Collection",
            "stac_version": "1.0.0",
            "links": [
                {
                    "rel": "child",
                    "href": str(item_file),
                    "title": "my-dataset.2D_hourly",
                }
            ],
        }
        with open(coll_file, "w", encoding="utf-8") as f:
            json.dump(coll_data, f)

        # 1. Collection lists item by title
        source = IntakeSTACSource(urlpath=str(coll_file))
        assert "my-dataset.2D_hourly" in source.keys()

        # 2. Select item by title and asset by key
        item_source = IntakeSTACSource(urlpath=str(coll_file))(item="my-dataset.2D_hourly", asset="preview")
        assert item_source.asset_name == "preview"
        assert item_source["preview"] is item_source
        assert item_source["dkrz-disk"].asset_name == "dkrz-disk"

        # 3. Role-based matching: unique role 'thumbnail' matches 'preview'
        item_source_role = IntakeSTACSource(urlpath=str(coll_file), item="my-dataset.2D_hourly", asset="thumbnail")
        assert item_source_role.asset_name == "preview"

        # 4. Ambiguous role 'data' raises informative KeyError with available assets
        with pytest.raises(KeyError, match="matches multiple assets by role"):
            IntakeSTACSource(urlpath=str(coll_file), item="my-dataset.2D_hourly", asset="data")

    @pytest.mark.parametrize("link_style", ["items", "unlinked"])
    def test_stac_collection_item_fallbacks(self, tmp_path, link_style):
        """Resolve items from a STAC API items link or a sibling item file."""
        item_id = "fallback-item"
        if link_style == "items":
            item_dir = tmp_path / "items"
            item_dir.mkdir()
            item_path = item_dir / item_id
            links = [{"rel": "items", "href": "./items"}]
        else:
            item_path = tmp_path / f"{item_id}.json"
            links = []

        item_path.write_text(
            json.dumps(
                {
                    "id": item_id,
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "properties": {"title": "Fallback item"},
                    "assets": {"data": {"href": "data.nc", "type": "application/x-netcdf"}},
                }
            )
        )
        collection_path = tmp_path / "collection.json"
        collection_path.write_text(json.dumps({"id": "fallback", "type": "Collection", "links": links}))

        source = IntakeSTACSource(urlpath=str(collection_path), item=item_id)
        assert source.item_base_urlpath == str(item_path)
        assert source.metadata["id"] == item_id
        assert source.asset_name == "data"

    def test_stac_entries_interface(self, sample_stac_collection_path):
        """Test that IntakeSTACSource exposes _entries and entries compatible with catalog workflows."""
        source = IntakeSTACSource(urlpath=sample_stac_collection_path)

        # Collection level _entries
        assert hasattr(source, "_entries")
        assert hasattr(source, "entries")
        assert "ifs-short-ifs2d-tco79" in source._entries
        assert "ifs-short-ifs2d-tco79" in source.entries

        entry = source._entries["ifs-short-ifs2d-tco79"]
        assert entry.name == "ifs-short-ifs2d-tco79"
        assert entry._driver == "stac"
        assert entry._user_parameters == []
        assert entry.describe()["driver"] == "stac"

        # Instantiating through entry() returns child item source
        item_source = entry()
        assert item_source.item_name == "ifs-short-ifs2d-tco79"
        assert hasattr(item_source, "_entries")
        assert "data" in item_source

        # Item level _entries lists assets
        assert "data" in item_source._entries
        asset_entry = item_source._entries["data"]
        assert asset_entry.name == "data"
        assert asset_entry._driver == "stac"
        assert asset_entry.describe()["description"] == "STAC asset data"

        # Calling asset_entry() with kwargs returns instantiated asset source
        asset_source = asset_entry(chunks={"time": 1})
        assert asset_source.asset_name == "data"
        assert asset_source.extra_kwargs.get("chunks") == {"time": 1}
        assert getattr(asset_source, "_entry", None) is not None

    def test_stac_source_reselects_item(self, sample_stac_collection_path):
        """A collection source can select an item and describe its available assets."""
        collection = IntakeSTACSource(urlpath=sample_stac_collection_path)
        assert collection() is collection
        assert "ifs-short-ifs2d-tco79" in collection.describe()["available_items"]

        item = collection(item="ifs-short-ifs2d-tco79", asset="data")
        assert item.item_name == "ifs-short-ifs2d-tco79"
        assert item.asset_name == "data"
        assert item.describe()["available_assets"] == ["data"]


class TestSTACBackendIntegration:
    """Backend selectors and item metadata."""

    @pytest.mark.parametrize(
        ("exp", "item", "variable", "grid", "fixer"),
        [
            ("ifs", "ifs-long-regridded-r18x9", "2t", "lon-lat", "ifs-ci-test"),
            ("ifs", "ifs-short-ifs2d-tco79", "2t", "tco79", False),
            ("nemo", "nemo-eorca1-eorca1-sst-2d", "sst", "eORCA1-2d", False),
            ("nemo", "nemo-eorca1-eorca1-salinity-3d", "avg_so", "test-eORCA1-3d", False),
            ("ifs-items", "ifs-long-regridded-r18x9", "2t", "lon-lat", "ifs-ci-test"),
            ("ifs-items", "ifs-short-ifs2d-tco79", "2t", "tco79", None),
        ],
    )
    @pytest.mark.parametrize("use_aliases", [False, True], ids=["model-exp-source", "collection-item-asset"])
    def test_reader_stac_catalogue(self, stac_catalogue_dir, exp, item, variable, grid, fixer, use_aliases):
        """Retrieve generated STAC examples through the installed CI catalogue and Reader."""
        selectors = (
            {"collection": "stac", "item": exp, "asset": item}
            if use_aliases
            else {"model": "stac", "exp": exp, "source": item}
        )
        reader = Reader(catalog="ci", chunks={"time": 1}, areas=False, **selectors)
        data = reader.retrieve(var=variable)

        assert isinstance(reader.backend.esmcat, IntakeSTACSource)
        assert reader.backend.esmcat.item_name == item
        assert reader.backend.esmcat.asset_name == "data"
        assert reader.metadata["aqua:model"] == item.split("-", 1)[0].upper()
        assert reader.src_grid_name == grid
        assert reader.fixer_name == fixer
        assert reader.fix is (fixer is not False)
        assert isinstance(data, xr.Dataset)
        assert list(data.data_vars) == [variable]
        assert all(size > 0 for size in data.sizes.values())
        assert is_dask_collection(data[variable])
        if "time" in data.dims:
            assert set(data[variable].chunksizes["time"]) == {1}
        assert data.aqua.instance is reader
        assert data.attrs["history"]
        first_dim = data[variable].dims[0]
        assert data.isel({first_dim: slice(0, 1)}).attrs["history"] == data.attrs["history"]
        for key, value in {"catalog": "ci", "model": "stac", "exp": exp, "source": item}.items():
            assert data[variable].attrs[f"AQUA_{key}"] == value
        if fixer:
            assert data[variable].attrs["donald"] == "duck"

    def test_backend_factory_stac_aliases(self):
        """STAC naming aliases stay in sync with the established backend selectors."""
        factory = BackendFactory(configurer=ConfigContext(), collection="IFS", item="test-tco79", asset="short")
        assert (factory.model, factory.exp, factory.source) == ("IFS", "test-tco79", "short")

        factory.collection = "NEMO"
        factory.item = "eorca1"
        factory.asset = "sst"
        assert (factory.model, factory.exp, factory.source) == ("NEMO", "eorca1", "sst")
        assert (factory.collection, factory.item, factory.asset) == ("NEMO", "eorca1", "sst")

        same_values = BackendFactory(
            configurer=ConfigContext(),
            model="IFS",
            exp="test-tco79",
            source="short",
            collection="IFS",
            item="test-tco79",
            asset="short",
        )
        assert (same_values.collection, same_values.item, same_values.asset) == ("IFS", "test-tco79", "short")

    @pytest.mark.parametrize(
        ("canonical", "alias"),
        [("model", "collection"), ("exp", "item"), ("source", "asset")],
    )
    def test_backend_factory_rejects_conflicting_stac_aliases(self, canonical, alias):
        """Do not silently choose between conflicting selector names."""
        selectors = {"model": "IFS", "exp": "test-tco79", "source": "short", alias: "different"}
        with pytest.raises(ValueError, match=f"Conflicting values provided for {canonical}"):
            BackendFactory(configurer=ConfigContext(), **selectors)

    def test_catalog_mixin_stac_aliases(self):
        """Changing an Intake backend alias updates its canonical selector."""
        backend = CatalogMixin()
        backend.model, backend.exp, backend.source = "IFS", "test-tco79", "short"
        assert (backend.collection, backend.item, backend.asset) == ("IFS", "test-tco79", "short")

        backend.collection = "NEMO"
        backend.item = "eorca1"
        backend.asset = "sst"
        assert (backend.model, backend.exp, backend.source) == ("NEMO", "eorca1", "sst")

    def test_backend_factory_stac_metadata_dict(self, sample_stac_collection_path):
        """Test BackendFactory.get_metadata resolves dict-based fixer_name and source_grid_name per item for STAC."""
        configurer = ConfigContext()
        factory = BackendFactory(configurer=configurer, model="ifs", exp="item1", source="data")
        factory.backend_type = "intake_stac"

        # Case 1: Real IntakeSTACSource with item_name matching a key in the dictionary
        stac_source = IntakeSTACSource(urlpath=sample_stac_collection_path, item="ifs-short-ifs2d-tco79", asset="data")
        factory.esmcat = stac_source
        factory.metadata = {
            "fixer_name": {
                "ifs-short-ifs2d-tco79": "ifs-short-fixer",
                "ifs-long-regridded-r18x9": "ifs-long-fixer",
            },
            "source_grid_name": {
                "ifs-short-ifs2d-tco79": "tco79-grid",
                "ifs-long-regridded-r18x9": "r18x9-grid",
            },
        }

        fixer, grid, conv, dm = factory.get_metadata()
        assert fixer == "ifs-short-fixer"
        assert grid == "tco79-grid"

        # Case 2: Another item matches its respective entry in the dictionary
        factory.esmcat = IntakeSTACSource(urlpath=sample_stac_collection_path, item="ifs-long-regridded-r18x9", asset="data")
        fixer, grid, conv, dm = factory.get_metadata()
        assert fixer == "ifs-long-fixer"
        assert grid == "r18x9-grid"

        # Case 3: Item not found in the dictionary resolves to None
        factory.esmcat = IntakeSTACSource(
            urlpath=sample_stac_collection_path, item="ifs-teleconnections-enso-test", asset="data"
        )
        fixer, grid, conv, dm = factory.get_metadata()
        assert fixer is None
        assert grid is None

        # Case 4: item is None on esmcat (e.g. collection-level source)
        factory.esmcat = IntakeSTACSource(urlpath=sample_stac_collection_path)
        assert factory.esmcat.item_name is None
        fixer, grid, conv, dm = factory.get_metadata()
        assert fixer is None
        assert grid is None

        # Case 5: Direct arguments passed as dicts to get_metadata
        factory.esmcat = stac_source
        fixer, grid, conv, dm = factory.get_metadata(
            fixer_name={"ifs-short-ifs2d-tco79": "arg-fixer"},
            src_grid_name={"ifs-short-ifs2d-tco79": "arg-grid"},
        )
        assert fixer == "arg-fixer"
        assert grid == "arg-grid"

        # Case 6: Non-stac driver does not resolve dictionaries by item
        factory.backend_type = "intake_netcdf"
        fixer, grid, conv, dm = factory.get_metadata()
        assert fixer == factory.metadata["fixer_name"]
        assert grid == factory.metadata["source_grid_name"]
