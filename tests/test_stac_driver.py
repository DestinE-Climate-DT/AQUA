"""Tests for the AQUA STAC intake driver."""

import json
from unittest.mock import Mock

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
from aqua.core.intake_drivers.stac.stac import _find_item_in_collection, _resolve_href

xr.set_options(keep_attrs=True)

pytestmark = pytest.mark.aqua


def _write_json(path, document):
    """Write temporary STAC/catalog metadata and return its path."""
    path.write_text(json.dumps(document))
    return str(path)


def _catalog(path, urlpath, *, metadata=None, parameters=None, **args):
    """Open a real Intake YAML catalog with one STAC entry."""
    entry = {
        "driver": "stac",
        "args": {"urlpath": str(urlpath), **args},
        "metadata": metadata or {},
        "parameters": parameters or {},
    }
    return intake.open_catalog(_write_json(path, {"sources": {"stac": entry}}))


@pytest.fixture(scope="session")
def sample_stac_netcdf_path(stac_catalogue_dir):
    """Path to the generated IFS STAC item."""
    return str(stac_catalogue_dir / "items" / "ifs" / "ifs-long-regridded-r18x9.json")


@pytest.fixture(scope="session")
def sample_stac_collection_path(stac_catalogue_dir):
    """Path to the generated IFS STAC collection."""
    return str(stac_catalogue_dir / "collections" / "ifs.json")


@pytest.fixture
def selectable_collection(tmp_path, sample_stac_netcdf_path):
    """Wrap an existing asset with distinct defaults, roles, and an item/asset name collision."""
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
        _write_json(tmp_path / f"{item_id}.json", {**template, "id": item_id, "assets": assets})
        links.append({"rel": "item", "id": item_id, "href": f"{item_id}.json"})
    return _write_json(tmp_path / "collection.json", {"type": "Collection", "id": "selectable", "links": links})


@pytest.fixture
def selectable_stac_source(tmp_path, selectable_collection, request):
    """Exercise both direct sources and Intake reconstruction, including an explicit None override."""
    if request.param == "direct":
        return IntakeSTACSource(urlpath=selectable_collection, item="first")
    return _catalog(tmp_path / "catalog.yaml", selectable_collection, item="first", asset="shared")["stac"](asset=None)


@pytest.fixture
def stac_factory(sample_stac_collection_path):
    """A real STAC source with per-item fixer/grid metadata."""
    factory = BackendFactory(configurer=ConfigContext(), model="ifs", exp="item1", source="data")
    factory.backend_type = "intake_stac"
    factory.esmcat = IntakeSTACSource(urlpath=sample_stac_collection_path, item="ifs-short-ifs2d-tco79", asset="data")
    factory.metadata = {
        "fixer_name": {"ifs-short-ifs2d-tco79": "ifs-short-fixer", "ifs-long-regridded-r18x9": "ifs-long-fixer"},
        "source_grid_name": {"ifs-short-ifs2d-tco79": "tco79-grid", "ifs-long-regridded-r18x9": "r18x9-grid"},
    }
    return factory


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


class TestSTACItemLookup:
    """Only missing resources should trigger another lookup candidate."""

    @pytest.mark.parametrize(
        ("lookup", "error"),
        [
            ("linked", PermissionError("denied")),
            ("api", OSError("connection failed")),
            ("sibling", json.JSONDecodeError("bad JSON", "", 0)),
        ],
    )
    def test_lookup_preserves_errors(self, monkeypatch, lookup, error):
        links = {
            "linked": [{"rel": "item", "id": "requested", "href": "item.json"}],
            "api": [{"rel": "items", "href": "items"}],
            "sibling": [],
        }[lookup]
        expected_url = {
            "linked": "https://example.org/item.json",
            "api": "https://example.org/items/requested",
            "sibling": "https://example.org/requested",
        }[lookup]
        opened = Mock(side_effect=error)
        monkeypatch.setattr("aqua.core.intake_drivers.stac.stac.fsspec.open", opened)

        with pytest.raises(type(error)) as caught:
            _find_item_in_collection({"links": links}, "requested", "https://example.org/collection.json")

        assert caught.value is error
        assert any(expected_url in note for note in caught.value.__notes__)
        opened.assert_called_once_with(expected_url, "r")

    @pytest.mark.parametrize("api", [False, True])
    def test_missing_item_remains_key_error(self, monkeypatch, api):
        collection = {"id": "test", "links": [{"rel": "items", "href": "items"}] if api else []}
        opened = Mock(side_effect=FileNotFoundError("missing"))
        monkeypatch.setattr("aqua.core.intake_drivers.stac.stac.fsspec.open", opened)

        with pytest.raises(KeyError, match="Item 'requested' not found in STAC collection 'test'"):
            _find_item_in_collection(collection, "requested", "https://example.org/collection.json")

        assert opened.call_count == (3 if api else 2)

    @pytest.mark.parametrize("api", [False, True])
    def test_missing_candidates_allow_sibling_lookup(self, tmp_path, api):
        item = {"type": "Feature", "id": "requested", "assets": {}}
        item_path = tmp_path / "requested.json"
        item_path.write_text(json.dumps(item))
        collection = {"links": [{"rel": "items", "href": "items"}] if api else []}

        loaded, path = _find_item_in_collection(collection, "requested", str(tmp_path / "collection.json"))

        assert loaded == item
        assert path == str(item_path)


class TestSTACDriver:
    """Driver registration and individual STAC items."""

    @pytest.mark.parametrize("argument", ["urlpath", "path"])
    @pytest.mark.parametrize("document", [{}, {"type": "Feature", "id": "example", "assets": {}}])
    def test_rejects_dictionary_input(self, argument, document):
        """Require a document location for both source path arguments."""
        with pytest.raises(TypeError, match="must be a STAC JSON path or URL, not a dictionary"):
            IntakeSTACSource(**{argument: document})

    def test_stac_driver_registration(self):
        """Verify that 'stac' driver is registered in intake."""
        registered = intake.registry.drivers.registered()
        assert "stac" in registered
        assert registered["stac"] is IntakeSTACSource

    @pytest.mark.parametrize(
        ("document", "catalogued", "method"),
        [
            ("item", False, "read"),
            ("item", True, "discover"),
            ("collection", False, "to_dask"),
            ("collection", True, "read_chunked"),
        ],
    )
    def test_netcdf_reads(self, tmp_path, sample_stac_netcdf_path, sample_stac_collection_path, document, catalogued, method):
        """Direct/YAML items and collections preserve metadata and eager/lazy read behavior."""
        urlpath = sample_stac_collection_path if document == "collection" else sample_stac_netcdf_path
        args = {"asset": "data", "chunks": {"time": 1}}
        if document == "collection":
            args["item"] = "ifs-long-regridded-r18x9"
        source = (
            _catalog(tmp_path / "catalog.yaml", urlpath, metadata={"fixer_name": "catalog-fixer"}, **args)["stac"]
            if catalogued
            else IntakeSTACSource(urlpath=urlpath, **args)
        )
        assert source.metadata["aqua:model"] == "IFS"
        assert source.metadata["id"] == "ifs-long-regridded-r18x9"
        assert source.metadata["collection"] == "ifs"
        assert "2t" in source.metadata["aqua:variables"]
        assert source.asset_name == "data"
        assert "href" in source.asset_info
        if document == "collection":
            assert source.collection_info["id"] == "ifs"
            assert source.metadata["collection_title"] == "AQUA IFS test data"
        if catalogued:
            assert source.metadata["fixer_name"] == "catalog-fixer"
        with getattr(source, method)() as data:
            assert isinstance(data, xr.Dataset)
            assert {"2t", "ttr"} <= set(data.data_vars)
            lazy = method in ("to_dask", "read_chunked")
            assert is_dask_collection(data["2t"]) == lazy
            if lazy:
                assert set(data["2t"].chunksizes["time"]) == {1}

    @pytest.mark.parametrize(
        ("method", "error_type", "message"),
        [
            ("read", ValueError, "not enough values to unpack (expected 2, got 1)"),
            ("to_dask", ValueError, "not enough values to unpack (expected 2, got 1)"),
            ("discover", ValueError, "not enough values to unpack (expected 2, got 1)"),
            ("read_chunked", ValueError, "not enough values to unpack (expected 2, got 1)"),
            ("read", PermissionError, "access denied"),
            ("to_dask", OSError, "connection failed"),
        ],
    )
    def test_read_preserves_error(self, sample_stac_netcdf_path, method, error_type, message):
        """Keep the original read exception and identify the asset's resolved location."""
        source = IntakeSTACSource(urlpath=sample_stac_netcdf_path, asset="data")
        error = error_type(message)
        source.reader = Mock()
        source.reader.return_value = source.reader
        source.reader.read.side_effect = error

        with pytest.raises(error_type) as caught:
            getattr(source, method)()

        assert caught.value is error
        assert str(caught.value) == message
        note = caught.value.__notes__[-1]
        assert "STAC asset 'data'" in note
        assert str(source.data.url) in note

    def test_stac_read_zarr(self, tmp_path):
        """Read a Zarr asset through a relative STAC link."""
        xr.Dataset({"temperature": (["time", "x"], np.ones((5, 10)))}).to_zarr(tmp_path / "sample.zarr")
        path = _write_json(
            tmp_path / "item.json",
            {
                "type": "Feature",
                "id": "zarr-item",
                "properties": {"title": "Zarr item", "aqua:model": "SyntheticModel"},
                "assets": {"zarr_store": {"href": "sample.zarr", "type": "application/vnd+zarr", "roles": ["data"]}},
            },
        )
        source = IntakeSTACSource(urlpath=path, asset="zarr_store")
        assert source.metadata["title"] == "Zarr item"
        assert source.metadata["aqua:model"] == "SyntheticModel"
        with source.read() as data:
            assert data["temperature"].shape == (5, 10)

    @pytest.mark.parametrize(
        ("assets", "asset", "error", "message"),
        [
            ({}, None, ValueError, "contains no assets"),
            ({"preview": {"href": "preview.png"}}, "missing", KeyError, "Asset 'missing' not found"),
            ({"data": {}}, None, ValueError, "has no 'href'"),
        ],
    )
    def test_invalid_assets(self, tmp_path, assets, asset, error, message):
        """Reject empty assets, unknown selectors, and missing hrefs."""
        path = _write_json(tmp_path / "item.json", {"type": "Feature", "id": "invalid", "assets": assets})
        with pytest.raises(error, match=message):
            IntakeSTACSource(urlpath=path, asset=asset)

    @pytest.mark.parametrize(
        ("assets", "expected"),
        [
            ({"preview": {"href": "preview.png"}}, "preview"),
            ({"preview": {"href": "preview.png"}, "primary": {"href": "data.nc", "roles": ["data"]}}, "primary"),
        ],
    )
    def test_default_asset(self, tmp_path, assets, expected):
        """Prefer a unique data role, otherwise use the first asset."""
        path = _write_json(tmp_path / "item.json", {"type": "Feature", "id": "defaults", "assets": assets})
        assert IntakeSTACSource(urlpath=path).asset_name == expected


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
        assert collection() is collection
        assert set(collection.keys()) == {"first", "second", "data"}
        assert set(collection._entries) == set(collection.keys())
        assert set(collection.entries) == set(collection.keys())
        assert set(collection.describe()["available_items"]) == set(collection.keys())
        entry = collection._entries["first"]
        assert (entry.name, entry._driver, entry._user_parameters) == ("first", "stac", [])
        assert entry.describe()["driver"] == "stac"
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
        assert set(item.describe()["available_assets"]) == set(item.keys())
        assert all(key in item for key in item.keys())
        assert "second" not in item
        with pytest.raises(KeyError, match="Asset 'second' not found"):
            item["second"]
        assert item["data"].item_name == "first"
        assert item["data"].asset_arg == "data"
        asset_entry = item._entries["data"]
        assert asset_entry.name == "data"
        assert asset_entry.describe()["description"] == "STAC asset data"
        asset_source = asset_entry(chunks={"time": 1})
        assert asset_source.item_name == "first"
        assert asset_source.asset_name == "data"
        assert asset_source.extra_kwargs["chunks"] == {"time": 1}
        assert asset_source._entry is not None

    @pytest.mark.parametrize("method", ["read", "to_dask", "discover", "read_chunked"])
    def test_stac_collection_missing_item_error(self, sample_stac_collection_path, method):
        """Test that reading directly from a collection without selecting an item raises ValueError."""
        source = IntakeSTACSource(urlpath=sample_stac_collection_path, metadata={"owner": "catalog"})
        assert source.metadata["owner"] == "catalog"
        assert source.item_name is None
        assert "ifs-short-ifs2d-tco79" in source.keys()
        with pytest.raises(ValueError, match="cannot be read directly"):
            getattr(source, method)()

    def test_stac_collection_unknown_item_error(self, sample_stac_collection_path):
        """Test that requesting an unknown item from a collection raises KeyError with available items."""
        with pytest.raises(KeyError, match="Item 'non-existent-item' not found in STAC collection"):
            IntakeSTACSource(
                urlpath=sample_stac_collection_path,
                item="non-existent-item",
            )
        with pytest.raises(KeyError, match="Item 'non-existent-item' not found in STAC collection"):
            IntakeSTACSource(urlpath=sample_stac_collection_path)["non-existent-item"]

    @pytest.mark.parametrize(
        ("item", "variable", "selection"),
        [("ifs-short-ifs2d-tco79", "2t", "call"), ("ifs-teleconnections-enso-test", "skt", "index")],
    )
    def test_yaml_item_selection(self, tmp_path, sample_stac_collection_path, item, variable, selection):
        """YAML parameter defaults can be overridden by calls or hierarchical selection."""
        parameters = {"item": {"description": "Item to load", "type": "str", "default": "ifs-long-regridded-r18x9"}}
        source = _catalog(
            tmp_path / "catalog.yaml", sample_stac_collection_path, parameters=parameters, item="{{ item }}", asset="data"
        )["stac"]
        assert source.item_name == "ifs-long-regridded-r18x9"
        selected = source(item=item) if selection == "call" else source[item]
        assert selected.item_name == item
        with selected["data"].read() as data:
            assert variable in data.data_vars


class TestSTACCollectionDiscovery:
    """Collection links, assets, and Intake entries."""

    def test_child_links_and_roles(self, tmp_path):
        """Discover child links by title and distinguish unique from ambiguous asset roles."""
        assets = {
            "dkrz-disk": {"href": "disk.nc", "roles": ["data"]},
            "eerie-cloud": {"href": "cloud.nc", "roles": ["data"]},
            "preview": {"href": "preview.png", "roles": ["thumbnail"]},
        }
        item_path = _write_json(tmp_path / "item.json", {"type": "Feature", "id": "my-item", "assets": assets})
        collection_path = _write_json(
            tmp_path / "collection.json",
            {
                "type": "Collection",
                "id": "children",
                "links": [{"rel": "child", "href": item_path, "title": "my-dataset.2D_hourly"}],
            },
        )
        source = IntakeSTACSource(urlpath=collection_path)
        assert "my-dataset.2D_hourly" in source.keys()
        item = source(item="my-dataset.2D_hourly", asset="preview")
        assert item.asset_name == "preview"
        assert item["preview"] is item
        assert item["dkrz-disk"].asset_name == "dkrz-disk"
        assert source(item="my-dataset.2D_hourly", asset="thumbnail").asset_name == "preview"
        with pytest.raises(KeyError, match="matches multiple assets by role"):
            source(item="my-dataset.2D_hourly", asset="data")

    @pytest.mark.parametrize("link_style", ["items", "unlinked"])
    def test_stac_collection_item_fallbacks(self, tmp_path, link_style):
        """Resolve items from an API items link or a sibling JSON file."""
        item_id = "fallback-item"
        item_dir = tmp_path / "items" if link_style == "items" else tmp_path
        item_dir.mkdir(exist_ok=True)
        item_path = item_dir / (item_id if link_style == "items" else f"{item_id}.json")
        _write_json(
            item_path,
            {"type": "Feature", "id": item_id, "assets": {"data": {"href": "data.nc", "type": "application/x-netcdf"}}},
        )
        links = [{"rel": "items", "href": "./items"}] if link_style == "items" else []
        collection_path = _write_json(tmp_path / "collection.json", {"type": "Collection", "id": "fallback", "links": links})
        source = IntakeSTACSource(urlpath=collection_path, item=item_id)
        assert source.item_base_urlpath == str(item_path)
        assert source.metadata["id"] == item_id
        assert source.asset_name == "data"

    @pytest.mark.parametrize("selector", ["item", "asset"])
    @pytest.mark.parametrize("matching_default", [False, True])
    @pytest.mark.parametrize("access", ["entry", "backend"])
    def test_entry_selector_overrides_parent_default(
        self, selectable_collection, tmp_path, selector, matching_default, access
    ):
        """Named item and asset entries retain their selection when defaults are forwarded."""
        selected = "second" if selector == "item" else "data"
        defaults = {"item": "first", "asset": "shared"}
        if matching_default:
            defaults[selector] = selected
        catalog_path = tmp_path / "parameterized.yaml"
        catalog = _catalog(
            catalog_path,
            selectable_collection,
            parameters={key: {"description": key, "type": "str", "default": value} for key, value in defaults.items()},
            item="{{ item }}",
            asset="{{ asset }}",
        )

        if access == "entry":
            parent = catalog["stac"] if selector == "item" else catalog["stac"]["first"]
            source = parent._entries[selected](**defaults, chunks={"time": 1})
        else:
            if selector == "item":
                root_path = tmp_path / "root.yaml"
                catalog = intake.open_catalog(
                    _write_json(
                        root_path, {"sources": {"model": {"driver": "yaml_file_cat", "args": {"path": str(catalog_path)}}}}
                    )
                )
            configurer = Mock()
            configurer.deliver_intake_catalog.return_value = (catalog, str(catalog_path), None)
            configurer.get_machine_info.return_value = ({}, {})
            backend = CatalogMixin()
            backend.logger = Mock()
            model, exp = ("model", "stac") if selector == "item" else ("stac", "first")
            backend.setup_catalog(model, exp, selected, configurer, chunks={"time": 1})
            assert backend.kwargs[selector] == defaults[selector]
            source = backend.esmcat

        assert source.item_name == (selected if selector == "item" else "first")
        assert source.asset_name == ("shared" if selector == "item" else selected)
        assert source.extra_kwargs["chunks"] == {"time": 1}


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

    @pytest.mark.parametrize("kind", ["factory", "backend"])
    def test_selector_aliases(self, kind):
        """Factory and backend aliases read and update the canonical selectors."""
        if kind == "factory":
            source = BackendFactory(configurer=ConfigContext(), collection="IFS", item="test-tco79", asset="short")
        else:
            source = CatalogMixin()
            source.model, source.exp, source.source = "IFS", "test-tco79", "short"
        assert (source.model, source.exp, source.source) == ("IFS", "test-tco79", "short")
        assert (source.collection, source.item, source.asset) == ("IFS", "test-tco79", "short")
        source.collection, source.item, source.asset = "NEMO", "eorca1", "sst"
        assert (source.model, source.exp, source.source) == ("NEMO", "eorca1", "sst")
        assert (source.collection, source.item, source.asset) == ("NEMO", "eorca1", "sst")

    @pytest.mark.parametrize(
        ("canonical", "alias"),
        [("model", "collection"), ("exp", "item"), ("source", "asset")],
    )
    def test_backend_factory_rejects_conflicting_stac_aliases(self, canonical, alias):
        """Matching canonical/alias arguments are valid; conflicting ones fail clearly."""
        selectors = {"model": "IFS", "exp": "test-tco79", "source": "short"}
        matching = BackendFactory(configurer=ConfigContext(), **selectors, **{alias: selectors[canonical]})
        assert getattr(matching, alias) == selectors[canonical]
        with pytest.raises(ValueError, match=f"Conflicting values provided for {canonical}"):
            BackendFactory(configurer=ConfigContext(), **selectors, **{alias: "different"})

    @pytest.mark.parametrize("item", ["ifs-short-ifs2d-tco79", "IFS2d.tco79.nc", "ifs-short-ifs2d-tco79.json"])
    @pytest.mark.parametrize("fixer_name", ["ifs-short-fixer", False])
    def test_backend_factory_stac_metadata_by_id(self, stac_factory, sample_stac_collection_path, item, fixer_name):
        """ID/title/filename aliases use the canonical metadata key, including a disabled fixer."""
        stac_factory.esmcat = IntakeSTACSource(urlpath=sample_stac_collection_path, item=item, asset="data")
        stac_factory.metadata["fixer_name"]["ifs-short-ifs2d-tco79"] = fixer_name
        assert stac_factory.get_metadata()[:2] == (fixer_name, "tco79-grid")
        assert stac_factory.esmcat.item_name == item

    @pytest.mark.parametrize(
        ("item", "expected"),
        [
            ("ifs-long-regridded-r18x9", ("ifs-long-fixer", "r18x9-grid")),
            ("ifs-teleconnections-enso-test", (None, None)),
            (None, (None, None)),
        ],
    )
    def test_metadata_item_mapping(self, stac_factory, sample_stac_collection_path, item, expected):
        """Resolve a second item, a missing mapping, and an unselected collection."""
        stac_factory.esmcat = IntakeSTACSource(urlpath=sample_stac_collection_path, item=item)
        assert stac_factory.get_metadata()[:2] == expected

    @pytest.mark.parametrize("backend_type", ["intake_stac", "intake_netcdf"])
    def test_metadata_argument_mapping(self, stac_factory, backend_type):
        """Direct dictionaries resolve only for STAC; other backends keep the supplied mappings."""
        stac_factory.backend_type = backend_type
        fixers = {"ifs-short-ifs2d-tco79": "arg-fixer"}
        grids = {"ifs-short-ifs2d-tco79": "arg-grid"}
        expected = ("arg-fixer", "arg-grid") if backend_type == "intake_stac" else (fixers, grids)
        assert stac_factory.get_metadata(fixer_name=fixers, src_grid_name=grids)[:2] == expected
        if backend_type == "intake_netcdf":
            assert stac_factory.get_metadata()[:2] == (
                stac_factory.metadata["fixer_name"],
                stac_factory.metadata["source_grid_name"],
            )
