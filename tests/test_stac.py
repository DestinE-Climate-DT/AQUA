"""Reader integration tests for the pystac backend, using the generated local STAC catalogue."""

import json
import os

import pystac
import pytest

from aqua import Reader

pytestmark = pytest.mark.stac

LOGLEVEL = "DEBUG"
IFS_ITEM = "ifs-short-ifs2d-tco79"
ERA5_ITEM = "era5-era5-2d-flip-2mon2021"
IFS = ("ifs", IFS_ITEM)
ERA5 = ("era5", ERA5_ITEM)


@pytest.fixture(scope="module")
def urls(stac_catalogue_dir):
    """Catalog, collection and item (regular lat-lon grid) of the generated STAC catalogue."""
    return {
        "catalog": os.path.join(stac_catalogue_dir, "catalog.json"),
        "collection": os.path.join(stac_catalogue_dir, "collections", "ifs.json"),
        "item": os.path.join(stac_catalogue_dir, "items", "era5", f"{ERA5_ITEM}.json"),
    }


def write_item(item_url, tmp_path, **asset_fields):
    """Copy a STAC item with an absolute data href and extra fields in its 'data' asset; return its path."""
    with open(item_url) as f:
        item = json.load(f)
    asset = item["assets"]["data"]
    asset["href"] = os.path.abspath(os.path.join(os.path.dirname(item_url), asset["href"]))
    asset.update(asset_fields)
    item["links"] = []
    item_path = tmp_path / "item.json"
    item_path.write_text(json.dumps(item))
    return str(item_path)


class TestSTACReader:
    """Data retrieval through Reader(url=..., stac_kwargs=...)."""

    @pytest.mark.parametrize(
        "url_key, selection, catalog, expected",
        [
            ("catalog", {"stac_kwargs": {"ifs": {IFS_ITEM: "data"}}}, "aqua-local-netcdf", IFS),
            ("catalog", {"collection": "ifs", "item": IFS_ITEM}, "aqua-local-netcdf", IFS),
            ("catalog", {"collection": "ifs", "item": IFS_ITEM, "asset": "data"}, "aqua-local-netcdf", IFS),
            ("collection", {"stac_kwargs": {IFS_ITEM: "data"}}, None, IFS),
            ("collection", {"item": IFS_ITEM}, None, IFS),
            ("collection", {"collection": "ifs", "item": IFS_ITEM}, None, IFS),
            ("item", {"stac_kwargs": "data"}, None, ERA5),
            ("item", {}, None, ERA5),  # default asset 'data'
        ],
    )
    def test_select_asset(self, urls, url_key, selection, catalog, expected):
        """stac_kwargs or collection/item/asset walk from url to the asset; the STAC ids become AQUA_* labels.
        Fails if the walk ignores the selection or labels the wrong object (AQUA_catalog only from a catalog url)."""
        url = urls[url_key]
        data = Reader(url=url, **selection, areas=False, loglevel=LOGLEVEL).retrieve()
        attrs = data["2t"].attrs
        assert (attrs["AQUA_model"], attrs["AQUA_exp"], attrs["AQUA_source"]) == (*expected, "data")
        assert attrs.get("AQUA_catalog") == catalog
        assert attrs["AQUA_url"] == url
        assert data["2t"].chunks is not None  # lazy
        assert f"Retrieved {expected[0]} {expected[1]} data from {url}" in data.attrs["history"]

    def test_user_labels_override(self, urls):
        """With stac_kwargs, model/exp/catalog are only labels and take priority over the STAC ids."""
        reader = Reader(url=urls["item"], stac_kwargs="data", model="ERA5", exp="reanalysis", catalog="obs", areas=False)
        attrs = reader.retrieve()["2t"].attrs
        assert (attrs["AQUA_catalog"], attrs["AQUA_model"], attrs["AQUA_exp"]) == ("obs", "ERA5", "reanalysis")
        assert attrs["AQUA_source"] == "data"

    @pytest.mark.parametrize(
        "startdate, enddate, expected",
        [
            ("2021-02-01", None, ["2021-02-01"]),
            (None, "2021-01-31", ["2021-01-01"]),
            ("2021-01-01", "2021-02-28", ["2021-01-01", "2021-02-01"]),
        ],
    )
    def test_retrieve_selection(self, urls, startdate, enddate, expected):
        """var, startdate and enddate select the variables and time steps of the asset."""
        reader = Reader(url=urls["item"], stac_kwargs="data", areas=False, loglevel=LOGLEVEL)
        data = reader.retrieve(var="T2M", startdate=startdate, enddate=enddate)
        assert list(data.data_vars) == ["T2M"]
        assert [str(t)[:10] for t in data.time.values.astype("datetime64[D]")] == expected

    @pytest.mark.parametrize("chunks, expected", [(None, (1, 1)), ({"time": 2}, (2,))])
    def test_asset_open_kwargs(self, urls, tmp_path, chunks, expected):
        """Asset options not accepted by the engine (DKRZ EERIE style, storage_options for netcdf4) are dropped,
        the asset chunks are used and Reader chunks override them. Fails if any is passed to open_dataset."""
        open_kwargs = {
            "url": "reference:://elsewhere.parquet",
            "root": "",
            "metadata": {"catalog_dir": "/elsewhere"},
            "storage_options": {"lazy": True, "remote_protocol": "file"},
            "engine": "netcdf4",
            "chunks": {"time": 1},
        }
        fields = {"xarray:open_kwargs": open_kwargs, "xarray:storage_options": {"anon": True}}
        item_path = write_item(urls["item"], tmp_path, **fields)
        data = Reader(url=item_path, chunks=chunks, areas=False, loglevel=LOGLEVEL).retrieve()
        assert data["2t"].chunksizes["time"] == expected

    def test_areas(self, urls, tmp_path, monkeypatch):
        """Default Reader (areas=True) builds the area from a minimal sample, reading the STAC tree once."""
        monkeypatch.chdir(tmp_path)  # areas/weights/grids go to ./areas, ./weights, ./grids
        calls = []
        read_file = pystac.read_file
        monkeypatch.setattr(pystac, "read_file", lambda *a, **k: calls.append(a) or read_file(*a, **k))
        reader = Reader(url=urls["item"], stac_kwargs="data", loglevel=LOGLEVEL)
        assert reader.retrieve()["2t"].dims[-2:] == ("lat", "lon")
        reader.retrieve(var="2t")
        assert len(calls) == 1
        assert [p.name for p in (tmp_path / "areas").iterdir()] != ["cell_area_unknown.nc"]

    def test_area_identifier(self, urls):
        """Area/weight filenames depend on url and selected asset, not on labels, so different assets do not collide."""
        ids = [
            Reader(url=urls[key], stac_kwargs=sel, areas=False).regridder_metadata.get_path_identifier()
            for key, sel in [
                ("catalog", {"ifs": {IFS_ITEM: "data"}}),
                ("catalog", {"era5": {ERA5_ITEM: "data"}}),
                ("item", "data"),
            ]
        ]
        assert len(set(ids)) == 3
        labelled = Reader(url=urls["item"], stac_kwargs="data", model="ERA5", areas=False)
        assert labelled.regridder_metadata.get_path_identifier() == ids[2]

    @pytest.mark.parametrize(
        "url_key, kwargs, error, match",
        [
            ("catalog", {"stac_kwargs": {"nonexistent": "data"}}, KeyError, "not found"),
            ("item", {"stac_kwargs": "nonexistent"}, KeyError, "Available"),
            ("item", {"stac_kwargs": {"child": "data"}}, KeyError, "is an Item"),
            ("item", {"model": "ERA5"}, KeyError, "Pass stac_kwargs"),  # label meant, selector used
            ("catalog", {"stac_kwargs": "data"}, ValueError, "has no assets"),
            ("catalog", {}, ValueError, "has no assets"),
            ("catalog", {"stac_kwargs": {"ifs": "data", "era5": "data"}}, ValueError, "single-branch"),
            ("catalog", {"path": "file.nc"}, ValueError, "Both url and path"),
        ],
    )
    def test_errors(self, urls, url_key, kwargs, error, match):
        """Wrong selections and inconsistent arguments raise an explicit error."""
        with pytest.raises(error, match=match):
            Reader(url=urls[url_key], **kwargs, areas=False).retrieve()
