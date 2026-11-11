"""Reader integration tests for the pystac backend, using the generated local STAC catalogue."""

import json
import os

import pytest

from aqua import Reader

pytestmark = pytest.mark.stac

LOGLEVEL = "DEBUG"
IFS_ITEM = "ifs-short-ifs2d-tco79"
ERA5_ITEM = "era5-era5-2d-flip-2mon2021"


@pytest.fixture(scope="module")
def catalog_url(stac_catalogue_dir):
    """Root catalog of the generated STAC catalogue."""
    return os.path.join(stac_catalogue_dir, "catalog.json")


@pytest.fixture(scope="module")
def era5_item_url(stac_catalogue_dir):
    """Single STAC item (regular lat-lon grid)."""
    return os.path.join(stac_catalogue_dir, "items", "era5", f"{ERA5_ITEM}.json")


@pytest.fixture(scope="module")
def collection_url(stac_catalogue_dir):
    """Single STAC collection."""
    return os.path.join(stac_catalogue_dir, "collections", "ifs.json")


class TestSTACReader:
    """Data retrieval through Reader(url=..., stac_kwargs=...)."""

    def test_nested_catalog(self, catalog_url):
        """Walk catalog -> collection -> item -> asset with a nested mapping."""
        reader = Reader(url=catalog_url, stac_kwargs={"ifs": {IFS_ITEM: "data"}}, areas=False, loglevel=LOGLEVEL)
        data = reader.retrieve()

        assert "2t" in data
        assert data["2t"].chunks is not None  # lazy
        assert data["2t"].attrs["AQUA_url"] == catalog_url
        assert "STAC Backend" in data.attrs["history"]

    @pytest.mark.parametrize(
        "url_fixture, selection",
        [
            ("catalog_url", {"collection": "ifs", "item": IFS_ITEM}),
            ("catalog_url", {"collection": "ifs", "item": IFS_ITEM, "asset": "data"}),
            ("collection_url", {"item": IFS_ITEM}),
            ("collection_url", {"collection": "ifs", "item": IFS_ITEM}),
        ],
    )
    def test_select_without_stac_kwargs(self, request, url_fixture, selection):
        """collection/item/asset select the asset when stac_kwargs is missing; fails if the walk ignores them."""
        url = request.getfixturevalue(url_fixture)
        attrs = Reader(url=url, **selection, areas=False, loglevel=LOGLEVEL).retrieve()["2t"].attrs
        assert (attrs["AQUA_model"], attrs["AQUA_exp"], attrs["AQUA_source"]) == ("ifs", IFS_ITEM, "data")

    def test_labels_from_stac(self, catalog_url, era5_item_url):
        """The catalog/collection/item/asset ids become AQUA_catalog/model/exp/source; the catalog only from a catalog url."""
        data = Reader(url=catalog_url, stac_kwargs={"ifs": {IFS_ITEM: "data"}}, areas=False).retrieve()
        attrs = data["2t"].attrs
        assert attrs["AQUA_catalog"] == "aqua-local-netcdf"
        assert (attrs["AQUA_model"], attrs["AQUA_exp"], attrs["AQUA_source"]) == ("ifs", IFS_ITEM, "data")
        assert f"Retrieved ifs {IFS_ITEM} data from {catalog_url}" in data.attrs["history"]

        attrs = Reader(url=era5_item_url, areas=False).retrieve()["2t"].attrs  # default asset 'data'
        assert "AQUA_catalog" not in attrs
        assert (attrs["AQUA_model"], attrs["AQUA_exp"], attrs["AQUA_source"]) == ("era5", ERA5_ITEM, "data")

    def test_user_labels_override(self, era5_item_url):
        """With stac_kwargs, model/exp/source are only labels and take priority over the STAC ids."""
        reader = Reader(url=era5_item_url, stac_kwargs="data", model="ERA5", exp="reanalysis", catalog="obs", areas=False)
        attrs = reader.retrieve()["2t"].attrs
        assert (attrs["AQUA_catalog"], attrs["AQUA_model"], attrs["AQUA_exp"]) == ("obs", "ERA5", "reanalysis")
        assert attrs["AQUA_source"] == "data"

    def test_item_with_asset_key(self, era5_item_url):
        """An item URL only needs the asset key as a string."""
        reader = Reader(url=era5_item_url, stac_kwargs="data", areas=False, loglevel=LOGLEVEL)
        data = reader.retrieve(var="T2M")

        assert list(data.data_vars) == ["T2M"]
        assert data["T2M"].chunks is not None

    def test_collection_url(self, collection_url):
        """A collection URL is walked down to one of its items."""
        reader = Reader(url=collection_url, stac_kwargs={IFS_ITEM: "data"}, areas=False, loglevel=LOGLEVEL)
        assert "2t" in reader.retrieve()

    def test_chunks(self, era5_item_url):
        """Chunks given to the Reader are applied to the asset."""
        reader = Reader(url=era5_item_url, stac_kwargs="data", areas=False, chunks={"time": 1}, loglevel=LOGLEVEL)
        data = reader.retrieve()
        assert data["2t"].chunksizes["time"] == (1, 1)

    def test_foreign_open_kwargs(self, era5_item_url, tmp_path):
        """Asset open_kwargs not accepted by xarray (DKRZ EERIE style) are dropped; fails if passed to open_dataset."""
        with open(era5_item_url) as f:
            item = json.load(f)
        asset = item["assets"]["data"]
        asset["href"] = os.path.abspath(os.path.join(os.path.dirname(era5_item_url), asset["href"]))
        asset["xarray:open_kwargs"] = {
            "url": "reference:://elsewhere.parquet",
            "root": "",
            "metadata": {"catalog_dir": "/elsewhere"},
            "storage_options": {"lazy": True, "remote_protocol": "file"},
            "engine": "netcdf4",
        }
        item["links"] = []
        item_path = tmp_path / "item.json"
        item_path.write_text(json.dumps(item))

        data = Reader(url=str(item_path), areas=False, loglevel=LOGLEVEL).retrieve()
        assert data["2t"].attrs["AQUA_exp"] == ERA5_ITEM

    @pytest.mark.parametrize(
        "startdate, enddate, expected",
        [
            ("2021-02-01", None, ["2021-02-01"]),
            (None, "2021-01-31", ["2021-01-01"]),
            ("2021-01-01", "2021-02-28", ["2021-01-01", "2021-02-01"]),
        ],
    )
    def test_date_selection(self, era5_item_url, startdate, enddate, expected):
        """startdate/enddate select the time steps of the asset."""
        reader = Reader(url=era5_item_url, stac_kwargs="data", areas=False, loglevel=LOGLEVEL)
        data = reader.retrieve(startdate=startdate, enddate=enddate)
        assert [str(t)[:10] for t in data.time.values.astype("datetime64[D]")] == expected

    def test_retrieve_plain_for_areas(self, era5_item_url, tmp_path, monkeypatch):
        """Default Reader (areas=True) builds the area from a minimal sample of the asset."""
        monkeypatch.chdir(tmp_path)  # areas/weights/grids go to ./areas, ./weights, ./grids
        reader = Reader(url=era5_item_url, stac_kwargs="data", loglevel=LOGLEVEL)
        assert reader.retrieve()["2t"].dims[-2:] == ("lat", "lon")
        assert [p.name for p in (tmp_path / "areas").iterdir()] != ["cell_area_unknown.nc"]

    def test_area_identifier(self, catalog_url, era5_item_url):
        """Area/weight filenames depend on url and selected asset, so different assets do not collide."""
        ids = [
            Reader(url=url, stac_kwargs=sel, areas=False).regridder_metadata.get_path_identifier()
            for url, sel in [
                (catalog_url, {"ifs": {IFS_ITEM: "data"}}),
                (catalog_url, {"era5": {ERA5_ITEM: "data"}}),
                (era5_item_url, "data"),
            ]
        ]
        assert len(set(ids)) == 3
        # labels with stac_kwargs do not change the data, hence neither the identifier
        labelled = Reader(url=era5_item_url, stac_kwargs="data", model="ERA5", areas=False)
        assert labelled.regridder_metadata.get_path_identifier() == ids[2]


class TestSTACErrors:
    """Wrong selections and inconsistent arguments."""

    def test_missing_child(self, catalog_url):
        reader = Reader(url=catalog_url, stac_kwargs={"nonexistent": "data"}, areas=False)
        with pytest.raises(KeyError, match="not found"):
            reader.retrieve()

    def test_missing_asset(self, era5_item_url):
        reader = Reader(url=era5_item_url, stac_kwargs="nonexistent", areas=False)
        with pytest.raises(KeyError, match="Available"):
            reader.retrieve()

    def test_descend_into_item(self, era5_item_url):
        reader = Reader(url=era5_item_url, stac_kwargs={"child": "data"}, areas=False)
        with pytest.raises(KeyError, match="is an Item"):
            reader.retrieve()

    @pytest.mark.parametrize("stac_kwargs", ["data", None])
    def test_catalog_without_item(self, catalog_url, stac_kwargs):
        """A catalog has no assets: an item must be selected."""
        reader = Reader(url=catalog_url, stac_kwargs=stac_kwargs, areas=False)
        with pytest.raises(ValueError, match="has no assets"):
            reader.retrieve()

    def test_not_single_branch(self, catalog_url):
        reader = Reader(url=catalog_url, stac_kwargs={"ifs": "data", "era5": "data"}, areas=False)
        with pytest.raises(ValueError, match="single-branch"):
            reader.retrieve()

    def test_url_and_path(self, catalog_url):
        with pytest.raises(ValueError, match="Both url and path"):
            Reader(url=catalog_url, path="file.nc", areas=False)
