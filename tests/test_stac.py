"""Reader integration tests for the pystac backend, using the generated local STAC catalogue."""

import pytest

from aqua import Reader

pytestmark = pytest.mark.stac

LOGLEVEL = "DEBUG"
IFS_ITEM = "ifs-short-ifs2d-tco79"
ERA5_ITEM = "era5-era5-2d-flip-2mon2021"


@pytest.fixture(scope="module")
def catalog_url(stac_catalogue_dir):
    """Root catalog of the generated STAC catalogue."""
    return str(stac_catalogue_dir / "catalog.json")


@pytest.fixture(scope="module")
def era5_item_url(stac_catalogue_dir):
    """Single STAC item (regular lat-lon grid)."""
    return str(stac_catalogue_dir / "items" / "era5" / f"{ERA5_ITEM}.json")


@pytest.fixture(scope="module")
def collection_url(stac_catalogue_dir):
    """Single STAC collection."""
    return str(stac_catalogue_dir / "collections" / "ifs.json")


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

    def test_invalid_leaf_on_catalog(self, catalog_url):
        reader = Reader(url=catalog_url, stac_kwargs="data", areas=False)
        with pytest.raises(ValueError, match="not a valid asset key"):
            reader.retrieve()

    def test_url_without_stac_kwargs(self, catalog_url):
        with pytest.raises(ValueError, match="Incomplete 'stac'"):
            Reader(url=catalog_url, areas=False)

    def test_conflicting_setups(self, catalog_url):
        with pytest.raises(ValueError, match="multiple setups"):
            Reader(url=catalog_url, stac_kwargs="data", path="file.nc", areas=False)
