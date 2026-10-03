"""Tests for the Intake STAC backend."""

from unittest.mock import MagicMock, patch

import dask.array as da
import pytest
import xarray as xr

from aqua.core.backend.backend_factory import BackendFactory
from aqua.core.backend.backend_stac import BackendSTAC


def _dataset():
    """Return a small lazy dataset suitable for backend tests."""
    values = da.from_array([[1.0, 2.0], [3.0, 4.0]], chunks=(1, 2))
    return xr.Dataset(
        {"tas": (("time", "x"), values)},
        coords={"time": ["2000-01-01", "2000-01-02"], "x": [0, 1]},
    )


@pytest.mark.stac
def test_stac_backend_retrieves_and_postprocesses_dataset():
    """The selected asset is read and passed through fixer and data model."""
    data = _dataset()
    asset_reader = MagicMock()
    asset_reader.read.return_value = data
    catalog = {"collection": {"item": {"asset": asset_reader}}}

    fixer = MagicMock()
    fixer.fixer.side_effect = lambda dataset, variables: dataset
    fixer.fixerdatamodel.apply.side_effect = lambda dataset: dataset
    datamodel = MagicMock()
    datamodel.apply.side_effect = lambda dataset: dataset

    with (
        patch("aqua.core.backend.backend_stac.intake.datatypes.STACJSON") as stac_json,
        patch("aqua.core.backend.backend_stac.intake.catalogs.StacCatalogReader") as catalog_reader,
    ):
        catalog_reader.return_value.read.return_value = catalog
        backend = BackendSTAC(
            path="https://example.org/catalog.json",
            stac={"collection": {"item": "asset"}},
            chunks={"time": 1},
            fixer=fixer,
            datamodel=datamodel,
            decode_times=False,
        )
        result = backend.retrieve(var="tas")

    stac_json.assert_called_once_with("https://example.org/catalog.json")
    asset_reader.read.assert_called_once_with(chunks={"time": 1}, decode_times=False)
    fixer.fixer.assert_called_once_with(data, ["tas"])
    fixer.fixerdatamodel.apply.assert_called_once_with(data)
    datamodel.apply.assert_called_once_with(data)
    assert result["tas"].attrs["AQUA_path"] == "https://example.org/catalog.json"
    assert result["tas"].attrs["AQUA_stac"] == "collection/item/asset"
    assert "Intake STAC" in result.attrs["history"]


@pytest.mark.parametrize(
    "selection",
    [
        None,
        {},
        {"one": "asset", "two": "asset"},
        {"collection": {}},
        {"collection": 1},
        {"": "asset"},
    ],
)
@pytest.mark.stac
def test_stac_backend_rejects_invalid_selection(selection):
    """Only a non-empty, single-branch nested mapping is accepted."""
    with pytest.raises(ValueError):
        BackendSTAC(path="catalog.json", stac=selection)


@pytest.mark.stac
def test_backend_factory_selects_stac_backend():
    """A STAC selection takes precedence over native xarray path access."""
    selection = {"collection": "asset"}
    factory = BackendFactory(configurer=MagicMock(), path="catalog.json", stac=selection)

    factory.select_backend()

    assert factory.driver == "stac"
    backend = factory.create_backend(fixer=None, datamodel=None)
    assert isinstance(backend, BackendSTAC)
    assert backend.catalog_path == ("collection", "asset")
    assert backend.read_kwargs["chunks"] == "auto"


@pytest.mark.stac
def test_backend_factory_requires_path_for_stac():
    """A selector without a catalog endpoint is rejected."""
    with pytest.raises(ValueError, match="STAC catalog path"):
        BackendFactory(configurer=MagicMock(), stac={"collection": "asset"})


@pytest.mark.stac
def test_stac_backend_drops_mapping_attrs_from_grid_sample():
    """Backend-only mapping attributes cannot be serialized by area generation."""
    data = _dataset()
    data["x"].attrs["filter_by_keys"] = {}
    backend = BackendSTAC(path="catalog.json", stac={"collection": "asset"})

    backend._drop_nonserializable_sample_attrs(data)

    assert "filter_by_keys" not in data["x"].attrs
