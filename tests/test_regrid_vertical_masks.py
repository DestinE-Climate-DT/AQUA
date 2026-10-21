"""Regression tests for grid files whose ocean mask changes with depth."""

import numpy as np
import pytest
import xarray as xr

from aqua import Regridder
from aqua.regridder.griddicthandler import GridDictHandler


@pytest.mark.aqua
@pytest.mark.parametrize("vertical_dims", ["level", ["level"], ["depth_full", "depth_half"],
                                          ["2d"], ["2d", "2dm"], None])
def test_file_grid_preserves_vertical_dimensions(tmp_path, vertical_dims):
    """A file with declared vertical coordinates must retain those coordinates."""
    gridfile = tmp_path / "grid.nc"
    gridfile.write_bytes(b"grid")
    grid = {"path": str(gridfile), "vert_coord": vertical_dims}
    handler = GridDictHandler({"grids": {"ocean": grid}})

    normalized = handler.normalize_grid_dict("ocean")
    expected = vertical_dims or ["2d"]
    if isinstance(expected, str):
        expected = [expected]
    assert normalized["path"] == {dim: str(gridfile) for dim in expected}


@pytest.mark.aqua
def test_explicit_grid_paths_are_preserved(tmp_path):
    """Explicit 2D, masked, and staggered 3D grids keep their separate files."""
    paths = {}
    for dim in ("2d", "2dm", "depth_full", "depth_half"):
        gridfile = tmp_path / f"{dim}.nc"
        gridfile.write_bytes(b"grid")
        paths[dim] = str(gridfile)
    grid = {"path": paths, "vert_coord": ["depth_full", "depth_half"]}
    normalized = GridDictHandler({"grids": {"ocean": grid}}).normalize_grid_dict("ocean")
    assert normalized["path"] == paths


@pytest.mark.aqua
def test_cdo_grid_with_vertical_metadata():
    """A CDO grid name provides horizontal geometry without per-level masks."""
    grid = {"path": "r24x12", "vert_coord": ["level"]}
    normalized = GridDictHandler({"grids": {"ocean": grid}}).normalize_grid_dict("ocean")
    assert normalized["path"] == {"2d": "r24x12"}


@pytest.mark.aqua
def test_regrid_file_with_depth_dependent_mask(tmp_path):
    """Deeper missing cells must not contaminate valid cells with fill values."""
    values = np.full((3, 4, 8), 280.)
    values[:, :, 0] = np.nan
    values[1, 1:3, 2] = np.nan
    values[2, 1:3, 2:4] = np.nan
    data = xr.DataArray(
        values, dims=("level", "lat", "lon"), name="thetao",
        coords={"level": [1., 2., 3.], "idx_level": ("level", [0, 1, 2]),
                "lat": [-67.5, -22.5, 22.5, 67.5],
                "lon": np.arange(0., 360., 45.) + .3})
    data.lat.attrs = {"units": "degrees_north", "standard_name": "latitude"}
    data.lon.attrs = {"units": "degrees_east", "standard_name": "longitude"}
    data.level.attrs = {"axis": "Z", "positive": "down"}
    gridfile = tmp_path / "ocean3d.nc"
    data.drop_vars("idx_level").to_netcdf(gridfile)

    cfg = {
        "grids": {"ocean": {"path": str(gridfile), "vert_coord": ["level"],
                             "space_coord": ["lat", "lon"]}},
        "paths": {"weights": str(tmp_path)},
        "weights": {"template_grid": "{sourcegrid}_{method}_{targetgrid}_l{level}.nc"}}
    # An old surface-only cache must not be reused for a depth-dependent grid.
    old_weights = tmp_path / "ocean_ycon_r24x12_l2d.nc"
    old_weights.write_bytes(b"obsolete surface weights")
    regridder = Regridder(cfg, src_grid_name="ocean")
    regridder.weights("r24x12")

    fixed = data.rename({"level": "depth", "idx_level": "idx_depth"})
    for source in (data, data.chunk({"level": 1}), fixed, fixed.chunk({"depth": 1})):
        vertical_dim = "level" if "level" in source.dims else "depth"
        result = regridder.regrid(source).compute()
        assert result.sizes == {vertical_dim: 3, "lat": 12, "lon": 24}
        assert bool(result.notnull().any())
        np.testing.assert_allclose(result.values[np.isfinite(result.values)], 280.)
        missing = result.isnull().sum(("lat", "lon")).values
        assert missing[0] < missing[1] < missing[2]

        dataset_result = regridder.regrid(source.to_dataset()).compute()
        xr.testing.assert_equal(dataset_result.thetao, result)
        selected = regridder.regrid(source.isel({vertical_dim: [2, 0]})).compute()
        xr.testing.assert_equal(selected, result.isel({vertical_dim: [2, 0]}))
        scalar = regridder.regrid(source.isel({vertical_dim: 2})).compute()
        xr.testing.assert_equal(scalar.squeeze(vertical_dim), result.isel({vertical_dim: 2}))

    # Loading the saved 3D weights must give the same result as generating them.
    cached = Regridder(cfg, src_grid_name="ocean")
    cached.weights("r24x12")
    xr.testing.assert_equal(cached.regrid(fixed).compute(), result)
    assert old_weights.read_bytes() == b"obsolete surface weights"
