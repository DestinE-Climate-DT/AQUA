"""Test regridding from Reader"""

import numpy as np
import pytest
import xarray as xr
from utils_tests import APPROX_REL, LOGLEVEL

from aqua import Reader, Regridder
from aqua.core.regridder.filename_handler import FilenameHandler
from aqua.core.regridder.grid_dict_handler import GridDictHandler


@pytest.fixture(
    params=[
        ("IFS", "test-tco79", "short", "2t", 0),
        ("IFS", "test-tco79", "short_nn", "2t", 0),
        ("IFS", "test-tco79", "long-nogrid", "mtntrf", 0),
        ("ICON", "test-r2b0", "short", "2t", 0),
        ("ICON", "test-healpix", "short", "2t", 0),
        ("FESOM", "test-pi", "original_2d", "tos", 0.33925926),
        ("NEMO", "test-eORCA1", "long-2d", "tos", 0.3379),
    ]
)
def reader_arguments(request):
    return request.param


def cosine_weighted_mean(field):
    """Mean of a regular lon-lat field weighted with cos(lat), skipping the missing points (independent of fldstat)."""
    return float(field.weighted(np.cos(np.deg2rad(field.lat))).mean(("lon", "lat")))


def land_fraction(field):
    """Fraction of missing points of a field."""
    return float(field.isnull().sum() / field.size)


@pytest.fixture
def regrid_paths(tmp_path):
    """Regridder ``paths`` block sending weights and areas to ``tmp_path`` instead of the current directory."""
    return {"weights": str(tmp_path), "areas": str(tmp_path)}


@pytest.fixture(scope="module")
def fesom3d_r100_rebuilt():
    """FESOM 3D regridded to r100 with the weights rebuilt once, shared by the tests of the same weights file."""
    reader = Reader(
        model="FESOM",
        exp="test-pi",
        source="original_3d",
        regrid="r100",
        rebuild=True,
        datamodel=False,
        fix=False,
        loglevel=LOGLEVEL,
    )
    return reader.regrid(reader.retrieve(var="temp"))


@pytest.fixture(scope="module")
def nemo3d_r200_rebuilt():
    """NEMO 3D regridded to r200 with the weights rebuilt once, shared by the tests of the same weights file."""
    reader = Reader(
        model="NEMO",
        exp="test-eORCA1",
        source="short-3d",
        regrid="r200",
        rebuild=True,
        datamodel=False,
        fix=False,
        loglevel=LOGLEVEL,
    )
    return reader.regrid(reader.retrieve(var="avg_so"))


# fake dictionary for doing tests
cfg_dict = {
    "grids": {
        "r1000": "r36x18",
        "n128": "n128",
        "tragic": "tragic",
        "afternoon": {"path": {"2d": "r100x50"}},
        "doing": {"path": "noise.nc"},
        "wonderful": {"path": {"2d": "r360x180"}},
        "lovely": {"path": {"2d": "tests"}},
        "amazing": {
            "path": {"2d": "r100x50", "level": "banana"},
        },
        "romantic": {"path": "n128"},
        "tests": {"path": {"pizza please!"}},
    }
}


@pytest.mark.aqua
class TestRegridder:
    """class for regridding test"""

    def test_grid_dict_handler(self):
        """Test the grid dictionary handler"""

        # generic test
        with pytest.raises(ValueError, match="cfg_grid_dict must be a dictionary."):
            gdh = GridDictHandler("what a wonderful test", loglevel=LOGLEVEL)
        with pytest.raises(ValueError, match="No 'grids' key found in the cfg_grid_dict."):
            gdh = GridDictHandler({"pizza": "margherita"}, loglevel=LOGLEVEL)

        # empty one
        gdh = GridDictHandler(None, loglevel=LOGLEVEL)
        assert gdh.normalize_grid_dict("n256") == {"path": {"2d": "n256"}}
        with pytest.raises(ValueError, match="No cfg_grid_dict dictionary provided, only CDO grid names can be used."):
            gdh.normalize_grid_dict("mywonderfulgrid")

        # empty one
        gdh = GridDictHandler(cfg_grid_dict=cfg_dict, loglevel=LOGLEVEL)
        assert gdh.normalize_grid_dict(None) == {"path": {}}
        # standard
        assert gdh.normalize_grid_dict("r1000") == {"path": {"2d": "r36x18"}}
        # couple of CDO grids
        assert gdh.normalize_grid_dict("n256") == {"path": {"2d": "n256"}}
        assert gdh.normalize_grid_dict("r100x50") == {"path": {"2d": "r100x50"}}

        # test of errors
        with pytest.raises(ValueError, match="Grid name 'ciao' not found in the configuration."):
            gdh.normalize_grid_dict("ciao")
        with pytest.raises(TypeError, match="Grid name '20' is not a valid type."):
            gdh.normalize_grid_dict(20)
        with pytest.raises(ValueError, match="Grid name 'tragic' is a string but not a valid CDO grid name."):
            gdh.normalize_grid_dict("tragic")

        # test on grid path
        assert gdh.normalize_grid_dict("afternoon")["path"] == {"2d": "r100x50"}
        assert gdh.normalize_grid_dict("lovely")["path"] == {"2d": "tests"}
        assert gdh.normalize_grid_dict("wonderful")["path"] == {"2d": "r360x180"}
        assert gdh.normalize_grid_dict("romantic")["path"] == {"2d": "n128"}

        # errors on grid path
        with pytest.raises(FileNotFoundError, match="Grid file 'noise.nc' does not exist."):
            gdh.normalize_grid_dict("doing")
        with pytest.raises(TypeError, match="Grid path '{'pizza please!'}' is not a valid type."):
            gdh.normalize_grid_dict("tests")
        with pytest.raises(ValueError, match="Grid path 'banana' is not a valid CDO grid name nor a file path."):
            gdh.normalize_grid_dict("amazing")

    def test_regridder(self, regrid_paths, ifs_tco79_long_reader):
        """Testing the regridder all in independent way"""

        data = ifs_tco79_long_reader.retrieve()
        regridder = Regridder(cfg_grid_dict={"grids": {}, "paths": regrid_paths}, data=data.isel(time=0), loglevel="debug")

        # Regrid the data
        weights = regridder.weights(tgt_grid_name="r144x72", regrid_method="bil")

        # initialize regridder with weights
        regridder.initialize(weights)
        out = regridder.regrid(data)

        assert len(out.lon) == 144
        assert len(out.lat) == 72

        # bilinear interpolation cannot create values outside the range of the source
        source, regridded = data["2t"].isel(time=0), out["2t"].isel(time=0)
        assert source.min() <= regridded.min() and regridded.max() <= source.max()
        assert cosine_weighted_mean(regridded) == pytest.approx(285.72838, rel=APPROX_REL)

        # the cells of any grid add up to the surface of the sphere used by CDO
        sphere = 4 * np.pi * 6.371e6**2
        src_area = regridder.areas()
        assert len(src_area.lon) == 18
        assert len(src_area.lat) == 9
        assert float(src_area.cell_area.sum()) == pytest.approx(sphere)

        tgt_area = regridder.areas(tgt_grid_name="F32")
        assert len(tgt_area.lon) == 128
        assert len(tgt_area.lat) == 64
        assert float(tgt_area.cell_area.sum()) == pytest.approx(sphere)

    def test_basic_interpolation(self, reader_arguments):
        """
        Test basic interpolation,
        checking output grid dimension and
        fraction of land (i.e. any missing points)
        """
        model, exp, source, variable, ratio = reader_arguments

        reader = Reader(model=model, exp=exp, source=source, regrid="r200", fix=True, loglevel=LOGLEVEL)
        data = reader.retrieve()
        rgd = reader.regrid(data[variable])
        assert getattr(rgd, "AQUA_regridded") == 1
        assert getattr(rgd, "AQUA_target_grid") == "r200"
        assert len(rgd.lon) == 180
        assert len(rgd.lat) == 90
        assert ratio == pytest.approx((rgd.isnull().sum() / rgd.size).values, rel=APPROX_REL)  # land fraction
        assert hasattr(rgd, "AQUA_catalog")

        # test on Dataset regridding, through the accessor, of the variable under test only
        rgd = data[[variable]].aqua.regrid()[variable]
        assert hasattr(rgd, "AQUA_exp")

    def test_recompute_weights_fesom2d(self):
        """
        Test interpolation on FESOM, at different grid rebuilding weights,
        checking output grid dimension and fraction of land
        (i.e. any missing points)
        """
        reader = Reader(
            model="FESOM", exp="test-pi", source="original_2d", regrid="r100", rebuild=True, fix=False, loglevel=LOGLEVEL
        )
        data = reader.retrieve(var="sst")
        rgd = reader.regrid(data)

        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.time) == 2
        assert land_fraction(rgd["sst"]) == pytest.approx(0.35785494, rel=APPROX_REL)
        assert cosine_weighted_mean(rgd["sst"].isel(time=0)) == pytest.approx(17.959776, rel=APPROX_REL)

    def test_recompute_weights_ifs(self):
        """Test the case where no source grid path is specified in the regrid.yaml file
        and areas/weights are reconstructed from the file itself"""
        reader = Reader(model="IFS", exp="test-tco79", source="long", regrid="r100", rebuild=True, loglevel=LOGLEVEL)
        data = reader.retrieve(var="ttr")
        rgd = reader.regrid(data)

        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.time) == 4728
        assert cosine_weighted_mean(rgd["ttr"].isel(time=1000)) == pytest.approx(-13586109.26, rel=APPROX_REL)

    def test_recompute_weights_healpix(self):
        """Test Healpix and areas/weights are reconstructed from the file itself"""
        reader = Reader(model="ICON", exp="test-healpix", source="short", regrid="r100", rebuild=True)
        data = reader.retrieve(var="t")
        rgd = reader.regrid(data)

        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.height) == 90
        assert len(rgd.time) == 2
        assert cosine_weighted_mean(rgd["t"].isel(time=0, height=0)) == pytest.approx(207.20236, rel=APPROX_REL)
        assert cosine_weighted_mean(rgd["t"].isel(time=0, height=45)) == pytest.approx(215.01335, rel=APPROX_REL)

    @pytest.mark.xdist_group(name="regrid_fesom3d")  # same worker as the other user of the rebuilt weights
    def test_recompute_weights_fesom3d(self, fesom3d_r100_rebuilt):
        """
        Test interpolation on FESOM, at different grid rebuilding weights,
        checking output grid dimension and fraction of land
        (i.e. any missing points)
        """
        rgd = fesom3d_r100_rebuilt

        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.time) == 2
        # the land fraction grows with depth
        assert land_fraction(rgd.temp.isel(nz1=0)) == pytest.approx(0.35785494, rel=APPROX_REL)
        assert land_fraction(rgd.temp.isel(nz1=2)) == pytest.approx(0.45964509, rel=APPROX_REL)
        assert cosine_weighted_mean(rgd.temp.isel(nz1=0, time=0)) == pytest.approx(17.959776, rel=APPROX_REL)
        assert cosine_weighted_mean(rgd.temp.isel(nz1=2, time=0)) == pytest.approx(1.7581643, rel=APPROX_REL)

    @pytest.mark.xdist_group(name="regrid_nemo3d")  # same worker as the other user of the rebuilt weights
    def test_recompute_weights_nemo3d(self, nemo3d_r200_rebuilt):
        """
        Test interpolation on NEMO, at different grid rebuilding weights,
        checking output grid dimension and fraction of land
        (i.e. any missing points)
        """
        rgd = nemo3d_r200_rebuilt

        assert len(rgd.lon) == 180
        assert len(rgd.lat) == 90
        # the land fraction grows with depth
        # (one dictionary, so that a failure reports every value: the weights are rebuilt by the CDO of the machine)
        deep, surface = rgd.avg_so.isel(level=6), rgd.avg_so.isel(level=0)
        assert {
            "land fraction, surface": land_fraction(surface),
            "land fraction, level 6": land_fraction(deep),
            "mean, surface": cosine_weighted_mean(surface),
            "mean, level 6": cosine_weighted_mean(deep),
        } == pytest.approx(
            {
                "land fraction, surface": 0.33777778,
                "land fraction, level 6": 0.52462963,
                "mean, surface": 33.982137,
                "mean, level 6": 35.184904,
            },
            rel=APPROX_REL,
        )

    @pytest.mark.xdist_group(name="regrid_nemo3d")  # same worker as the other user of the rebuilt weights
    def test_skipna_nemo3d(self, nemo3d_r200_rebuilt):
        """
        Test the 'skipna' grid path key on NEMO 3D: a single 2D weights matrix
        reproduces the 3D-masked regridding
        """
        ref = nemo3d_r200_rebuilt  # 3D-masked regridding of the same source, with rebuilt weights

        reader = Reader(
            model="NEMO",
            exp="test-eORCA1",
            source="short-3d-skipna",
            regrid="r200",
            rebuild=True,
            datamodel=False,
            fix=False,
            loglevel=LOGLEVEL,
        )
        assert reader.regridder.skipna
        assert list(reader.regridder.smmregridder) == ["skipna"]
        filename = reader.regridder.filenames.weights_filename(
            "r200", reader.regridder.src_grid_path, "ycon", "skipna", reader.regridder_metadata
        )
        assert filename.endswith("_lskipna.nc")

        rgd = reader.regrid(reader.retrieve(var="avg_so"))
        assert len(rgd.lon) == 180
        assert len(rgd.lat) == 90
        for level in [0, 6]:
            skipna, masked = rgd.avg_so.isel(level=level), ref.avg_so.isel(level=level)
            # na_thres 0.5 as without skipna: same land fraction, up to a few cells
            assert abs(skipna.isnull().mean() - masked.isnull().mean()) < 1e-3
            both = skipna.notnull() & masked.notnull()
            xr.testing.assert_allclose(skipna.where(both), masked.where(both))
        # the mask still varies with the level
        assert rgd.avg_so.isel(level=6).isnull().sum() > rgd.avg_so.isel(level=0).isnull().sum()

    def test_skipna_combined_keys(self):
        """The 'skipna' path key cannot be combined with other keys"""
        cfg = {"grids": {"mixed": {"path": {"2d": "r100x50", "skipna": "r100x50"}}}}
        with pytest.raises(ValueError, match="cannot be combined"):
            Regridder(cfg, src_grid_name="mixed", loglevel=LOGLEVEL)

    def test_skipna_from_data(self, regrid_paths):
        """The 'data' path value takes the grid from the dataset: masks varying in time are handled"""
        cfg = {"grids": {"lonlat-skipna": {"path": {"skipna": "data"}, "space_coord": ["lon", "lat"]}}, "paths": regrid_paths}
        gdh = GridDictHandler(cfg)
        assert gdh.normalize_grid_dict("lonlat-skipna")["path"] == {"skipna": "data"}
        assert GridDictHandler({"grids": {"d": {"path": "data"}}}).normalize_grid_dict("d")["path"] == {"2d": "data"}

        data = np.full((3, 18, 36), 280.0)
        data[1, 2:6, 4:10] = np.nan  # the mask changes with time
        da = xr.DataArray(
            data,
            coords={"time": np.arange(3), "lat": np.arange(-85, 90, 10.0), "lon": np.arange(5, 360, 10.0)},
            dims=("time", "lat", "lon"),
            name="temp",
        )
        da.lat.attrs.update({"standard_name": "latitude", "units": "degrees_north"})
        da.lon.attrs.update({"standard_name": "longitude", "units": "degrees_east"})

        # without the dataset the grid is not available
        assert Regridder(cfg, src_grid_name="lonlat-skipna", loglevel=LOGLEVEL).error

        regridder = Regridder(cfg, src_grid_name="lonlat-skipna", data=da.isel(time=0), loglevel=LOGLEVEL)
        assert regridder.skipna and not regridder.error
        assert cfg["grids"]["lonlat-skipna"]["path"] == {"skipna": "data"}  # configuration untouched
        regridder.weights(tgt_grid_name="r18x9", regrid_method="ycon", rebuild=True)
        assert list(regridder.smmregridder) == ["skipna"]
        out = regridder.regrid(da)
        assert out.sizes["lon"] == 18 and out.sizes["lat"] == 9
        # constant field: any mask gives the same value, no garbage
        assert float(out.min()) == pytest.approx(280.0) and float(out.max()) == pytest.approx(280.0)
        assert out.isel(time=1).notnull().sum() > 0.8 * out.isel(time=1).size

    @pytest.mark.xdist_group(name="regrid_fesom3d")  # same worker as the rebuild of the weights it uses
    def test_levels_and_regrid(self, fesom3d_r100_rebuilt):
        """
        Test regridding selected levels, with the weights rebuilt by the fixture.
        """
        reader = Reader(model="FESOM", exp="test-pi", source="original_3d", datamodel=False, regrid="r100", loglevel=LOGLEVEL)
        data = reader.retrieve()

        layers = [0, 2]
        val = data.aqua.regrid().isel(time=1, nz=2, nz1=layers).wo.aqua.fldmean().values
        # assert val == pytest.approx(8.6758228e-08) #smmregrid <= v0.1.3
        assert val == pytest.approx(7.00622013e-08, rel=APPROX_REL)
        val = data.isel(time=1, nz=2, nz1=layers).aqua.regrid().wo.aqua.fldmean().values
        # assert val == pytest.approx(8.6758228e-08) #smmregrid <= v0.1.3
        assert val == pytest.approx(7.00622013e-08, rel=APPROX_REL)
        val = data.isel(time=1, nz=2, nz1=layers).wo.aqua.regrid().aqua.fldmean().values
        # assert val == pytest.approx(8.6758228e-08) #smmregrid <= v0.1.3
        assert val == pytest.approx(7.00622013e-08, rel=APPROX_REL)
        val = data.isel(time=1, nz=2, nz1=layers).aqua.regrid().thetao.isel(nz1=1).aqua.fldmean().values
        # assert val == pytest.approx(274.9045) #smmregrid <= v0.1.3
        assert val == pytest.approx(274.90709, rel=APPROX_REL)
        val = data.aqua.regrid().isel(time=1, nz=2, nz1=layers).thetao.isel(nz1=1).aqua.fldmean().values
        # assert val == pytest.approx(274.9045) #smmregrid <= v0.1.3
        assert val == pytest.approx(274.90709, rel=APPROX_REL)
        val = data.isel(time=1, nz=2, nz1=layers).thetao.aqua.regrid().isel(nz1=1).aqua.fldmean().values
        # assert val == pytest.approx(274.9045) #smmregrid <= v0.1.3
        assert val == pytest.approx(274.90709, rel=APPROX_REL)

        # test reading specific levels for first vertical coordinate (nz1)
        data = reader.retrieve(level=[2.5, 2275], level_coord="nz1")
        val = data.isel(time=1).aqua.regrid().thetao.isel(nz1=1).aqua.fldmean().values
        # assert val == pytest.approx(274.9045) #smmregrid <= v0.1.3
        assert val == pytest.approx(274.90709, rel=APPROX_REL)

    @pytest.mark.parametrize("as_dataset", [True, False], ids=["dataset", "dataarray"])
    def test_regridder_without_weights(self, as_dataset, aqua_caplog):
        """Regridding with no weights initialised logs the error and does not regrid: a Dataset comes back
        empty, a DataArray comes back unchanged"""
        field = xr.DataArray(
            np.full((10, 20), 280.0),
            coords={"lat": np.arange(-90, 90, 18), "lon": np.arange(0, 360, 18)},
            dims=("lat", "lon"),
            name="temp",
        )
        field.lat.attrs.update({"standard_name": "latitude", "units": "degrees_north"})
        field.lon.attrs.update({"standard_name": "longitude", "units": "degrees_east"})
        data = field.to_dataset() if as_dataset else field
        regridder = Regridder(data=data, loglevel="ERROR")  # the weights are never generated nor initialised
        caplog = aqua_caplog(regridder.logger)

        out = regridder.regrid(data)

        assert "Regridder for vertical coordinate 2d not found." in caplog.messages
        if as_dataset:
            assert "Cannot regrid variables ['temp']" in caplog.messages
            assert len(out.data_vars) == 0
        else:
            assert "Cannot regrid variable temp" in caplog.messages
            xr.testing.assert_identical(out, field)

    def test_regridder_wrong_data_type(self):
        """Only a Dataset or a DataArray can be regridded"""
        regridder = Regridder(src_grid_name="r36x18", loglevel="ERROR")
        with pytest.raises(ValueError, match="Data must be an xarray Dataset or DataArray."):
            regridder.regrid("not a dataset")

    @pytest.mark.parametrize("paths", [None, {"areas": "unused"}])
    def test_filename_without_weights_path(self, paths):
        """Without a `paths` block, or without its `weights` entry, the weights name has no directory."""
        handler = FilenameHandler({"paths": paths} if paths else {}, loglevel=LOGLEVEL)
        assert handler.weights_filename("r18x9", {"2d": "r36x18"}, "ycon", "2d", None) == "weights_r18x9_ycon_l2d.nc"

    def test_regridder_weights(self, regrid_paths, aqua_caplog):
        """Test regridding using explicitly the Regridder class and the weights method: without initialising
        the weights nothing is regridded, with them a constant field stays constant on the new grid."""

        field = xr.DataArray(
            np.full((10, 20), 280.0),
            coords={"lat": np.arange(-90, 90, 18), "lon": np.arange(0, 360, 18)},
            dims=("lat", "lon"),
            name="temp",
        )
        field.lat.attrs.update({"standard_name": "latitude", "units": "degrees_north"})
        field.lon.attrs.update({"standard_name": "longitude", "units": "degrees_east"})

        regridder = Regridder(cfg_grid_dict={"grids": {}, "paths": regrid_paths}, data=field)
        caplog = aqua_caplog(regridder.logger)
        regridder.weights(tgt_grid_name="r30x15", regrid_method="bil", initialize=False)
        out = regridder.regrid(field)
        assert "Cannot regrid variable temp" in caplog.messages  # the weights were not initialised
        xr.testing.assert_identical(out, field)  # nothing was regridded

        caplog.clear()
        regridder = Regridder(cfg_grid_dict={"grids": {}, "paths": regrid_paths}, data=field)
        aqua_caplog(regridder.logger)
        regridder.weights(tgt_grid_name="r30x15", regrid_method="bil", initialize=True)
        out = regridder.regrid(field)
        assert not [message for message in caplog.messages if "Cannot regrid" in message]
        assert out.shape == (15, 30)
        assert out.values == pytest.approx(280.0)  # a constant field is not changed by the interpolation

    def test_regridder_invalid_data_type(self):
        """Test _apply_regrid when data is neither Dataset nor DataArray (Line 639)."""
        regridder = Regridder(src_grid_name="r36x18", loglevel="ERROR")

        with pytest.raises(ValueError, match="Data must be an xarray Dataset or DataArray."):
            regridder._apply_regrid("not a dataset", {})


@pytest.mark.aqua
def test_non_latlon_interpolation():
    """
    Test interpolation to a non regular grid,
    checking appropriate logging message
    """
    reader = Reader(model="IFS", exp="test-tco79", source="short", regrid="F80", fix=True, loglevel=LOGLEVEL, rebuild=True)

    data = reader.retrieve(var="2t")["2t"].isel(time=0).aqua.regrid()

    assert data.shape == (160, 320)
    assert data.values[0, 0] == pytest.approx(246.71156470963325)


@pytest.mark.aqua
def test_regrid_method():
    """Test different regridding method from the grid file"""
    reader = Reader(
        model="ERA5", exp="era5-hpz3", source="monthly-nn", regrid="r100", fix=True, loglevel=LOGLEVEL, rebuild=True
    )

    data = reader.retrieve(var="2t")["2t"].isel(time=0).aqua.regrid()

    assert data.shape == (180, 360)
    assert data.values[0, 0] == pytest.approx(242.4824981689453)

    reader = Reader(
        model="ERA5",
        exp="era5-hpz3",
        source="monthly-nn",
        regrid="r100",
        fix=True,
        loglevel=LOGLEVEL,
        rebuild=True,
        regrid_method="bil",
    )
    data = reader.retrieve(var="2t")["2t"].isel(time=0).aqua.regrid()

    assert data.shape == (180, 360)
    assert data.values[0, 0] == pytest.approx(252.35510736926696)


# missing test for ICON-Healpix
