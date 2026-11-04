"""Testing if fldmean method works"""

import numpy as np
import pytest
from utils_tests import LOGLEVEL

from aqua import FldStat, Reader


# Aliases with module scope for fixtures
@pytest.fixture(scope="module")
def reader_fesom(fesom_test_pi_original_2d_reader):
    return fesom_test_pi_original_2d_reader


@pytest.fixture(scope="module")
def data_fesom(fesom_test_pi_original_2d_data):
    return fesom_test_pi_original_2d_data


@pytest.fixture(scope="module")
def reader_icon(icon_test_healpix_short_reader):
    return icon_test_healpix_short_reader


@pytest.fixture(scope="module")
def data_icon(icon_test_healpix_short_data):
    return icon_test_healpix_short_data


@pytest.fixture(scope="module")
def reader_icon_r2b0(icon_test_r2b0_short_reader):
    return icon_test_r2b0_short_reader


@pytest.fixture(scope="module")
def data_icon_r2b0(icon_test_r2b0_short_data):
    return icon_test_r2b0_short_data


@pytest.fixture(scope="module")
def reader_nemo_2d(nemo_test_e_orca1_long_2d_reader):
    return nemo_test_e_orca1_long_2d_reader


@pytest.fixture(scope="module")
def data_nemo_2d(nemo_test_e_orca1_long_2d_data):
    return nemo_test_e_orca1_long_2d_data


@pytest.fixture(scope="module")
def reader_nemo_3d(nemo_test_e_orca1_short_3d_reader):
    return nemo_test_e_orca1_short_3d_reader


@pytest.fixture(scope="module")
def data_nemo_3d(nemo_test_e_orca1_short_3d_data):
    return nemo_test_e_orca1_short_3d_data


@pytest.fixture(scope="module")
def reader_ifs(ifs_tco79_short_reader):
    return ifs_tco79_short_reader


@pytest.fixture(scope="module")
def reader_ifs_noarea(ifs_tco79_long_areasfalse_fixfalse_reader):
    return ifs_tco79_long_areasfalse_fixfalse_reader


@pytest.fixture(scope="module")
def data_ifs(ifs_tco79_short_data_2t):
    return ifs_tco79_short_data_2t


# Test classes
@pytest.mark.aqua
class TestFldModule:
    """Class for fldmean standalone"""

    def test_fldmean_from_data(self, ifs_tco79_short_reader, ifs_tco79_short_data_2t):
        """FldStat on an unstructured grid: with the cell area it is the area-weighted mean, without it the plain mean"""
        area = ifs_tco79_short_reader.src_grid_area.cell_area
        temperature = ifs_tco79_short_data_2t["2t"].values  # (time, cell)

        weighted = FldStat(area=area, loglevel=LOGLEVEL).fldstat(ifs_tco79_short_data_2t, stat="mean")["2t"].values
        unweighted = FldStat(loglevel=LOGLEVEL).fldstat(ifs_tco79_short_data_2t, stat="mean")["2t"].values

        assert weighted == pytest.approx(np.average(temperature, axis=1, weights=area.values))  # independent oracle
        assert weighted[1] == pytest.approx(285.75920)
        assert unweighted == pytest.approx(temperature.mean(axis=1))
        assert unweighted[1] == pytest.approx(286.38151)
        assert (unweighted - weighted).min() > 0.5  # the polar cells are small: the weights matter, by about 0.6 K

    def test_fldmean_from_data_selection(self, ifs_tco79_long_reader):
        """FldStat on a regular grid with reversed latitude gives the same area-weighted mean"""
        data = ifs_tco79_long_reader.retrieve(var="2t").isel(time=slice(0, 3))
        area = ifs_tco79_long_reader.src_grid_area.cell_area
        reverted = data.reindex({"lat": list(reversed(data.coords["lat"]))})
        fldmodule = FldStat(area=area, loglevel=LOGLEVEL)

        expected = data["2t"].weighted(area).mean(("lon", "lat")).values  # independent oracle
        assert fldmodule.fldstat(data, stat="mean")["2t"].values == pytest.approx(expected)
        assert fldmodule.fldstat(reverted, stat="mean")["2t"].values == pytest.approx(expected)
        assert expected[0] == pytest.approx(286.07560)

    def test_fldmean_raise(self):
        """test Fldmean class raise error if no area provided"""
        with pytest.raises(ValueError, match="Area must be an xarray DataArray or Dataset."):
            FldStat(loglevel=LOGLEVEL, area="pippo")


@pytest.mark.aqua
class TestFldmean:
    """Test class for fldmean"""

    @pytest.mark.parametrize(
        ("source, value, shape"), [("short", 285.75920, 2), ("short_nn", 285.75920, 2), ("long", 285.86724, 20)]
    )
    def test_fldmean_ifs(self, source, value, shape):
        """Fldmean test for IFS"""
        reader = Reader(model="IFS", exp="test-tco79", source=source, loglevel=LOGLEVEL)
        data = reader.retrieve(var="2t")
        if source == "long":
            data = data.isel(time=slice(0, 20))
        avg = reader.fldmean(data["2t"]).values
        assert avg.shape == (shape,)
        assert avg[1] == pytest.approx(value)

    def test_fldmean_fesom(self, reader_fesom, data_fesom):
        """Fldmean test for FESOM"""
        avg = reader_fesom.fldmean(data_fesom["tos"]).values
        assert avg.shape == (2,)
        # assert avg[1] == pytest.approx(17.9806)
        assert avg[1] == pytest.approx(291.1306)

    def test_fldmean_fesom_selection(self):
        """Fldmean test for FESOM"""
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d", regrid="r100", loglevel=LOGLEVEL)
        data = reader.retrieve(var="tos")
        data = reader.regrid(data)
        avg = reader.fldmean(data["tos"], lon_limits=[50, 90], lat_limits=[10, 40]).values
        assert avg.shape == (2,)
        # assert avg[1] == pytest.approx(17.9806)
        assert avg[1] == pytest.approx(300.1865)

    def test_fldmean_healpix(self, reader_icon, data_icon):
        """Fldmean test for ICON"""
        avg = reader_icon.fldmean(data_icon["2t"]).values
        assert avg.shape == (2,)
        assert avg[1] == pytest.approx(286.1479)

    def test_fldmean_healpix_selection(self):
        """Fldmean test for ICON with area selection"""
        reader = Reader(model="ICON", exp="test-healpix", source="short", regrid="r200", loglevel=LOGLEVEL)
        data = reader.retrieve(var="2t")
        data = reader.regrid(data)
        avg = reader.fldmean(data["2t"], lon_limits=[-30, 50], lat_limits=[-30, -90])
        assert "Area selection: lat=[-90, -30], lon=[330, 50]" in avg.history
        assert avg.values.shape == (2,)
        assert avg.values[0] == pytest.approx(285.131484)

    def test_fldmean_healpix_selection_lat_only(self):
        """Fldmean test for ICON with area selection, only lat"""
        reader = Reader(model="ICON", exp="test-healpix", source="short", regrid="r200", loglevel=LOGLEVEL)
        data = reader.retrieve(var="2t")
        data = reader.regrid(data)
        avg = reader.fldmean(data["2t"], lat_limits=[-30, 30]).values
        assert avg.shape == (2,)
        assert avg[0] == pytest.approx(292.6823)

    def test_fldmean_icon(self, reader_icon_r2b0, data_icon_r2b0):
        """Fldmean test for ICON"""
        avg = reader_icon_r2b0.fldmean(data_icon_r2b0["t"]).values
        assert avg.shape == (2, 90)
        assert avg[1, 1] == pytest.approx(214.4841)

    def test_fldmean_regridded(self):
        """Fldmean test for regridded data: the mean of the regridded field, close to but not the native one"""
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d", regrid="r250", loglevel=LOGLEVEL)
        data = reader.retrieve(var="tos")
        avg = reader.fldmean(reader.regrid(data)["tos"]).values
        assert avg.shape == (2,)
        assert avg[1] == pytest.approx(291.06373)
        assert abs(avg[1] - reader.fldmean(data["tos"]).values[1]) > 0.01  # the regridding changes the mean

    def test_fldmean_nemo(self, reader_nemo_2d, data_nemo_2d):
        """Fldmean test for NEMO"""
        avg = reader_nemo_2d.fldmean(data_nemo_2d["tos"]).values
        assert avg.shape == (6,)
        assert avg[5] == pytest.approx(290.5516)

    def test_fldmean_nemo_3d(self, reader_nemo_3d, data_nemo_3d):
        """Fldmean test for NEMO-3D"""
        avg = reader_nemo_3d.fldmean(data_nemo_3d["avg_so"]).values
        assert avg.shape == (8,)
        assert avg[4] == pytest.approx(34.63406)

    def test_fldmean_era5_toward_gaussian(self):
        """Fldmean test for ERA5 toward gaussian grid and verification of fldmean"""
        reader = Reader(model="ERA5", exp="era5-hpz3", source="monthly", regrid="F32", loglevel=LOGLEVEL)
        data = reader.retrieve(var="2t").isel(time=0)
        regrid = reader.regrid(data)
        avg_reg = reader.fldmean(regrid["2t"])
        assert regrid.lat[0] > regrid.lat[1]  # verify lat decreasing
        assert avg_reg.values == pytest.approx(285.426661)  # verify fldmean value

    def test_areasel_areasfalse(self, reader_ifs_noarea):
        """Area selection test for IFS long source with areas=False"""
        data_ifs = reader_ifs_noarea.retrieve(var="2t")
        area_sel = reader_ifs_noarea.select_area(data_ifs, lon=[0, 360], lat=[-80, 80], drop=True)
        assert area_sel.lat.min().values == pytest.approx(-67.5)


@pytest.mark.aqua
class TestFldStatDims:
    """Test class for dims parameter functionality"""

    def test_fldmean_dims_icon(self, reader_icon_r2b0, data_icon_r2b0):
        """On ICON the only horizontal dimension is cell: explicit dims equal the default, height is kept"""
        fldmodule = FldStat(area=reader_icon_r2b0.src_grid_area.cell_area, loglevel=LOGLEVEL)

        result_default = fldmodule.fldstat(data_icon_r2b0["t"], stat="mean")
        result = fldmodule.fldstat(data_icon_r2b0["t"], stat="mean", dims=["cell"])

        assert result.dims == ("time", "height")
        assert result.shape == (2, 90)
        assert result.equals(result_default)
        assert result.values[1, 1] == pytest.approx(214.4841)  # Same value as existing test

    def test_fldmean_partial_dims_latlon(self, ifs_tco79_long_reader):
        """With two horizontal dimensions, dims selects the one that is averaged: the other one is kept"""
        data = ifs_tco79_long_reader.retrieve(var="2t").isel(time=slice(0, 3))
        area = ifs_tco79_long_reader.src_grid_area.cell_area
        fldmodule = FldStat(area=area, loglevel=LOGLEVEL)

        lon_only = fldmodule.fldstat(data, stat="mean", dims=["lon"])["2t"]
        both = fldmodule.fldstat(data, stat="mean", dims=["lat", "lon"])["2t"]

        assert lon_only.dims == ("time", "lat")
        assert lon_only.shape == (3, 9)
        assert lon_only.values == pytest.approx(data["2t"].weighted(area).mean("lon").values)  # independent oracle
        assert both.dims == ("time",)
        assert both.equals(fldmodule.fldstat(data, stat="mean")["2t"])

    def test_fldmean_dims_validation_icon(self, reader_icon_r2b0, data_icon_r2b0):
        """Test dims validation with ICON data"""
        fldmodule = FldStat(area=reader_icon_r2b0.src_grid_area.cell_area, loglevel=LOGLEVEL)

        # Test invalid dimension
        with pytest.raises(ValueError, match="Dimension invalid_dim not found in horizontal dimensions"):
            fldmodule.fldstat(data_icon_r2b0["t"], stat="mean", dims=["invalid_dim"])

    def test_fldmean_dims_not_list(self, reader_icon_r2b0, data_icon_r2b0):
        """Test that dims must be a list"""
        fldmodule = FldStat(area=reader_icon_r2b0.src_grid_area.cell_area, loglevel=LOGLEVEL)

        with pytest.raises(ValueError, match="dims must be a list of dimension names."):
            fldmodule.fldstat(data_icon_r2b0["t"], stat="mean", dims="cell")


@pytest.mark.aqua
class TestFldStatWrappers:
    """Test class for fldstat wrapper methods"""

    def test_fldmean(self, reader_ifs, data_ifs):
        """Test fldmean wrapper method"""
        avg = reader_ifs.fldmean(data_ifs["2t"])
        assert avg[1] == pytest.approx(285.75920)
        avg_st = reader_ifs.fldstat(data_ifs["2t"], stat="mean")
        assert avg[1] == pytest.approx(285.75920)
        assert avg_st[1] == pytest.approx(285.75920)

    def test_fldmax(self, reader_ifs, data_ifs):
        """Test fldmax wrapper method"""
        maxval = reader_ifs.fldmax(data_ifs["2t"])
        avg = reader_ifs.fldmean(data_ifs["2t"])
        assert maxval[1].values == pytest.approx(310.6103)
        # max should be greater than or equal to mean
        assert maxval[0] >= avg[0]

    def test_fldmin(self, reader_ifs, data_ifs):
        """Test fldmin wrapper method"""
        minval = reader_ifs.fldmin(data_ifs["2t"])
        assert minval[1].values == pytest.approx(232.79393)
        maxval = reader_ifs.fldmax(data_ifs["2t"])
        assert minval[0] <= maxval[0]

    def test_fldstd(self, reader_ifs, data_ifs):
        """Test fldstd wrapper method"""
        stdval = reader_ifs.fldstd(data_ifs["2t"])
        assert stdval[1].values == pytest.approx(15.6998)

    def test_fldsum(self, reader_ifs, data_ifs):
        """Test fldsum wrapper method"""
        sumval = reader_ifs.fldsum(data_ifs["2t"])
        assert sumval[1].values == pytest.approx(1.4572956e17)

    def test_fldintg(self, reader_ifs, data_ifs):
        """Test fldintg wrapper method"""
        intgval = reader_ifs.fldintg(data_ifs["2t"])
        assert intgval[0].values == pytest.approx(1.4583612e17)

    def test_fldarea(self, reader_ifs, data_ifs):
        """Test fldsum wrapper method"""
        asumval = reader_ifs.fldarea(data_ifs["2t"])
        assert asumval[0].values == pytest.approx(5.0997329e14)

    def test_fldstat_attrs(self, reader_ifs, data_ifs):
        """The statistics keep the AQUA attributes and set units and history consistently"""
        data = data_ifs["2t"]

        mean = reader_ifs.fldmean(data)
        assert mean.attrs["units"] == "K"
        assert mean.attrs["AQUA_model"] == "IFS"
        assert "Computed field stat 'mean'" in mean.attrs["history"]

        integral = reader_ifs.fldintg(data)
        assert integral.attrs["units"] == "kelvin * meter ** 2"
        assert integral.attrs["long_name"] == "Integrated 2 metre temperature"
        assert "Computed field stat 'integral'" in integral.attrs["history"]

        area = reader_ifs.fldarea(data)
        assert area.attrs["units"] == "m2"
        assert "Computed field stat 'areasum'" in area.attrs["history"]

    def test_fldstat_compare(self, reader_ifs, data_ifs):
        """Test that wrapper methods give consistent results"""
        data_var = data_ifs["2t"]

        # Compare wrapper methods with generic fldstat
        mean_wrapper = reader_ifs.fldmean(data_var)
        mean_generic = reader_ifs.fldstat(data_var, stat="mean")

        max_wrapper = reader_ifs.fldmax(data_var)
        max_generic = reader_ifs.fldstat(data_var, stat="max")

        assert mean_wrapper.equals(mean_generic)
        assert max_wrapper.equals(max_generic)

        # Test logical relationships
        assert (reader_ifs.fldmin(data_var) <= reader_ifs.fldmean(data_var)).all()
        assert (reader_ifs.fldmean(data_var) <= reader_ifs.fldmax(data_var)).all()


@pytest.mark.aqua
class TestAccessorFldStat:
    """Test class for fldstat methods exposed via the aqua accessor"""

    def test_accessor_fldstat_generic(self, reader_ifs, data_ifs):
        """Accessor fldstat(stat='mean') must match Reader.fldstat(stat='mean')."""
        reader_ifs.set_default()
        data_var = data_ifs["2t"]

        mean_accessor = data_var.aqua.fldstat(stat="mean")
        mean_reader = reader_ifs.fldstat(data_var, stat="mean")

        assert mean_accessor.equals(mean_reader)

    @pytest.mark.parametrize(
        ("method_name", "stat_name"),
        [
            ("fldmean", "mean"),
            ("fldmax", "max"),
            ("fldmin", "min"),
            ("fldstd", "std"),
            ("fldsum", "sum"),
            ("fldintg", "integral"),
            ("fldarea", "areasum"),
        ],
    )
    def test_accessor_wrappers_match_reader(self, reader_ifs, data_ifs, method_name, stat_name):
        """
        Each fld* accessor method must give the same result as the corresponding
        Reader.fldstat(..., stat=...) wrapper.
        """
        reader_ifs.set_default()
        data_var = data_ifs["2t"]

        accessor_result = getattr(data_var.aqua, method_name)()
        reader_result = reader_ifs.fldstat(data_var, stat=stat_name)

        assert accessor_result.equals(reader_result)


@pytest.mark.aqua
class TestAlignAreaCoordinates:
    """
    Unit tests for FldStat.align_area_coordinates().
    Covers the fix for a rounding-boundary comparison bug on grids whose
    spacing is a power-of-two fraction of 360 (e.g. reduced Gaussian grids).
    """

    @staticmethod
    def _make_area_and_data(lon_area, lon_data, lat=None):
        import xarray as xr

        lat = lat if lat is not None else np.array([0.0, 10.0, 20.0])

        area = xr.DataArray(
            np.ones((len(lat), len(lon_area))),
            dims=["lat", "lon"],
            coords={"lat": lat, "lon": lon_area},
            name="cell_area",
        )
        data = xr.DataArray(
            np.ones((2, len(lat), len(lon_data))),
            dims=["time", "lat", "lon"],
            coords={"time": [0, 1], "lat": lat, "lon": lon_data},
            name="var",
        )
        return area, data

    def test_exact_match_fast_path(self):
        """Coordinates identical: fast path, no change needed."""
        lon = np.array([0.0, 30.0, 60.0, 90.0])
        area, data = self._make_area_and_data(lon, lon.copy())

        fld = FldStat(area=area, horizontal_dims=["lat", "lon"], loglevel=LOGLEVEL)
        aligned = fld.align_area_coordinates(data, area=area)

        np.testing.assert_array_equal(aligned.lon.values, data.lon.values)

    def test_reversed_coordinate_alignment(self):
        """Area lon reversed relative to data: should reindex, not raise."""
        lon_data = np.array([0.0, 30.0, 60.0, 90.0])
        lon_area = lon_data[::-1].copy()
        area, data = self._make_area_and_data(lon_area, lon_data)

        fld = FldStat(area=area, horizontal_dims=["lat", "lon"], loglevel=LOGLEVEL)
        aligned = fld.align_area_coordinates(data, area=area)

        np.testing.assert_array_equal(aligned.lon.values, lon_data)

    def test_floating_point_noise_within_tolerance_aligns(self):
        """Reproduces the float noise bug landing exactly on a rounding boundary."""
        lon_data = np.array([28.828125, 45.703125, 55.546875, 61.171875])
        lon_area = lon_data + np.array([0.0, 0.0, -4e-7, 3e-7])
        area, data = self._make_area_and_data(lon_area, lon_data)

        fld = FldStat(area=area, horizontal_dims=["lat", "lon"], loglevel=LOGLEVEL)
        aligned = fld.align_area_coordinates(data, area=area)

        np.testing.assert_array_equal(aligned.lon.values, lon_data)

    def test_tolerance_boundary_just_within_aligns(self):
        """A difference just under the tolerance threshold must still align."""
        lon_data = np.array([50.0])
        lon_area = np.array([50.0 + 1.4e-5])  # atol is 1.5e-5
        area, data = self._make_area_and_data(lon_area, lon_data, lat=np.array([0.0]))

        fld = FldStat(area=area, horizontal_dims=["lat", "lon"], loglevel=LOGLEVEL)
        aligned = fld.align_area_coordinates(data, area=area)

        np.testing.assert_array_equal(aligned.lon.values, lon_data)

    def test_tolerance_boundary_just_outside_raises(self):
        """A difference clearly beyond the tolerance must still raise."""
        lon_data = np.array([50.0])
        lon_area = np.array([50.0 + 5e-5])
        area, data = self._make_area_and_data(lon_area, lon_data, lat=np.array([0.0]))

        fld = FldStat(area=area, horizontal_dims=["lat", "lon"], loglevel=LOGLEVEL)
        with pytest.raises(ValueError, match="Mismatch in values for coordinate 'lon'"):
            fld.align_area_coordinates(data, area=area)

    def test_genuine_convention_mismatch_still_raises(self):
        """A real longitude-convention mismatch (0-360 vs -180/180) must still raise."""
        lon_data = np.array([0.0, 18.0, 36.0, 342.0])
        lon_area = np.array([0.0, 18.0, 36.0, -18.0])
        area, data = self._make_area_and_data(lon_area, lon_data)

        fld = FldStat(area=area, horizontal_dims=["lat", "lon"], loglevel=LOGLEVEL)
        with pytest.raises(ValueError, match="Mismatch in values for coordinate 'lon'"):
            fld.align_area_coordinates(data, area=area)

    def test_size_mismatch_raises(self):
        """A genuine length mismatch must raise before any value comparison is attempted."""
        lon_data = np.array([0.0, 30.0, 60.0, 90.0])
        lon_area = np.array([0.0, 30.0, 60.0])
        area, data = self._make_area_and_data(lon_area, lon_data)

        fld = FldStat(area=area, horizontal_dims=["lat", "lon"], loglevel=LOGLEVEL)
        with pytest.raises(ValueError, match="lon has a mismatch in length"):
            fld.align_area_coordinates(data, area=area)
