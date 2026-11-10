import os

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import pytest
import xarray as xr
from matplotlib.collections import QuadMesh
from matplotlib.font_manager import FontProperties
from utils_tests import DPI, LOGLEVEL

from aqua import Reader
from aqua.core.graphics import (
    boxplot,
    index_plot,
    indexes_plot,
    plot_gregory_annual,
    plot_gregory_monthly,
    plot_histogram,
    plot_hovmoller,
    plot_lat_lon_profiles,
    plot_maps,
    plot_maps_diff,
    plot_seasonal_lat_lon_profiles,
    plot_seasonalcycle,
    plot_single_map,
    plot_single_map_diff,
    plot_timeseries,
    plot_vertical_lines,
    plot_vertical_profile,
    plot_vertical_profile_diff,
)

pytestmark = pytest.mark.graphics


@pytest.fixture(scope="module")
def data_ifs_tc():
    return Reader(model="IFS", exp="test-tco79", source="teleconnections", fix=True, loglevel=LOGLEVEL).retrieve(var="skt")


@pytest.fixture(scope="module")
def sst_maps(fesom_test_pi_original_2d_r200_fixfalse_reader, fesom_test_pi_original_2d_r200_fixfalse_data):
    """Two regridded FESOM SST maps (first and second time step)"""
    sst = fesom_test_pi_original_2d_r200_fixfalse_reader.regrid(fesom_test_pi_original_2d_r200_fixfalse_data["sst"])
    return sst.isel(time=0), sst.isel(time=1)


def _legend_texts(ax):
    """Texts of the legend of an Axes, None without legend."""
    legend = ax.get_legend()
    return None if legend is None else [text.get_text() for text in legend.get_texts()]


def _absmax(data):
    """Largest absolute value of the data, the limit of a symmetric colorbar."""
    return float(abs(data).max())


class TestMaps:
    """Tests for the single and multiple map functions"""

    @pytest.mark.parametrize("sym", [True, False])
    def test_plot_single_map(self, tmp_path, sst_maps, sym):
        """Title, extent, colorbar label and limits: with sym=True the given vmin/vmax are replaced by symmetric ones."""
        plot_data = sst_maps[0]
        fig, ax = plot_single_map(
            data=plot_data,
            proj=ccrs.PlateCarree(),
            contour=False,
            extent=[-180, 180, -90, 90],
            nlevels=5,
            vmin=-2.0,
            vmax=30.0,
            sym=sym,
            cmap="viridis",
            display=False,
            return_fig=True,
            transform_first=False,
            title="Test plot",
            cbar_label="Sea surface temperature [°C]",
            nxticks=5,
            nyticks=5,
            ticks_rounding=1,
            cbar_ticks_rounding=1,
            loglevel=LOGLEVEL,
        )
        assert ax.get_title() == "Test plot"
        assert (ax.get_xlim(), ax.get_ylim()) == ((-180, 180), (-90, 90))
        assert isinstance(ax.collections[0], QuadMesh)  # contour=False
        expected_clim = (-_absmax(plot_data), _absmax(plot_data)) if sym else (-2.0, 30.0)
        assert ax.collections[0].get_clim() == pytest.approx(expected_clim)
        assert fig.axes[1].get_xlabel() == "Sea surface temperature [°C]"

        if sym:
            fig.savefig(os.path.join(tmp_path, "test_plot_single_map.png"), dpi=DPI)

    def test_plot_single_map_hpx(self, era5_hpz3_monthly_data):
        """HEALPix data: title and colorbar label from the attributes, filled contours from the data minimum to maximum."""
        data = era5_hpz3_monthly_data["2t"].isel(time=0)
        fig, ax = plot_single_map(data=data, return_fig=True, loglevel=LOGLEVEL)
        assert ax.get_title() == "2 metre temperature ERA5 era5-hpz3"
        assert fig.axes[1].get_xlabel() == "2 metre temperature [K]"
        assert ax.collections[0].filled
        assert ax.collections[0].levels[[0, -1]] == pytest.approx([float(data.min()), float(data.max())])

    def test_plot_single_map_diff(self, tmp_path, sst_maps):
        """Filled difference within the fill limits, symmetric contours of the data, gridlines and labels."""
        plot_data, plot_data2 = sst_maps
        fig, ax = plot_single_map_diff(
            data=plot_data,
            data_ref=plot_data2,
            nlevels=5,
            vmin_fill=-5.0,
            vmax_fill=5.0,
            sym=False,
            vmin_contour=-2.0,
            vmax_contour=30.0,
            sym_contour=True,
            cmap="viridis",
            display=False,
            return_fig=True,
            title="Test plot",
            cbar_label="Sea surface temperature [°C]",
            nxticks=5,
            nyticks=5,
            gridlines=True,
            loglevel=LOGLEVEL,
        )
        fill, contour = ax.collections[0], ax.collections[-1]
        assert ax.get_title() == "Test plot"
        assert fill.filled and fill.levels.tolist() == pytest.approx([-5, -3, -1, 1, 3, 5])
        assert not contour.filled and contour.levels.tolist() == pytest.approx([-30, -20, -10, 0, 10, 20, 30])
        assert len(ax.lines) > 0  # gridlines
        assert fig.axes[1].get_xlabel() == "Sea surface temperature [°C]"

        fig.savefig(os.path.join(tmp_path, "test_plot_single_map_diff.png"), dpi=DPI)

    def test_plot_single_map_diff_hpx(self, era5_hpz3_monthly_data):
        """By default the difference colorbar is symmetric around the largest absolute difference."""
        data, data_ref = era5_hpz3_monthly_data["2t"].isel(time=0), era5_hpz3_monthly_data["2t"].isel(time=1)
        fig, ax = plot_single_map_diff(data=data, data_ref=data_ref, return_fig=True, loglevel=LOGLEVEL)
        limit = _absmax(data - data_ref)
        assert ax.collections[0].get_clim() == pytest.approx((-limit, limit))
        assert not ax.collections[-1].filled
        assert fig.axes[1].get_xlabel() == "2 metre temperature [K]"

    def test_plot_single_map_no_diff(self, sst_maps):
        """Identical data: only the contours of the data are drawn, without fill and colorbar."""
        plot_data = sst_maps[0]
        fig, ax = plot_single_map_diff(data=plot_data, return_fig=True, data_ref=plot_data.copy(), loglevel=LOGLEVEL)
        assert len(fig.axes) == 1
        assert not ax.collections[0].filled

    def test_maps(self, tmp_path, sst_maps):
        """Panels with their titles, a suptitle and one colorbar shared by all panels."""
        plot_data, plot_data2 = sst_maps
        fig = plot_maps(
            maps=[plot_data, plot_data2],
            nlevels=5,
            vmin=-2,
            vmax=30,
            sym=False,
            cmap="viridis",
            title="Test plot",
            titles=["Test plot 1", "Test plot 2"],
            cbar_label="Sea surface temperature [°C]",
            nxticks=5,
            nyticks=6,
            return_fig=True,
            loglevel=LOGLEVEL,
        )
        *panels, cbar = fig.axes
        assert fig.get_suptitle() == "Test plot"
        assert [ax.get_title() for ax in panels] == ["Test plot 1", "Test plot 2"]
        assert [ax.collections[0].get_clim() for ax in panels] == [(-2, 30), (-2, 30)]
        assert cbar.get_xlabel() == "Sea surface temperature [°C]"

        fig.savefig(os.path.join(tmp_path, "test_plot_maps.png"), dpi=DPI)

        fig2 = plot_maps_diff(
            maps=[plot_data, plot_data],
            maps_ref=[plot_data2, plot_data2],
            nlevels=5,
            vmin_fill=-2,
            vmax_fill=2,
            vmin_contour=-2,
            vmax_contour=30,
            sym=False,
            sym_contour=True,
            cmap="viridis",
            title="Test plot",
            titles=["Test plot 1", "Test plot 2"],
            cbar_label="Sea surface temperature [°C]",
            nxticks=5,
            nyticks=6,
            return_fig=True,
            loglevel=LOGLEVEL,
        )
        *panels, cbar = fig2.axes
        assert fig2.get_suptitle() == "Test plot"
        assert [ax.get_title() for ax in panels] == ["Test plot 1", "Test plot 2"]
        assert [ax.collections[0].get_clim() for ax in panels] == [(-2, 2), (-2, 2)]
        assert all(not ax.collections[-1].filled for ax in panels)
        assert cbar.get_xlabel() == "Sea surface temperature [°C]"

    def test_maps_error(self):
        """Test plot_maps function with error"""
        with pytest.raises(ValueError, match="Maps should be a list of xarray.DataArray"):
            plot_maps(maps="test")

        with pytest.raises(ValueError, match="Maps and reference maps should be lists of xarray.DataArray"):
            plot_maps_diff(maps="test", maps_ref="test")


class TestVerticalProfiles:
    """Tests for the vertical profile functions"""

    @pytest.fixture(autouse=True)
    def setup(self, era5_hpz3_monthly_r100_reader, era5_hpz3_monthly_r100_data):
        q = era5_hpz3_monthly_r100_reader.regrid(era5_hpz3_monthly_r100_data["q"]).mean("lon")
        self.q0, self.q1 = q.isel(time=0), q.isel(time=1)

    def test_plot_vertical_profile(self, tmp_path):
        """Latitude-pressure section with the pressure axis inverted and the given colorbar limits and levels."""
        fig, ax = plot_vertical_profile(
            data=self.q0,
            var="q",
            vmin=-0.002,
            vmax=0.002,
            nlevels=8,
            title="Test vertical profile",
            return_fig=True,
            loglevel=LOGLEVEL,
        )
        assert ax.get_title() == "Test vertical profile"
        assert (ax.get_xlabel(), ax.get_ylabel()) == ("Latitude", "Pressure Level (Pa)")
        assert ax.yaxis_inverted()
        assert ax.collections[0].get_clim() == (-0.002, 0.002)
        assert len(ax.collections[0].levels) == 8
        assert fig.axes[1].get_ylabel() == "q [kg kg$^{-1}$]"
        # Without title_size the title follows the style title size, not the generic font.size
        assert ax.title.get_fontsize() == FontProperties(size=plt.rcParams["axes.titlesize"]).get_size_in_points()

        fig.savefig(os.path.join(tmp_path, "test_plot_vertical_profile.png"), dpi=DPI)

    def test_plot_vertical_profile_diff(self):
        """The difference is filled and the data are added as contour lines; title_size is applied."""
        fig, ax = plot_vertical_profile_diff(
            data=self.q0,
            data_ref=self.q1,
            var="q",
            vmin=-0.002,
            vmax=0.002,
            vmin_contour=-0.002,
            vmax_contour=0.002,
            add_contour=True,
            nlevels=8,
            title="Test vertical profile diff",
            title_size=16,
            return_fig=True,
            loglevel=LOGLEVEL,
        )
        assert ax.get_title() == "Test vertical profile diff"
        assert ax.title.get_fontsize() == 16
        assert [collection.filled for collection in ax.collections] == [True, False]
        assert ax.yaxis_inverted()


class TestTimeseries:
    """Tests for the timeseries functions"""

    @pytest.fixture(autouse=True)
    def setup(self, data_ifs_tc):
        """Monthly and yearly series at two grid points"""
        self.reader = Reader(model="IFS", exp="test-tco79", source="teleconnections", fix=True, loglevel=LOGLEVEL)
        self.t1 = data_ifs_tc["skt"].isel(lat=1, lon=1)
        self.t2 = data_ifs_tc["skt"].isel(lat=10, lon=10)
        self.t1_yearly = self.reader.timmean(self.t1, freq="YS", center_time=True)
        self.t2_yearly = self.reader.timmean(self.t2, freq="YS", center_time=True)

    def test_plot_timeseries(self, tmp_path):
        """Monthly, annual and reference lines with their values, two std bands, legend and labels."""
        fig, ax = plot_timeseries(
            monthly_data=[self.t1, self.t2],
            annual_data=[self.t1_yearly, self.t2_yearly],
            ref_monthly_data=self.t1,
            ref_annual_data=self.t1_yearly,
            std_monthly_data=self.t1.groupby("time.month").std("time"),
            std_annual_data=self.t1_yearly.std(dim="time"),
            ref_label="t1",
            title="Temperature at two locations",
            data_labels=["t1", "t2"],
        )
        assert ax.get_title() == "Temperature at two locations"
        assert ax.get_ylabel() == "Skin temperature [K]"
        assert len(ax.lines) == 6  # 2 monthly, 2 annual, reference monthly and annual
        assert ax.lines[0].get_ydata() == pytest.approx(self.t1.values)
        assert ax.lines[1].get_ydata() == pytest.approx(self.t2.values)
        assert len(ax.collections) == 2  # monthly and annual std bands
        assert _legend_texts(ax) == ["t1", "t2", "t1"]

        fig.savefig(os.path.join(tmp_path, "test_plot_timeseries.png"), dpi=DPI)

    def test_plot_seasonalcycle(self):
        """The cycle is extended to 14 points (Dec, Jan..Dec, Jan) and the std band surrounds the reference."""
        t1_seasonal = self.t1.groupby("time.month").mean("time")
        t2_seasonal = self.t2.groupby("time.month").mean("time")

        fig, ax = plot_seasonalcycle(
            data=t1_seasonal,
            ref_data=t2_seasonal,
            std_data=self.t1.groupby("time.month").std("time"),
            title="Seasonal cycle of temperature at two locations",
            data_labels=["t1"],
            ref_label="t2",
            loglevel=LOGLEVEL,
        )
        data_line, ref_line = ax.lines
        assert data_line.get_ydata()[1:13] == pytest.approx(t1_seasonal.values)
        assert data_line.get_ydata()[[0, 13]] == pytest.approx(t1_seasonal.values[[11, 0]])
        assert ref_line.get_ydata()[1:13] == pytest.approx(t2_seasonal.values)
        assert len(ax.collections) == 1
        assert ax.get_xlim() == (0.5, 12.5)
        assert _legend_texts(ax) == ["t1", "t2"]
        assert ax.get_title() == "Seasonal cycle of temperature at two locations"

    def test_plot_ensemble(self):
        """The ensemble mean is drawn with its label and a band for the monthly and the annual std."""
        ens_monthly = (self.t1 + self.t2) / 2
        ens_annual = self.reader.timmean(ens_monthly, freq="YS", center_time=True)

        fig, ax = plot_timeseries(
            monthly_data=[self.t1, self.t2],
            annual_data=[self.t1_yearly, self.t2_yearly],
            ens_monthly_data=ens_monthly,
            std_ens_monthly_data=abs(self.t1 - self.t2) / 2,
            ens_annual_data=ens_annual,
            std_ens_annual_data=ens_annual.std(dim="time"),
            ens_label="Ensemble mean",
            title="Ensemble mean temperature at two locations",
        )
        assert _legend_texts(ax) == ["Ensemble mean"]
        assert len(ax.collections) == 2
        ens_lines = [line.get_ydata() for line in ax.lines if len(line.get_ydata()) == ens_monthly.size]
        assert any(np.array_equal(ydata, ens_monthly.values) for ydata in ens_lines)


class TestHovmoller:
    """Tests for the Hovmoller functions"""

    @pytest.fixture(autouse=True)
    def setup(self, data_ifs_tc):
        self.data = data_ifs_tc["skt"]

    def test_plot_hovmoller(self, tmp_path):
        """invert_axis puts time on the y axis, invert_time reverses it; title, colorbar label and levels are applied."""
        fig, ax = plot_hovmoller(
            data=self.data,
            return_fig=True,
            cmap="RdBu_r",
            invert_axis=True,
            invert_time=True,
            cbar_label="test-label",
            nlevels=10,
            sym=True,
            title="Test hovmoller",
            loglevel=LOGLEVEL,
        )
        assert ax.get_title() == "Test hovmoller"
        assert (ax.get_xlabel(), ax.get_ylabel()) == ("lat (degrees north)", "time")
        assert ax.yaxis_inverted() and not ax.xaxis_inverted()
        assert len(ax.collections[0].levels) == 10
        assert ax.collections[0].get_clim() == pytest.approx((-_absmax(self.data), _absmax(self.data)))
        assert fig.axes[1].get_xlabel() == "test-label"
        assert [text.get_text() for text in ax.texts] == ["lon = 0.00 to 358.00"]

        fig.savefig(os.path.join(tmp_path, "test_hovmoller.png"), dpi=DPI)

    def test_plot_hovmoller_pcolormesh(self):
        """contour=False draws a mesh; with time on the x axis invert_time reverses the x axis."""
        fig, ax = plot_hovmoller(
            data=self.data, return_fig=True, contour=False, cmap="RdBu_r", invert_time=True, loglevel=LOGLEVEL
        )
        assert isinstance(ax.collections[0], QuadMesh)
        assert (ax.get_xlabel(), ax.get_ylabel()) == ("time", "lat (degrees north)")
        assert ax.xaxis_inverted() and not ax.yaxis_inverted()
        assert fig.axes[1].get_xlabel() == "Skin temperature [K]"

    def test_plot_hovmoller_error(self):
        with pytest.raises(TypeError, match="Data is not a DataArray"):
            plot_hovmoller(data="test")

    def test_plot_hovmoller_no_dim(self):
        """dim=None plots 2D data as they are: limits from the data, no averaging annotation"""
        data_2d = self.data.isel(lon=0)
        fig, ax = plot_hovmoller(data=data_2d, return_fig=True, dim=None, loglevel=LOGLEVEL)
        assert ax.collections[0].get_clim() == pytest.approx((float(data_2d.min()), float(data_2d.max())))
        assert len(ax.texts) == 0

    def test_plot_hovmoller_no_cbar(self):
        """Test plot_hovmoller with colorbar disabled"""
        fig, ax = plot_hovmoller(data=self.data, return_fig=True, cbar=False, cbar_label="explicit label", loglevel=LOGLEVEL)
        assert fig.axes == [ax]

    @pytest.mark.parametrize("sym", [True, False])
    def test_plot_hovmoller_vmin_vmax_sym(self, sym):
        """Explicit vmin/vmax are used, unless sym=True replaces them with the symmetric data limits"""
        fig, ax = plot_hovmoller(data=self.data, return_fig=True, vmin=-10.0, vmax=10.0, sym=sym, loglevel=LOGLEVEL)
        expected = (-_absmax(self.data), _absmax(self.data)) if sym else (-10.0, 10.0)
        assert ax.collections[0].get_clim() == pytest.approx(expected)


@pytest.fixture
def depth_section():
    """Monthly (time, depth) field within (0, 1), NaN below the sea floor at the deepest level"""
    time = np.arange("2000-01", "2001-01", dtype="datetime64[M]")
    depth = [5.0, 100.0, 1000.0, 3000.0, 6000.0]
    values = np.random.default_rng(0).uniform(size=(time.size, len(depth)))
    values[:, -1] = np.nan
    return xr.DataArray(values, dims=("time", "depth"), coords={"time": time, "depth": ("depth", depth, {"units": "m"})})


def test_plot_hovmoller_space_axis(depth_section):
    """The space axis is labelled with its units and leaves out the NaN-only band"""
    _, ax = plot_hovmoller(data=depth_section, dim=None, invert_space_coord=True, return_fig=True, loglevel=LOGLEVEL)
    assert ax.get_ylabel() == "depth (m)"
    assert ax.get_ylim() == (3000.0, 5.0)


@pytest.mark.parametrize("vmin, vmax, extend", [(0, 1, "neither"), (0.2, 1, "min"), (0, 0.8, "max"), (0.2, 0.8, "both")])
def test_plot_hovmoller_extend(depth_section, vmin, vmax, extend):
    """The colorbar is extended only on the sides where the data exceed vmin/vmax"""
    _, ax = plot_hovmoller(data=depth_section, dim=None, vmin=vmin, vmax=vmax, return_fig=True, loglevel=LOGLEVEL)
    assert ax.collections[-1].extend == extend


def test_plot_hovmoller_levels(depth_section):
    """Uneven levels replace vmin, vmax and nlevels"""
    levels = [0.1, 0.2, 0.5, 0.9]
    _, ax = plot_hovmoller(data=depth_section, dim=None, levels=levels, return_fig=True, loglevel=LOGLEVEL)
    assert list(ax.collections[-1].levels) == levels
    assert ax.collections[-1].extend == "both"


def test_plot_vertical_lines(tmp_path, era5_hpz3_monthly_data):
    """Data and reference profiles against pressure, inverted pressure axis, labels and legend."""
    data = era5_hpz3_monthly_data["q"].isel(time=0, cells=0)
    fig, ax = plot_vertical_lines(
        data=data,
        ref_data=data * 0.8,
        lev_name="plev",
        labels=["test"],
        ref_label="ref",
        title="Test vertical line",
        return_fig=True,
        invert_yaxis=True,
        loglevel=LOGLEVEL,
    )
    data_line, ref_line = ax.lines
    assert data_line.get_xdata() == pytest.approx(data.values)
    assert ref_line.get_xdata() == pytest.approx(0.8 * data.values)
    assert data_line.get_ydata() == pytest.approx(data.plev.values)
    assert ax.yaxis_inverted()
    assert (ax.get_xlabel(), ax.get_ylabel()) == ("Specific humidity (kg kg$^{-1}$)", "plev (Pa)")
    assert ax.get_title() == "Test vertical line"
    assert _legend_texts(ax) == ["test", "ref"]

    fig.savefig(os.path.join(tmp_path, "test_plot_vertical_lines.png"), dpi=DPI)


class TestLatLonProfiles:
    """Tests for the lat_lon_profiles function"""

    @pytest.fixture(autouse=True)
    def setup(self, data_ifs_tc):
        self.lat_profile = data_ifs_tc["skt"].mean(dim="lon")
        self.lon_profile = data_ifs_tc["skt"].mean(dim="lat")

    def test_plot_lat_lon_profiles_single(self, tmp_path):
        """A latitude profile: values, x label from the coordinate, units label, title and legend."""
        data = self.lat_profile.isel(time=0)
        fig, ax = plot_lat_lon_profiles(data=data, title="Latitude profile test", data_labels=["Test data"], loglevel=LOGLEVEL)
        assert ax.lines[0].get_ydata() == pytest.approx(data.values)
        assert (ax.get_xlabel(), ax.get_ylabel()) == ("Latitude", "Skin temperature [K]")
        assert ax.get_title() == "Latitude profile test"
        assert _legend_texts(ax) == ["Test data"]

        fig.savefig(os.path.join(tmp_path, "test_lat_profile.png"), dpi=DPI)

    def test_plot_lat_lon_profiles_multiple(self):
        """One line per DataArray with its label"""
        data_list = [self.lat_profile.isel(time=0), self.lat_profile.isel(time=1)]
        _, ax = plot_lat_lon_profiles(
            data=data_list, data_labels=["Time 0", "Time 1"], title="Multiple latitude profiles", loglevel=LOGLEVEL
        )
        assert [line.get_ydata().tolist() for line in ax.lines] == [data.values.tolist() for data in data_list]
        assert _legend_texts(ax) == ["Time 0", "Time 1"]

    def test_plot_lat_lon_profiles_with_ref(self):
        """The reference is drawn with its label and a std band"""
        _, ax = plot_lat_lon_profiles(
            data=self.lat_profile.isel(time=0),
            ref_data=self.lat_profile.isel(time=1),
            ref_std_data=self.lat_profile.std(dim="time"),
            data_labels=["Data"],
            ref_label="Reference",
            title="Profile with reference",
            loglevel=LOGLEVEL,
        )
        assert ax.lines[1].get_ydata() == pytest.approx(self.lat_profile.isel(time=1).values)
        assert len(ax.collections) == 1
        assert _legend_texts(ax) == ["Data", "Reference"]

    def test_plot_lon_profile(self):
        """A longitude profile is labelled with longitude: fails if the profile type is not detected"""
        data = self.lon_profile.isel(time=0)
        _, ax = plot_lat_lon_profiles(data=data, title="Longitude profile test", loglevel=LOGLEVEL)
        assert ax.get_xlabel() == "Longitude"
        assert ax.lines[0].get_xdata() == pytest.approx(data.lon.values)

    def test_plot_lat_lon_profiles_no_spatial_coords(self):
        """Test plot_lat_lon_profiles with DataArray without spatial coordinates"""
        data_no_coords = xr.DataArray(np.arange(10.0), dims=["time"], coords={"time": range(10)})
        _, ax = plot_lat_lon_profiles(data=data_no_coords, title="No spatial coordinates test", loglevel=LOGLEVEL)
        assert len(ax.lines) == 0


class TestSeasonalMeans:
    """Tests for the seasonal means functions"""

    @pytest.fixture(autouse=True)
    def setup(self, data_ifs_tc):
        data_seasonal = data_ifs_tc["skt"].mean(dim="lon")
        # Using simple time slicing for testing purposes
        self.seasons = [data_seasonal.isel(time=slice(3 * i, 3 * i + 3)).mean(dim="time") for i in range(4)]
        self.ref_data = [season * 0.95 for season in self.seasons]

    def test_plot_seasonal_lat_lon_profiles(self, tmp_path):
        """Four panels titled by season, each with its data, a suptitle and the legend on the first panel only"""
        fig, axs = plot_seasonal_lat_lon_profiles(
            seasonal_data=self.seasons, title="Seasonal Profiles Test", data_labels=["Test data"], loglevel=LOGLEVEL
        )
        assert [ax.get_title() for ax in axs] == ["DJF", "MAM", "JJA", "SON"]
        assert fig.get_suptitle() == "Seasonal Profiles Test"
        for ax, season in zip(axs, self.seasons):
            assert ax.lines[0].get_ydata() == pytest.approx(season.values)
        assert [_legend_texts(ax) for ax in axs] == [["Test data"], None, None, None]

        fig.savefig(os.path.join(tmp_path, "test_seasonal_profiles.png"), dpi=DPI)

    @pytest.mark.parametrize(
        "std_seasons, expected_bands", [([0, 1, 2, 3], [1, 1, 1, 1]), ([1, 3], [0, 1, 0, 1])], ids=["all", "some_none"]
    )
    def test_plot_seasonal_lat_lon_profiles_with_ref(self, std_seasons, expected_bands):
        """Data and reference in every panel, a std band only for the seasons with a reference std (None skips it)"""
        ref_std = [self.seasons[i].std() if i in std_seasons else None for i in range(4)]
        _, axs = plot_seasonal_lat_lon_profiles(
            seasonal_data=self.seasons,
            ref_data=self.ref_data,
            ref_std_data=ref_std,
            title="Seasonal Profiles with Reference",
            data_labels=["Data"],
            ref_label="Reference",
            loglevel=LOGLEVEL,
        )
        assert [len(ax.collections) for ax in axs] == expected_bands
        for ax, ref in zip(axs, self.ref_data):
            assert ax.lines[1].get_ydata() == pytest.approx(ref.values)
        assert _legend_texts(axs[0]) == ["Data", "Reference"]

    def test_plot_seasonal_lat_lon_profiles_multiple(self):
        """With a list per season every panel has one line per model"""
        seasonal_data = [[season, season * 1.05] for season in self.seasons]
        _, axs = plot_seasonal_lat_lon_profiles(
            seasonal_data=seasonal_data,
            title="Multiple Models Seasonal Profiles",
            data_labels=["Model 1", "Model 2"],
            loglevel=LOGLEVEL,
        )
        assert [len(ax.lines) for ax in axs] == [2, 2, 2, 2]
        assert axs[3].lines[1].get_ydata() == pytest.approx(1.05 * self.seasons[3].values)
        assert _legend_texts(axs[0]) == ["Model 1", "Model 2"]

    @pytest.mark.parametrize("seasonal_data", ["two_seasons", "dataarray"])
    def test_plot_seasonal_lat_lon_profiles_error(self, seasonal_data):
        """Test plot_seasonal_lat_lon_profiles with invalid input"""
        data = self.seasons[:2] if seasonal_data == "two_seasons" else self.seasons[0]
        with pytest.raises(ValueError, match="seasonal_data must be a list of 4 elements"):
            plot_seasonal_lat_lon_profiles(seasonal_data=data)


class TestHistogram:
    """Tests for the histogram plotting functions"""

    def setup_method(self):
        """Histogram decreasing as exp(-x / 2) over 20 bins in [0, 10]"""
        bins = np.linspace(0, 10, 20)
        self.hist_data = xr.DataArray(np.exp(-bins / 2), dims=["center_of_bin"], coords={"center_of_bin": bins})
        self.hist_data.center_of_bin.attrs["units"] = "m/s"
        self.hist_data.attrs["units"] = "count"

    def test_plot_histogram_basic(self, tmp_path):
        """Counts against the bin centers with automatic labels and the title"""
        fig, ax = plot_histogram(data=self.hist_data, title="Test Histogram", loglevel=LOGLEVEL)
        assert ax.lines[0].get_xdata() == pytest.approx(self.hist_data.center_of_bin.values)
        assert ax.lines[0].get_ydata() == pytest.approx(self.hist_data.values)
        assert ax.get_title() == "Test Histogram"

        fig.savefig(os.path.join(tmp_path, "test_histogram_basic.png"), dpi=DPI)

    def test_plot_histogram_multiple_with_ref(self):
        """Smoothing is a centred running mean, xlogscale sets a log x axis; one labelled line per dataset"""
        data_list = [self.hist_data, self.hist_data * 0.8]
        ref_data = self.hist_data * 1.2

        _, ax = plot_histogram(
            data=data_list,
            ref_data=ref_data,
            data_labels=["Data 1", "Data 2"],
            ref_label="Reference",
            smooth=True,
            smooth_window=3,
            xlogscale=True,
            loglevel=LOGLEVEL,
        )
        values = self.hist_data.values
        assert len(ax.lines) == 3  # 2 data + 1 ref
        assert ax.lines[0].get_ydata()[:2] == pytest.approx([values[:2].mean(), values[:3].mean()])
        assert ax.get_xscale() == "log"
        assert _legend_texts(ax) == ["Data 1", "Data 2", "Reference"]

    def test_plot_histogram_no_center_of_bin(self):
        """Test histogram with data missing center_of_bin dimension"""
        bad_data = xr.DataArray(np.arange(10.0), dims=["time"])
        _, ax = plot_histogram(data=bad_data, loglevel=LOGLEVEL)
        assert len(ax.lines) == 0  # No lines plotted

    def test_plot_histogram_auto_labels(self):
        """Labels from the attributes: counts with the bin units, a pdf with the inverse units"""
        _, ax = plot_histogram(data=self.hist_data, loglevel=LOGLEVEL)
        assert (ax.get_xlabel(), ax.get_ylabel()) == ("Value [m s$^{-1}$]", "Counts")

        pdf_data = self.hist_data.copy()
        pdf_data.attrs["units"] = "probability density"
        pdf_data.center_of_bin.attrs["long_name"] = "Wind Speed"
        _, ax = plot_histogram(data=pdf_data, loglevel=LOGLEVEL)
        assert (ax.get_xlabel(), ax.get_ylabel()) == ("Wind Speed [m s$^{-1}$]", "Probability Density [1/(m s$^{-1}$)]")

    def test_plot_histogram_custom_labels(self):
        """Test that custom labels override automatic ones"""
        _, ax = plot_histogram(data=self.hist_data, xlabel="Custom X", ylabel="Custom Y", loglevel=LOGLEVEL)
        assert (ax.get_xlabel(), ax.get_ylabel()) == ("Custom X", "Custom Y")

    def test_plot_histogram_labels_edge_cases(self):
        """Test edge cases for automatic label generation"""
        # var_name without units -> xlabel without units bracket
        data_no_units = self.hist_data.copy()
        data_no_units.center_of_bin.attrs = {"long_name": "Speed"}
        _, ax = plot_histogram(data=data_no_units, loglevel=LOGLEVEL)
        assert ax.get_xlabel() == "Speed"

        # PDF without center_of_bin units -> ylabel without inverse units
        pdf_no_units = self.hist_data.copy()
        pdf_no_units.attrs["units"] = "probability density"
        pdf_no_units.center_of_bin.attrs = {}
        _, ax = plot_histogram(data=pdf_no_units, loglevel=LOGLEVEL)
        assert ax.get_ylabel() == "Probability Density"


class TestBoxplot:
    """Basic tests for the boxplot function."""

    @staticmethod
    def _make_fldmean_dataset(tas_values, pr_values=None):
        data_vars = {
            "tas": xr.DataArray(np.array(tas_values), dims=["time"], attrs={"units": "K"}),
        }
        if pr_values is not None:
            data_vars["pr"] = xr.DataArray(np.array(pr_values), dims=["time"], attrs={"units": "mm/day"})
        return xr.Dataset(data_vars=data_vars)

    def test_boxplot_builds_figure_and_mean_lines(self, tmp_path):
        """Test figure creation and dashed mean lines."""
        fldmeans = [
            self._make_fldmean_dataset([280.0, 282.0, 281.0]),
            self._make_fldmean_dataset([279.0, 283.0, 280.0]),
        ]
        model_names = ["model_a", "model_b"]

        _, ax_no_mean = boxplot(
            fldmeans=fldmeans,
            model_names=model_names,
            variables=["tas"],
            variable_names=["Temperature"],
            add_mean_line=False,
            title="Boxplot test",
            loglevel=LOGLEVEL,
        )
        base_collections = len(ax_no_mean.collections)

        fig, ax = boxplot(
            fldmeans=fldmeans,
            model_names=model_names,
            variables=["tas"],
            variable_names=["Temperature"],
            add_mean_line=True,
            title="Boxplot test",
            loglevel=LOGLEVEL,
        )

        assert ax.get_xlabel() == "Variables"
        assert ax.get_title() == "Boxplot test"
        assert len(ax.collections) >= base_collections + len(model_names)
        assert _legend_texts(ax) == model_names

        fig.savefig(os.path.join(tmp_path, "test_boxplot_basic.png"), dpi=DPI)

    def test_boxplot_sets_mixed_units_ylabel(self):
        """Test fallback y-label for mixed units."""
        fldmeans = [
            self._make_fldmean_dataset([280.0, 282.0, 281.0], pr_values=[2.0, 3.0, 4.0]),
            self._make_fldmean_dataset([279.0, 283.0, 280.0], pr_values=[1.0, 2.0, 3.0]),
        ]

        _, ax = boxplot(
            fldmeans=fldmeans,
            model_names=["model_a", "model_b"],
            variables=["tas", "pr"],
            variable_names=["Temperature", "Precipitation"],
            add_mean_line=False,
            loglevel=LOGLEVEL,
        )

        assert ax.get_ylabel() == "Values (various units)"


@pytest.mark.parametrize("plot_func", [plot_gregory_monthly, plot_gregory_annual])
def test_gregory_time_colormap_is_applied_to_lines(plot_func):
    """Time colors should map consistently to trajectory segments and points."""
    times = np.array(["2000-01-01", "2001-01-01", "2002-01-01"], dtype="datetime64[D]")
    t2m = xr.DataArray([1.0, 2.0, 3.0], dims="time", coords={"time": times})
    net_toa = xr.DataArray([4.0, 2.0, 5.0], dims="time", coords={"time": times})

    fig, ax = plot_func(t2m, net_toa, cmap="viridis")

    assert len(ax.collections) == 2
    assert np.asarray(ax.collections[0].get_array()) == pytest.approx([0.25, 0.75])
    assert np.asarray(ax.collections[1].get_array()) == pytest.approx([0.0, 0.5, 1.0])
    assert len(fig.axes) == 1


@pytest.mark.parametrize("cmap", [None, "viridis"])
def test_plot_gregory_monthly_accepts_lists_of_model_data(cmap):
    """Each item in the monthly data lists should plot as a separate dataset."""
    times = np.array(["2000-01-01", "2001-01-01", "2002-01-01"], dtype="datetime64[D]")
    t2m = xr.DataArray([1.0, 2.0, 3.0], dims="time", coords={"time": times})
    net_toa = xr.DataArray([4.0, 2.0, 5.0], dims="time", coords={"time": times})

    _, ax = plot_gregory_monthly(
        [t2m, t2m + 1],
        [net_toa, net_toa + 1],
        labels=["model-1", "model-2"],
        cmap=cmap,
    )

    assert ax.get_legend_handles_labels()[1] == ["model-1", "model-2"]
    # with cmap: segments and points of each model; without: one line per model plus the zero line
    assert (len(ax.collections), len(ax.lines)) == ((4, 1) if cmap else (0, 3))


@pytest.mark.parametrize("cmap", [None, "viridis"])
def test_plot_gregory_annual_endpoint_triangles_follow_cmap(cmap):
    """Endpoint triangles are only needed when time coloring is disabled."""
    times = np.array(["2000-01-01", "2001-01-01", "2002-01-01"], dtype="datetime64[D]")
    t2m = xr.DataArray([1.0, 2.0, 3.0], dims="time", coords={"time": times})
    net_toa = xr.DataArray([4.0, 2.0, 5.0], dims="time", coords={"time": times})

    _, ax = plot_gregory_annual(t2m, net_toa, cmap=cmap)

    markers = [line.get_marker() for line in ax.lines]
    assert (">" in markers) is (cmap is None)
    assert ("<" in markers) is (cmap is None)


@pytest.mark.parametrize("plot_func", [plot_gregory_monthly, plot_gregory_annual])
def test_gregory_axis_limits_expand_to_the_data(plot_func):
    """The fixed axis ranges are expanded (with a 0.5 margin) when the data go beyond them.

    Fails on main for plot_gregory_monthly without reference data: gregory.py:139-140 and :148-149 replace
    the data limits with the fixed 11.5/16.5 and -11.5/11.5, so data outside them fall outside the axes
    (documented in #3374).
    """
    times = np.array(["2000-01-01", "2000-02-01", "2000-03-01"], dtype="datetime64[D]")
    t2m = xr.DataArray([5.0, 12.0, 20.0], dims="time", coords={"time": times})
    net_toa = xr.DataArray([-15.0, 0.0, 14.0], dims="time", coords={"time": times})

    _, ax = plot_func(t2m, net_toa, loglevel=LOGLEVEL)

    assert ax.get_xlim() == (4.5, 20.5)
    assert ax.get_ylim() == (-15.5, 14.5)


class TestGregory:
    """Tests for the Gregory plot functions on real data"""

    @pytest.fixture(autouse=True)
    def setup(self, data_ifs_tc):
        # Spatial mean in Celsius as temperature, a linear function of it as proxy for the TOA radiation
        self.t2m = data_ifs_tc["skt"].mean(dim=["lat", "lon"]) - 273.15
        self.net_toa = (self.t2m - 5.0) * 2.0
        self.t2m_annual = self.t2m.resample(time="YS").mean()
        self.net_toa_annual = self.net_toa.resample(time="YS").mean()

    def test_plot_gregory_monthly(self, tmp_path):
        """The model trajectory with the default axis labels and the zero line"""
        fig, ax = plot_gregory_monthly(
            t2m_monthly_data=self.t2m,
            net_toa_monthly_data=self.net_toa,
            title="Monthly Gregory Plot",
            loglevel=LOGLEVEL,
        )
        model_line = ax.lines[1]
        assert model_line.get_xdata() == pytest.approx(self.t2m.values)
        assert model_line.get_ydata() == pytest.approx(self.net_toa.values)
        assert (ax.get_xlabel(), ax.get_ylabel()) == ("2 m Temperature [°C]", r"Net radiation TOA [W m$^{-2}$]")
        assert ax.get_title() == "Monthly Gregory Plot"

        fig.savefig(os.path.join(tmp_path, "test_gregory_monthly.png"), dpi=DPI)

    def test_plot_gregory_monthly_with_ref(self):
        """The reference is the closed mean annual cycle (13 points), annotated with the month numbers"""
        t2m_ref, net_toa_ref = self.t2m * 0.98, self.net_toa * 1.02
        _, ax = plot_gregory_monthly(
            t2m_monthly_data=self.t2m,
            net_toa_monthly_data=self.net_toa,
            t2m_monthly_ref=t2m_ref,
            net_toa_monthly_ref=net_toa_ref,
            labels=["Model"],
            ref_label="Reference",
            title="Monthly Gregory Plot with Reference",
            loglevel=LOGLEVEL,
        )
        ref_line = ax.lines[-1]
        cycle = t2m_ref.groupby("time.month").mean("time").values
        assert ref_line.get_label() == "Reference"
        assert ref_line.get_xdata() == pytest.approx(np.append(cycle, cycle[0]))
        assert [text.get_text() for text in ax.texts] == [str(month) for month in range(1, 13)]

    def test_plot_gregory_annual(self):
        """The annual trajectory with first and last year marked and annotated"""
        _, ax = plot_gregory_annual(
            t2m_annual_data=self.t2m_annual,
            net_toa_annual_data=self.net_toa_annual,
            title="Annual Gregory Plot",
            loglevel=LOGLEVEL,
        )
        assert ax.lines[1].get_xdata() == pytest.approx(self.t2m_annual.values)
        assert [line.get_marker() for line in ax.lines[2:]] == [">", "<"]
        years = self.t2m_annual.time.dt.year.values
        assert [text.get_text() for text in ax.texts] == [str(years[0]), str(years[-1])]
        assert ax.get_title() == "Annual Gregory Plot"

    def test_plot_gregory_annual_with_ref(self):
        """With reference std a one-sigma band is drawn around the reference mean on both axes"""
        t2m_std = self.t2m_annual.std()
        _, ax = plot_gregory_annual(
            t2m_annual_data=self.t2m_annual,
            net_toa_annual_data=self.net_toa_annual,
            t2m_annual_ref=self.t2m_annual * 0.99,
            net_toa_annual_ref=self.net_toa_annual * 1.01,
            t2m_std=t2m_std,
            net_toa_std=self.net_toa_annual.std(),
            labels=["Model"],
            title="Annual Gregory Plot with Reference",
            loglevel=LOGLEVEL,
        )
        hband, vband = ax.patches
        assert hband.get_label() == r"1 $\sigma$ band"
        ref_mean = float((self.t2m_annual * 0.99).mean())
        assert vband.get_x() == pytest.approx(ref_mean - float(t2m_std))
        assert vband.get_width() == pytest.approx(2 * float(t2m_std))


class TestIndexPlot:
    """Tests for the index plot functions"""

    @pytest.fixture(autouse=True)
    def setup(self, data_ifs_tc):
        # Anomaly of the spatial mean as index
        index = data_ifs_tc["skt"].mean(dim=["lat", "lon"])
        self.index = index - index.mean()

    def test_index_plot(self, tmp_path):
        """The index line with its label, the positive and negative fills, labels and title"""
        fig, ax = index_plot(
            index=self.index,
            thresh=0.5,
            title="Index Plot Test",
            ylabel="Temperature Index",
            label="SKT Index",
            loglevel=LOGLEVEL,
        )
        assert ax.lines[0].get_ydata() == pytest.approx(self.index.values)
        assert len(ax.collections) == 2  # above thresh and below -thresh
        assert (ax.get_title(), ax.get_ylabel()) == ("Index Plot Test", "Temperature Index")
        assert _legend_texts(ax) == ["SKT Index"]

        fig.savefig(os.path.join(tmp_path, "test_index_plot.png"), dpi=DPI)

    def test_index_plot_with_ylim(self):
        """Test index_plot with custom ylim"""
        _, ax = index_plot(index=self.index, ylim=(-2, 2), title="Index Plot with ylim", loglevel=LOGLEVEL)
        assert ax.get_ylim() == (-2, 2)
        assert ax.get_ylabel() == "Index"

    def test_indexes_plot(self):
        """One panel per index with its title and label, a suptitle and a y range shared by the panels"""
        fig, axs = indexes_plot(
            indexes=[self.index, self.index * 0.5],
            thresh=0.3,
            titles=["Index 1", "Index 2"],
            labels=["SKT", "SKT scaled"],
            suptitle="Multiple Indexes",
            ylabel="Index Value",
            loglevel=LOGLEVEL,
        )
        assert fig.get_suptitle() == "Multiple Indexes"
        assert [ax.get_title() for ax in axs] == ["Index 1", "Index 2"]
        assert [_legend_texts(ax) for ax in axs] == [["SKT"], ["SKT scaled"]]
        assert axs[1].lines[0].get_ydata() == pytest.approx(0.5 * self.index.values)
        assert axs[0].get_ylim() == axs[1].get_ylim()
