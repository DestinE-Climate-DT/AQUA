import pytest
import os, glob
import numpy as np
import xarray as xr
from aqua.diagnostics import SeaIce, PlotSeaIce
from aqua.diagnostics.core import OutputSaver
from conftest import APPROX_REL, DPI, LOGLEVEL

approx_rel = APPROX_REL
loglevel = LOGLEVEL

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
# These tests use setup_class with shared resources (data fetching, tmp files)
pytestmark = [
    pytest.mark.diagnostics,
    pytest.mark.xdist_group(name="diagnostic_setup_class")
]

class TestPlotSeaIce:
    @classmethod
    def setup_class(cls):
        cls.tmp_path = "./"
        cls.catalog = 'ci'
        cls.model = 'FESOM'
        cls.exp = 'hpz3'
        cls.source = 'monthly-2d'
        cls.regions = ['arctic', 'antarctic']
        cls.loglevel = 'warning'
        cls.startdate = "1991-01-01"
        cls.enddate = "2000-01-01"
        cls.regrid = "r100"
        
        # Initialize sea ice objects
        cls.seaice = SeaIce(catalog=cls.catalog, model=cls.model, exp=cls.exp, source=cls.source,
                           startdate=cls.startdate, enddate=cls.enddate, regions=cls.regions, 
                           regrid=cls.regrid, loglevel=cls.loglevel)
        
        # Compute test data
        cls.siext = cls.seaice.compute_seaice(method='extent', var='siconc')
        cls.siext_seas = cls.seaice.compute_seaice(method='extent', var='siconc', get_seasonal_cycle=True)
        
        # Create reference data with bias for testing
        cls.siext_ref = cls.siext.copy()
        cls.siext_seas_ref = cls.siext_seas.copy()
        
        # Add constant bias to create difference for plotting
        bias_constant = 0.1
        
        # For extent data - add bias to each data variable in the dataset
        for var_name in cls.siext_ref.data_vars:
            cls.siext_ref[var_name] = cls.siext_ref[var_name] + bias_constant
        
        for var_name in cls.siext_seas_ref.data_vars:
            cls.siext_seas_ref[var_name] = cls.siext_seas_ref[var_name] + bias_constant

    def test_check_as_datasets_list_valid_input(self):
        """Validate the _check_as_datasets_list utility with valid input."""
        plot_seaice = PlotSeaIce(dpi=DPI)
        result = plot_seaice._check_as_datasets_list(self.siext)
        assert isinstance(result, list)
        assert result and isinstance(result[0], xr.Dataset)

    def test_check_as_datasets_list_invalid_input(self):
        """Validate the _check_as_datasets_list utility rejects invalid input."""
        plot_seaice = PlotSeaIce(dpi=DPI)
        
        with pytest.raises(ValueError):
            plot_seaice._check_as_datasets_list('string')
        
        with pytest.raises(ValueError):
            plot_seaice._check_as_datasets_list(123)
        
        with pytest.raises(ValueError):
            plot_seaice._check_as_datasets_list([1, 2, 3])

    def test_repack_datasetlists_all_regions(self):
        """Ensure repacking preserves structure for all regions."""
        psi = PlotSeaIce(monthly_models=self.siext, regions_to_plot=None, dpi=DPI)
        repacked = psi.repacked_dict

        assert 'extent' in repacked     # method key
        method_block = repacked['extent']
        expected_regions = ['Arctic', 'Antarctic']
        assert sorted(method_block.keys()) == sorted(expected_regions)

        for reg in expected_regions:
            assert method_block[reg]['monthly_models']    # non-empty list

    def test_repack_datasetlists_filtered_regions(self):
        """Ensure repacking obeys region filtering."""
        psi = PlotSeaIce(monthly_models=self.siext, regions_to_plot=['Arctic'], dpi=DPI)
        repacked = psi.repacked_dict

        assert 'extent' in repacked     # method key
        method_block = repacked['extent']
        expected_regions = ['Arctic']
        assert sorted(method_block.keys()) == sorted(expected_regions)

        for reg in expected_regions:
            assert method_block[reg]['monthly_models']    # non-empty list

    def test_invalid_regions_type_raises(self):
        """regions_to_plot must be list or None."""
        with pytest.raises(TypeError):
            PlotSeaIce(monthly_models=self.siext, regions_to_plot='arctic')
        
        with pytest.raises(TypeError):
            PlotSeaIce(monthly_models=self.siext, regions_to_plot=123)

    def test_plot_timeseries_nosave_fig(self):
        """Test the timeseries path with no files saved."""
        psi = PlotSeaIce(
            monthly_models=self.siext,
            monthly_ref=self.siext_ref,
            regions_to_plot=['Arctic', 'Antarctic'],
            model=self.model, exp=self.exp, source=self.source,
            catalog=self.catalog, loglevel=self.loglevel, dpi=DPI
        )
        psi.plot_seaice(plot_type='timeseries', save_pdf=False, save_png=False)

    def test_plot_seaice_seasonal_cycle(self):
        """Test the seasonal cycle path with no files saved."""
        psi = PlotSeaIce(
            regions_to_plot=['Arctic', 'Antarctic'],
            model=self.model, exp=self.exp, source=self.source,
            catalog=self.catalog, loglevel=self.loglevel, dpi=DPI
        )
        psi.plot_seaice(plot_type="seasonalcycle", save_pdf=False, save_png=False)

    def test_plot_seascycle_multi(self):
        """Test the seasonal cycle path with multiple datasets."""
        psi = PlotSeaIce(monthly_models=self.siext_seas,
                         monthly_ref=[self.siext_seas_ref], dpi=DPI)
        psi.plot_seaice(plot_type="seasonalcycle", save_pdf=False, save_png=False)

    def test_invalid_plot_type_raises(self):
        """Test that invalid plot type raises ValueError."""
        psi = PlotSeaIce(monthly_models=self.siext, dpi=DPI)
        with pytest.raises(ValueError):
            psi.plot_seaice(plot_type='bad_type', save_pdf=False, save_png=False)

    def test_get_region_name_from_attrs(self):
        """Test region name extraction from DataArray attributes."""
        psi = PlotSeaIce(dpi=DPI)
        da = xr.DataArray([0, 1, 2], dims="time", attrs={"AQUA_region": "CustomReg"}, name="dummy")
        assert psi._get_region_name_in_datarray(da) == "CustomReg"

    def test_get_region_name_from_name(self):
        """Test region name extraction from DataArray name."""
        psi = PlotSeaIce(dpi=DPI)
        da = xr.DataArray([5, 6], dims="time", name="siext_Antarctic")
        assert psi._get_region_name_in_datarray(da) == "Antarctic"

    def test_get_region_name_missing_raises(self):
        """Test that missing region information raises KeyError."""
        da = xr.DataArray([7, 8], dims="time")  # no attrs, no region in name
        psi = PlotSeaIce(dpi=DPI)
        with pytest.raises(KeyError):
            psi._get_region_name_in_datarray(da)

    def _dummy_da(self, label):
        """Helper: make a one-point DataArray with AQUA_* attrs."""
        return xr.DataArray(
            [1],
            dims="time",
            attrs={"AQUA_model": label, "AQUA_exp": "e", "AQUA_source": "s"},
            name=f"var_{label}",
        )

    def test_gen_labelname_single(self):
        """Test label generation for single DataArray."""
        psi = PlotSeaIce(dpi=DPI)
        da = self._dummy_da("m1")
        assert psi._gen_labelname(da) == "m1 e s"

    def test_gen_labelname_list(self):
        """Test label generation for list of DataArrays."""
        psi = PlotSeaIce(dpi=DPI)
        da_list = [self._dummy_da("m1"), self._dummy_da("m2")]
        labels = psi._gen_labelname(da_list)
        assert labels == ["m1 e s", "m2 e s"]

    def test_getdata_fromdict_single(self):
        """Test data extraction from dict with single item."""
        psi = PlotSeaIce(dpi=DPI)
        dct = {"key": [xr.DataArray([0], dims="t")]}
        out = psi._getdata_fromdict(dct, "key")
        assert isinstance(out, xr.DataArray)

    def test_getdata_fromdict_multiple(self):
        """Test data extraction from dict with multiple items."""
        psi = PlotSeaIce(dpi=DPI)
        dct = {"key": [xr.DataArray([0], dims="t"), xr.DataArray([1], dims="t")]}
        out = psi._getdata_fromdict(dct, "key")
        assert isinstance(out, list)

    def test_getdata_fromdict_missing(self):
        """Test data extraction from dict with missing key."""
        psi = PlotSeaIce(dpi=DPI)
        dct = {}
        out = psi._getdata_fromdict(dct, "key")
        assert out is None

    def test_save_fig_calls_output_saver(self, monkeypatch):
        """Test that save_fig calls OutputSaver methods."""
        png_called = {"flag": False}
        pdf_called = {"flag": False}

        # fake save_png/pdf that just flips flags 
        def fake_png(self, *args, **kwargs):
            png_called["flag"] = True

        def fake_pdf(self, *args, **kwargs):
            pdf_called["flag"] = True

        monkeypatch.setattr(OutputSaver, "save_png", fake_png, raising=True)
        monkeypatch.setattr(OutputSaver, "save_pdf", fake_pdf, raising=True)

        psi = PlotSeaIce(monthly_models=self.siext,
                         regions_to_plot=["Arctic"], 
                         model=self.model, exp=self.exp, source=self.source, 
                         catalog=self.catalog, loglevel=self.loglevel, dpi=DPI)

        # Both branches inside save_fig should run 
        psi.plot_seaice(plot_type="timeseries", save_pdf=True, save_png=True)

        assert png_called["flag"] is True
        assert pdf_called["flag"] is True

    def test_plot_saves_outputs(self):
        """Test that plotting saves output files."""
        psi = PlotSeaIce(monthly_models=self.siext,
                         monthly_ref=self.siext_ref,
                         regions_to_plot=['Arctic', 'Antarctic'],
                         model=self.model, exp=self.exp, source=self.source,
                         catalog=self.catalog, loglevel=self.loglevel, dpi=DPI,
                         outputdir=self.tmp_path)

        psi.plot_seaice(plot_type='timeseries', save_pdf=True, save_png=True)

        png_files = glob.glob(os.path.join(self.tmp_path, "**/*.png"), recursive=True)
        pdf_files = glob.glob(os.path.join(self.tmp_path, "**/*.pdf"), recursive=True)
        
        assert len(png_files) > 0, "No PNG file saved."
        assert len(pdf_files) > 0, "No PDF file saved."