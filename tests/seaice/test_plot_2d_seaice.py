import pytest
import os, glob
import numpy as np
import xarray as xr
from aqua.diagnostics import SeaIce, Plot2DSeaIce
from conftest import APPROX_REL, DPI, LOGLEVEL

approx_rel = APPROX_REL
loglevel = LOGLEVEL

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
# These tests use setup_class with shared resources (data fetching, tmp files)
pytestmark = [
    pytest.mark.diagnostics,
    pytest.mark.xdist_group(name="diagnostic_setup_class")
]

class TestPlot2DSeaIce:
    @classmethod
    def setup_class(cls):
        cls.tmp_path = "./"
        cls.catalog = "ci"
        cls.model = "FESOM"
        cls.exp = "hpz3"
        cls.source = "monthly-2d"
        cls.regions = ["arctic", "antarctic"]
        cls.loglevel = "warning"
        cls.startdate = "1991-01-01"
        cls.enddate = "2000-01-01"
        cls.regrid = "r100"
        
        # Initialize sea ice objects
        cls.seaice = SeaIce(catalog=cls.catalog, model=cls.model, exp=cls.exp, source=cls.source,
                           startdate=cls.startdate, enddate=cls.enddate, regions=cls.regions, 
                           regrid=cls.regrid, loglevel=cls.loglevel)
        
        cls.frac_model = cls.seaice.compute_seaice(method="fraction", var="siconc")
        cls.thick_model_ds = cls.seaice.compute_seaice(method="thickness", var="siconc")
        
        # Use FESOM data as reference but add a small constant to avoid skipping the bias plot
        # this should be changed in the future with proper data (e.g. ORAS5)
        cls.frac_ref_arctic = cls.frac_model.copy()
        cls.frac_ref_antarctic = cls.frac_model.copy()
        
        # Add constant bias to create difference for plotting
        bias_constant_fraction = 0.1
        bias_constant_thickness = 0.5
        
        # For fraction data - add bias to each data variable in the dataset
        for var_name in cls.frac_ref_arctic.data_vars:
            cls.frac_ref_arctic[var_name] = cls.frac_ref_arctic[var_name] + bias_constant_fraction
        for var_name in cls.frac_ref_antarctic.data_vars:
            cls.frac_ref_antarctic[var_name] = cls.frac_ref_antarctic[var_name] + bias_constant_fraction
        
        # For thickness data - add bias to each data variable in the dataset
        cls.thick_ref_arctic = cls.thick_model_ds.copy()
        cls.thick_ref_antarctic = cls.thick_model_ds.copy()
        
        for var_name in cls.thick_ref_arctic.data_vars:
            cls.thick_ref_arctic[var_name] = cls.thick_ref_arctic[var_name] + bias_constant_thickness
        for var_name in cls.thick_ref_antarctic.data_vars:
            cls.thick_ref_antarctic[var_name] = cls.thick_ref_antarctic[var_name] + bias_constant_thickness
        
        cls.p2d = Plot2DSeaIce(loglevel=loglevel, dpi=DPI)
        
        cls.projkw = {"projname": "orthographic",
                     "projpars": {"central_longitude": 0.0, "central_latitude": "max_lat_signed"}}
        
        cls.projkw_extent = {'projpars': {'central_longitude': 0.0, 'central_latitude': 'max_lat_signed'},
                            'extent_regions': {'Arctic': [-180, 180, 50, 90], 'Antarctic': [-180, 180, -50, -90]},
                            'projname': 'azimuthal_equidistant'}

    def test_handle_data_rejects_invalid_types(self):
        with pytest.raises(TypeError):
            self.p2d._handle_data("string")
        
        with pytest.raises(TypeError):
            self.p2d._handle_data(123)

    def test_handle_data_accepts_dataset_or_dataarray(self):
        out = self.p2d._handle_data(self.frac_model)
        assert isinstance(out, list)
        assert out and isinstance(out[0], xr.DataArray)

    def test_mask_ice_at_mid_lats_fraction(self):
        """Test masking helper for fraction method."""
        lat = xr.DataArray(np.linspace(-90, 90, 181), dims="lat")
        lon = xr.DataArray(np.linspace(0, 359, 360), dims="lon")
        data = xr.DataArray(
            np.random.rand(181, 360),
            coords={"lat": lat, "lon": lon},
            dims=("lat", "lon"),
            attrs={"AQUA_method": "fraction", "AQUA_region": "arctic"},
        )
        self.p2d.method = "fraction"
        masked = self.p2d._mask_ice_at_mid_lats(data)
        
        # For fraction, mid-latitudes are overwritten with zeros (no NaNs)
        mids = masked.where((lat >= -45) & (lat <= 40), drop=True)
        assert not mids.isnull().any()

    def test_mask_ice_at_mid_lats_thickness(self):
        """Test masking helper for thickness method."""
        lat = xr.DataArray(np.linspace(-90, 90, 181), dims="lat")
        lon = xr.DataArray(np.linspace(0, 359, 360), dims="lon")
        data = xr.DataArray(
            np.random.rand(181, 360),
            coords={"lat": lat, "lon": lon},
            dims=("lat", "lon"),
            attrs={"AQUA_method": "thickness", "AQUA_region": "arctic"},
        )
        self.p2d.method = "thickness"
        masked = self.p2d._mask_ice_at_mid_lats(data)
        
        # Mid-latitudes and tiny thickness values should be turned to NaN
        assert masked.sel(lat=0.0, method="nearest").isnull().all()

    def test_plot_fraction_var(self):
        p2d = Plot2DSeaIce(ref=[self.frac_ref_antarctic, self.frac_ref_arctic],
                           models=self.frac_model, dpi=DPI)
        
        p2d.plot_2d_seaice(plot_type="var", projkw=self.projkw,
                          plot_ref_contour=True, save_pdf=False, save_png=False)

    def test_plot_fraction_bias(self):
        p2d = Plot2DSeaIce(ref=[self.frac_ref_antarctic, self.frac_ref_arctic],
                           models=self.frac_model, dpi=DPI)
        
        p2d.plot_2d_seaice(plot_type="bias", projkw=self.projkw_extent,
                          plot_ref_contour=True, save_pdf=False, save_png=False)

    def test_plot_thickness_var(self):
        p2d = Plot2DSeaIce(ref=[self.thick_ref_antarctic, self.thick_ref_arctic],
                           models=self.thick_model_ds, dpi=DPI)
        
        p2d.plot_2d_seaice(plot_type="var", projkw=self.projkw,
                          plot_ref_contour=True, save_pdf=False, save_png=False)

    def test_plot_thickness_bias(self):
        p2d = Plot2DSeaIce(ref=[self.thick_ref_antarctic, self.thick_ref_arctic],
                           models=self.thick_model_ds, dpi=DPI)
        
        p2d.plot_2d_seaice(plot_type="bias", projkw=self.projkw_extent,
                          plot_ref_contour=True, save_pdf=False, save_png=False)

    def test_bad_months_raise_value_error(self):
        p2d = Plot2DSeaIce(models=self.frac_model, dpi=DPI)
        with pytest.raises(ValueError):
            p2d.plot_2d_seaice(months=[0], projkw=self.projkw,
                              save_pdf=False, save_png=False)

    def test_bad_months_raise_type_error(self):
        p2d = Plot2DSeaIce(models=self.frac_model, dpi=DPI)
        with pytest.raises(TypeError):
            p2d.plot_2d_seaice(months=["Feb"], projkw=self.projkw,
                              save_pdf=False, save_png=False)

    def test_detect_common_regions_auto_detection(self):
        """Test automatic region detection without specifying regions_to_plot."""
        p2d = Plot2DSeaIce(ref=[self.frac_ref_antarctic, self.frac_ref_arctic],
                           models=self.frac_model,
                           regions_to_plot=None,  # This triggers _detect_common_regions
                           loglevel=loglevel, dpi=DPI)
        
        assert p2d.regions_to_plot is not None
        assert len(p2d.regions_to_plot) > 0

        # Should detect both arctic and antarctic regions
        assert "arctic" in p2d.regions_to_plot or "Arctic" in p2d.regions_to_plot
        assert "antarctic" in p2d.regions_to_plot or "Antarctic" in p2d.regions_to_plot

    def test_get_cmap_fraction(self):
        """Test colormap generation for fraction method."""
        p2d = Plot2DSeaIce(models=self.frac_model, loglevel=loglevel, dpi=DPI)
        p2d.method = "fraction"
        
        # Get a sample data array
        sample_data = self.frac_model[0] if isinstance(self.frac_model, list) else self.frac_model        
        cmap_dict = p2d._get_cmap(sample_data)
        
        assert 'colormap' in cmap_dict
        assert cmap_dict['norm'] is None  # fraction uses default normalization
        assert cmap_dict['boundaries'] is None

    def test_get_cmap_thickness(self):
        """Test colormap generation for thickness method."""
        p2d = Plot2DSeaIce(models=self.frac_model, loglevel=loglevel, dpi=DPI)
        p2d.method = "thickness"
        
        # Get a sample data array
        sample_data = self.frac_model[0] if isinstance(self.frac_model, list) else self.frac_model        
        cmap_dict = p2d._get_cmap(sample_data)
        
        assert 'colormap' in cmap_dict
        assert cmap_dict['norm'] is not None  # thickness uses BoundaryNorm
        assert cmap_dict['boundaries'] is not None
        assert len(cmap_dict['boundaries']) > 0

    def test_set_projpars_function_registry(self):
        """Test projection parameter processing with function registry."""
        p2d = Plot2DSeaIce(models=self.frac_model, loglevel=loglevel)
        
        # Set up test data with known lat values
        test_data = self.frac_model[0] if isinstance(self.frac_model, list) else self.frac_model
        
        p2d.projpars = {'central_longitude': 0.0,
                        'central_latitude': 'max_lat_signed'}
        
        # Mock the data to have known lat values
        p2d.reg_ref = [test_data]
        
        result = p2d._set_projpars()

        assert 'central_latitude' in result
        assert isinstance(result['central_latitude'], (int, float))
        
        # Test with invalid function name
        p2d.projpars = {'central_latitude': 'invalid_function_name'}
        
        result = p2d._set_projpars()
        assert 'central_latitude' not in result  # Invalid function should be skipped

    def test_bad_plot_type_raises(self):
        p2d = Plot2DSeaIce(models=self.frac_model, dpi=DPI)
        with pytest.raises(ValueError):
            p2d.plot_2d_seaice(plot_type="invalid_plot_type", projkw=self.projkw,
                               save_pdf=False, save_png=False)

    def test_bad_method_raises(self):
        p2d = Plot2DSeaIce(models=self.frac_model)
        with pytest.raises(ValueError):
            p2d.plot_2d_seaice(method="invalid_method", projkw=self.projkw,
                               save_pdf=False, save_png=False)

    def test_plot_saves_outputs(self):
        p2d = Plot2DSeaIce(ref=[self.frac_ref_antarctic, self.frac_ref_arctic],
                           models=self.frac_model, 
                           outputdir=self.tmp_path, loglevel="INFO", dpi=DPI)

        p2d.plot_2d_seaice(plot_type="var", projkw=self.projkw, 
                           save_pdf=True, save_png=True, months=[3])

        png_files = glob.glob(os.path.join(self.tmp_path, "**/*.png"), recursive=True)
        pdf_files = glob.glob(os.path.join(self.tmp_path, "**/*.pdf"), recursive=True)
        
        assert len(png_files) > 0, "No PNG file saved."
        assert len(pdf_files) > 0, "No PDF file saved."