import pytest 
import xarray as xr
from aqua.diagnostics import SeaIce, PlotSeaIce

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'

# Set up test parameters
catalog='ci'
model='FESOM'
exp='hpz3'
source='monthly-2d'
regions = ['Arctic', 'Antarctic']
loglevel = 'warning'
startdate = "1991-01-01"
enddate = "2000-01-01"

# Generate test datasets for models
seaice = SeaIce(model=model, exp=exp, source=source, regions=regions, catalog=catalog,
                regrid='r100',
                startdate=startdate, enddate=enddate, loglevel=loglevel)
siext = seaice.compute_seaice(method='extent', var='siconc')

def create_test_dataset_for_repack_datasetlists(method="extent", region="Arctic"):
    """Helper function to create a test dataset with required attributes."""
    data = xr.DataArray([1, 2, 3], dims="time", coords={"time": [1, 2, 3]}, name=f"sea_ice_{method}_{region.lower()}")
    data.attrs["AQUA_method"] = method
    data.attrs["AQUA_region"] = region
    return xr.Dataset({data.name: data}, attrs={"AQUA_method": method})


@pytest.mark.diagnostics
class Test_PlotSeaIce:
    """Test the Plot_SeaIce class."""
    
    @pytest.mark.parametrize(
        ('regions_to_plot', 'expect_exception', 'error_message', 'expected_output'),
        [
            (['Arctic', 'Antarctic'], None, None, ['Arctic', 'Antarctic']),
            (None, None, None, None),  # Allow plotting all available regions
            ('Arctic', TypeError, "Expected regions_to_plot to be a list", None),
            ([1, 2, 3], TypeError, "Expected a list of strings", None),
        ]
    )
    def test_check_list_regions_type(self, regions_to_plot, expect_exception, error_message, expected_output):
        """Test _check_list_regions_type method."""

        plot_seaice = PlotSeaIce()

        if expect_exception:
            with pytest.raises(expect_exception, match=error_message if error_message else ""):
                plot_seaice._check_list_regions_type(regions_to_plot)
            return

        result = plot_seaice._check_list_regions_type(regions_to_plot)
        assert result == expected_output


    @pytest.mark.parametrize(
        ('datain', 'expect_exception', 'error_message', 'expected_output'),
        [
            (siext, None, None, [siext]),  # Valid dataset
            ([siext], None, None, [siext]),  # Valid list of datasets
            (None, None, None, None),
            ("string",  ValueError, "Invalid input. Expected xr.Dataset, list of xr.Dataset, or None.", None),
            ([1, 2, 3], ValueError, "Invalid input. Expected xr.Dataset, list of xr.Dataset, or None.", None),
        ]
    )
    def test_check_as_datasets_list(self, datain, expect_exception, error_message, expected_output):
        """Test _check_as_datasets_list method."""

        plot_seaice = PlotSeaIce()

        if expect_exception:
            with pytest.raises(expect_exception, match=error_message if error_message else ""):
                plot_seaice._check_as_datasets_list(datain)
            return

        result = plot_seaice._check_as_datasets_list(datain)
        assert isinstance(result, list) or result is None

    @pytest.mark.parametrize(
        ("input_datasets, regions_to_plot, expected_output"),
        [
            # Single dataset, correctly structured output
            ({"monthly_models": [create_test_dataset_for_repack_datasetlists("extent", "Arctic")]}, None, {
                "extent": {"Arctic": {"monthly_models": [create_test_dataset_for_repack_datasetlists("extent", "Arctic")[f"sea_ice_extent_arctic"]]}}
            }),

            # Multiple datasets with different methods & regions
            ({"monthly_models": [create_test_dataset_for_repack_datasetlists("extent", "Arctic"), create_test_dataset_for_repack_datasetlists("volume", "Antarctic")]}, None, {
                "extent": {"Arctic": {"monthly_models": [create_test_dataset_for_repack_datasetlists("extent", "Arctic")[f"sea_ice_extent_arctic"]]}},
                "volume": {"Antarctic": {"monthly_models": [create_test_dataset_for_repack_datasetlists("volume", "Antarctic")[f"sea_ice_volume_antarctic"]]}}
            }),

            # Filtering by regions_to_plot
            ({"monthly_models": [create_test_dataset_for_repack_datasetlists("extent", "Arctic"), create_test_dataset_for_repack_datasetlists("extent", "Antarctic")]}, ["Arctic"], {
                "extent": {"Arctic": {"monthly_models": [create_test_dataset_for_repack_datasetlists("extent", "Arctic")[f"sea_ice_extent_arctic"]]}}
            }),

            # Dataset missing "AQUA_method" (defaults to "Unknown")
            ({"monthly_models": [xr.Dataset({"sea_ice_extent_arctic": (["time"], [1, 2, 3])}, coords={"time": [1, 2, 3]}, attrs={"AQUA_region": "Arctic"})]}, None, {
                "Unknown": {"Arctic": {"monthly_models": [xr.DataArray([1, 2, 3], dims="time", coords={"time": [1, 2, 3]}, name="var")]}}
            }),

            # Dataset missing "AQUA_region" (should raise KeyError)
            ({"monthly_models": [xr.Dataset({"dummy": (["time"], [1, 2, 3])},
                                            coords={"time": [1, 2, 3]},attrs={"AQUA_method": "extent"}).rename_vars({"dummy": ""})  # Make variable name empty
                                ]
            }, None, KeyError),

            # Empty input dictionary (should return empty dict)
            ({}, None, {}),

            # All datasets in input are None (should return empty dict)
            ({"monthly_models": None, "annual_models": None}, None, {}),
        ],
    )
    def test_repack_datasetlists(self, input_datasets, regions_to_plot, expected_output):
        """Test repack_datasetlists for correct structuring and filtering."""
        plot_seaice = PlotSeaIce(regions_to_plot=regions_to_plot)

        if isinstance(expected_output, type) and issubclass(expected_output, Exception):
            with pytest.raises(expected_output):
                plot_seaice.repack_datasetlists(**input_datasets)
        else:
            result = plot_seaice.repack_datasetlists(**input_datasets)
            assert isinstance(result, dict) and result.keys() == expected_output.keys()

            for method, regions in expected_output.items():
                assert method in result
                for region, datasets in regions.items():
                    assert region in result[method]
                    for key, expected_data in datasets.items():
                        assert key in result[method][region]
                        assert len(result[method][region][key]) == len(expected_data)

                        for da_expected, da_result in zip(expected_data, result[method][region][key]):
                            assert da_result.equals(da_expected)  # Ensure DataArrays match


    def test_plot_seaice_timeseries(self):
        """Test the plot_seaice_timeseries method with real datasets."""
        
        plot_seaice = PlotSeaIce(
                            monthly_models=siext, 
                            annual_models=None,
                            monthly_ref=None, #[siext_ref_nh, siext_ref_sh], 
                            annual_ref=None, 
                            monthly_std_ref=None, #[siext_std_ref_nh, siext_std_ref_sh], 
                            annual_std_ref=None,
                            model='Test_model', exp='Test_exp', source='Test_source', 
                            regions_to_plot=regions, 
                            outdir='./test_output/', rebuild=True, dpi=100, loglevel='WARNING'
                            )

        # Ensure the plot runs without errors
        plot_seaice.plot_seaice_timeseries(save_pdf=False, save_png=False)  # Disable file saving to focus on function execution

        print("Plot completed.")  # Debugging

        assert True  # If no exception was raised, the test is successful
