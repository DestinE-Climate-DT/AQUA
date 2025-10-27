import pytest
import numpy as np
from aqua import Reader, catalog

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = "DEBUG"

@pytest.fixture
def reader_instance():
    return Reader(model="FESOM", exp="test-pi", source="original_2d",
                  regrid="r200", fix=False, loglevel=loglevel)

# aqua class for tests
@pytest.mark.aqua
class TestAqua:
    """Basic tests for AQUA"""

    @pytest.mark.parametrize("module_name", ["aqua", "aqua.diagnostics"])
    def test_aqua_import(self, module_name):
        """
        Test if the aqua module is imported correctly
        """
        try:
            __import__(module_name)
        except ImportError:
            assert False, "Module {} could not be imported".format(module_name)

    def test_aqua_catalog(self):
        """
        Test if the catalog function returns a non-empty list
        """
        cat = catalog()
        assert len(cat) > 0

    def test_reader_init(self):
        """
        Test the initialization of the Reader class
        """
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d",
                        fix=False, loglevel=loglevel)
        assert reader.model == "FESOM"
        assert reader.exp == "test-pi"
        assert reader.source == "original_2d"

    def test_retrieve_data(self, reader_instance):
        """
        Test if the retrieve method returns data with the expected shape
        """
        data = reader_instance.retrieve()
        assert len(data) > 0
        assert data.a_ice.shape == (2, 3140)
        assert data.a_ice.attrs['AQUA_catalog'] == 'ci'
        assert data.a_ice.attrs['AQUA_model'] == 'FESOM'
        assert data.a_ice.attrs['AQUA_exp'] == 'test-pi'
        assert data.a_ice.attrs['AQUA_source'] == 'original_2d'

    def test_regrid_data(self, reader_instance):
        """
        Test if the regrid method returns data with the expected
        shape and values
        """
        data = reader_instance.retrieve()
        sstr = reader_instance.regrid(data["sst"][0:2, :])
        assert sstr.shape == (2, 90, 180)
        assert np.nanmean(sstr[0, :, :].values) == pytest.approx(13.350324258783935, rel=approx_rel)
        assert np.nanmean(sstr[1, :, :].values) == pytest.approx(13.319154700343551, rel=approx_rel)

    def test_fldmean(self, reader_instance):
        """
        Test if the fldmean method returns data with the expected
        shape and values
        """
        data = reader_instance.retrieve()
        global_mean = reader_instance.fldmean(data.sst[:2, :])
        assert global_mean.shape == (2,)
        assert global_mean.values[0] == pytest.approx(17.99434183,
                                                      rel=approx_rel)
        assert global_mean.values[1] == pytest.approx(17.98060367,
                                                      rel=approx_rel)
        
    def test_chunks(self):
        """
        Test that the Reader class correctly handles chunking
        """
        reader = Reader(model="IFS", exp="test-tco79", source="long",
                        chunks={"time": 12}, loglevel=loglevel)
        data = reader.retrieve()
        assert set(data['2t'].chunksizes['time']) == {12}
        reader = Reader(model="IFS", exp="test-tco79", source="long",
                        chunks={"time": 1}, loglevel=loglevel)
        data = reader.retrieve()
        assert set(data['2t'].chunksizes['time']) == {1}
        

    def test_catalog_override(self):
        """
        Test the compact catalog override functionality
        """
        reader = Reader(model="IFS", exp="test-tco79", source="short_override",
                        loglevel=loglevel)
        assert reader.esmcat.metadata['test-key'] == "test-value"  # from the default
        assert reader.src_grid_name == "tco79-nn"  # overwritten key

    def test_empty_dataset_error(self, reader_instance):
        """
        Test that an empty dataset is returned when nonexistent variable is retrieved
        Check that we get an empty dataset (not None)
        """
        result = reader_instance.retrieve(var="nonexistent_variable")
        assert len(result.data_vars) == 0

    def test_time_selection_with_dates(self, reader_instance):
        """
        Test that time selection is applied when both startdate and enddate are provided
        """
        full_data = reader_instance.retrieve()

        if len(full_data.time) < 2:
            pytest.skip("Not enough timesteps to test")
        
        startdate = str(full_data.time[0].values)[:10]
        enddate = str(full_data.time[-1].values)[:10]
    
        selected_data = reader_instance.retrieve(startdate=startdate, enddate=enddate)
        
        # Verify time selection was applied
        assert len(selected_data.time) == len(full_data.time)
        assert selected_data.time[0].values == full_data.time[0].values

    @pytest.fixture(
        params=[
            ("IFS", "test-tco79", "short", "r200", "tas"),
            ("FESOM", "test-pi", "original_2d", "r200", "sst"),
            ("NEMO", "test-eORCA1", "long-2d", "r200", "sst")
        ]
    )
    def reader_arguments(self, request):
        return request.param

    def test_reader_with_different_arguments(self, reader_arguments):
        """
        Test if the Reader class works with different combinations of arguments
        """
        model, exp, source, regrid, _ = reader_arguments
        reader = Reader(model=model, exp=exp, source=source, regrid=regrid,
                        fix=False, loglevel=loglevel)
        data = reader.retrieve()

        # Check the time precision
        if model == 'NEMO':
            assert data.time.values[0].dtype == 'datetime64[s]'

        assert len(data) > 0
