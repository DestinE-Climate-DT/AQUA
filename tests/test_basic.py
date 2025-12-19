import pytest
import numpy as np
from aqua import Reader
from conftest import APPROX_REL, LOGLEVEL

approx_rel = APPROX_REL
loglevel = LOGLEVEL

@pytest.fixture(scope='module')
def reader_instance(fesom_test_pi_original_2d_r200_fixFalse_reader):
    return fesom_test_pi_original_2d_r200_fixFalse_reader

@pytest.fixture(scope='module')
def data(fesom_test_pi_original_2d_r200_fixFalse_data):
    return fesom_test_pi_original_2d_r200_fixFalse_data

@pytest.fixture(scope='module')
def reader_ifs_tco79_long(ifs_tco79_long_reader):
    return ifs_tco79_long_reader

# aqua class for tests
@pytest.mark.aqua
class TestAqua:
    """Basic tests for AQUA"""

    @pytest.mark.parametrize("module_name", ["aqua"])
    def test_aqua_import(self, module_name):
        """
        Test if the aqua module is imported correctly
        """
        try:
            __import__(module_name)
        except ImportError:
            assert False, "Module {} could not be imported".format(module_name)

    def test_reader_init(self):
        """
        Test the initialization of the Reader class
        """
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d",
                        fix=False, loglevel=loglevel)
        assert reader.model == "FESOM"
        assert reader.exp == "test-pi"
        assert reader.source == "original_2d"

    def test_retrieve_data(self, data):
        """
        Test if the retrieve method returns data with the expected shape
        """
        assert len(data) > 0
        assert data.a_ice.shape == (2, 3140)
        assert data.a_ice.attrs['AQUA_catalog'] == 'ci'
        assert data.a_ice.attrs['AQUA_model'] == 'FESOM'
        assert data.a_ice.attrs['AQUA_exp'] == 'test-pi'
        assert data.a_ice.attrs['AQUA_source'] == 'original_2d'

    def test_regrid_data(self, reader_instance, data):
        """
        Test if the regrid method returns data with the expected
        shape and values
        """
        sstr = reader_instance.regrid(data["sst"][0:2, :])
        assert sstr.shape == (2, 90, 180)
        assert np.nanmean(sstr[0, :, :].values) == pytest.approx(13.350324258783935, rel=approx_rel)
        assert np.nanmean(sstr[1, :, :].values) == pytest.approx(13.319154700343551, rel=approx_rel)

    def test_fldmean(self, reader_instance, data):
        """
        Test if the fldmean method returns data with the expected
        shape and values
        """
        global_mean = reader_instance.fldmean(data.sst[:2, :])
        assert global_mean.shape == (2,)
        assert global_mean.values[0] == pytest.approx(17.99434183, rel=approx_rel)
        assert global_mean.values[1] == pytest.approx(17.98060367, rel=approx_rel)
        
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


    def test_time_selection(self, reader_ifs_tco79_long):
        """
        Test that time selection works correctly
        """
        reader = reader_ifs_tco79_long
        
        data = reader.retrieve(startdate='2020-03-01', enddate='2020-03-31')
        
        assert len(data.time) > 0
        assert '2t' in data
        
        assert all(data.time.dt.month == 3)

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

    @pytest.mark.parametrize(
        "realization_input, expected_output, expected_exception",
        [
            ('r2', 2, None),
            (3, 3, None),
            (10, None, ValueError),  # invalid -> expect ValueError
            ('rX', None, ValueError),  # invalid -> expect ValueError
        ],
    )
    def test_realization_formatting_int(self, realization_input, expected_output, expected_exception):
        """
        Test that the realization parameter is correctly formatted or raises for bad input.
        """
        # If the Reader raises during __init__, wrap the constructor in the context manager.
        if expected_exception is not None:
            with pytest.raises(expected_exception):
                Reader(model="ICON", exp="test-healpix", source="fake-ensemble-int",
                    loglevel=loglevel, areas=False, realization=realization_input)
        else:
            reader = Reader(model="ICON", exp="test-healpix", source="fake-ensemble-int",
                            loglevel=loglevel, areas=False, realization=realization_input)
            assert reader.kwargs['realization'] == expected_output

    def test_realization_formatting_str(self):
        """
        Test that the realization parameter is correctly formatted from string.
        """
        reader = Reader(model="ICON", exp="test-healpix", source="fake-ensemble-str",
                        loglevel=loglevel, areas=False, realization='r2i1p1')
        assert reader.kwargs['realization'] == 'r2i1p1'
        
