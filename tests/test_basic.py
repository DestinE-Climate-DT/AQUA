import pytest
import numpy as np
from aqua import Reader, catalogue

# pytest approximation, to bear with different machines
approx_rel=1e-4

@pytest.fixture
def reader_instance():
    return Reader(model="FESOM", exp="test-pi", source="original_2d", regrid="r200")

# aqua class for tests
@pytest.mark.aqua
class TestAqua:
    """Basic tests for AQUA"""

    def test_aqua_import(self):
        """
        Test if the aqua module is imported correctly
        """
        assert True

    def test_aqua_catalogue(self):
        """
        Test if the catalogue function returns a non-empty list
        """
        catalog = catalogue()
        assert len(catalog) > 0

    def test_reader_init(self):
        """
        Test the initialization of the Reader class
        """
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d", configdir = "config")
        assert reader.model == "FESOM"
        assert reader.exp == "test-pi"
        assert reader.source == "original_2d"
        assert reader.configdir == "config"

    def test_retrieve_data(self, reader_instance):
        """
        Test if the retrieve method returns data with the expected shape
        """
        data = reader_instance.retrieve(fix=False)
        assert len(data) > 0
        assert data.a_ice.shape == (2, 3140)

    def test_regrid_data(self, reader_instance):
        """
        Test if the regrid method returns data with the expected shape and values
        """
        data = reader_instance.retrieve(fix=False)
        sstr = reader_instance.regrid(data["sst"][0:2, :])
        assert sstr.shape == (2, 90, 180)
        assert np.nanmean(sstr[0, :, :].values) == pytest.approx(13.350324258783935, rel=approx_rel)
        assert np.nanmean(sstr[1, :, :].values) == pytest.approx(13.319154700343551, rel=approx_rel)

    def test_fldmean(self, reader_instance):
        """
        Test if the fldmean method returns data with the expected shape and values
        """
        data = reader_instance.retrieve(fix=False)
        global_mean = reader_instance.fldmean(data.sst[:2, :])
        assert global_mean.shape == (2,)
        assert global_mean.values[0] == pytest.approx(17.99434183, rel=approx_rel)
        assert global_mean.values[1] == pytest.approx(17.98060367, rel=approx_rel)

    @pytest.fixture(
        params=[
            ("IFS", "test-tco79", "short", "r200", "tas"),
            ("FESOM", "test-pi", "original_2d", "r200", "sst"),
        ]
    )
    def reader_arguments(self, request):
        return request.param

    def test_reader_with_different_arguments(self, reader_arguments):
        """
        Test if the Reader class works with different combinations of arguments
        """
        model, exp, source, regrid, variable = reader_arguments
        reader = Reader(model=model, exp=exp, source=source, regrid=regrid)
        data = reader.retrieve(fix=False)
        assert len(data) > 0
