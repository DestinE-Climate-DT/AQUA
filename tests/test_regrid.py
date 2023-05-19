"""Test regridding from Reader"""

import pytest
from aqua import Reader

@pytest.fixture(
    params=[
        ("IFS", "test-tco79", "short", "2t", 0),
        ("IFS", "test-tco79", "long", "ttr", 0),
        ("FESOM", "test-pi", "original_2d", "sst", 0.33925926),
    ]
)
def reader_arguments(request):
    return request.param

@pytest.mark.aqua
class TestRegridder():
    def test_basic_interpolation(self, reader_arguments):
        """Test basic interpolation on a set of variable, 
        checking output grid dimension and 
        fraction of land (i.e. any missing points)"""
        model, exp, source, variable, ratio = reader_arguments
        reader = Reader(model=model, exp=exp, source=source, regrid="r200")
        data = reader.retrieve(fix=False)
        rgd = reader.regrid(data[variable])
        assert len(rgd.lon) == 180
        assert len(rgd.lat) == 90
        assert ratio == pytest.approx((rgd.isnull().sum()/rgd.size).values) #land fraction

    def test_recompute_weights_fesom(self):
        """Test interpolation on FESOM, at different grid rebuilding weights, 
        checking output grid dimension and fraction of land (i.e. any missing points)"""
        reader = Reader(model='FESOM', exp='test-pi', source='original_2d',
                        regrid='r100', rebuild=True)
        rgd = reader.retrieve(var='sst', fix=False, regrid=True)
        ratio = rgd['sst'].isnull().sum()/rgd['sst'].size  #land fraction
        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.time) == 2
        assert 0.33 <= ratio <= 0.36

    def test_recompute_weights_ifs(self):
        """Test the case where no source grid path is specified in the regrid.yaml file
        and areas/weights are reconstructed from the file itself"""
        reader = Reader(model='IFS', exp='test-tco79', source='long',
                        regrid='r100', rebuild=True)
        rgd = reader.retrieve(vars='ttr', fix=False, regrid=True)
        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.time) == 4728

    #missing test for FESOM 3D and ICON-Healpix
