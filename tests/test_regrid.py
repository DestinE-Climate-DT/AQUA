"""Test regridding from Reader"""

import pytest
from aqua import Reader

loglevel = "DEBUG"
approx_rel = 1e-4

@pytest.fixture(
    params=[
        ("IFS", "test-tco79", "short", "2t", 0),
        ("IFS", "test-tco79", "short_nn", "2t", 0),
        ("IFS", "test-tco79", "long", "mtntrf", 0),
        ("ICON", "test-r2b0", "short", "2t", 0),
        ("ICON", "test-healpix", "short", "2t", 0),
        ("FESOM", "test-pi", "original_2d", "sst", 0.33925926),
        ("NEMO", "test-eORCA1", "long-2d", "sst", 0.28716)
    ]
)
def reader_arguments(request):
    return request.param


@pytest.mark.aqua
class TestRegridder():
    """class for regridding test"""

    def test_basic_interpolation(self, reader_arguments):
        """
        Test basic interpolation on a set of variable,
        checking output grid dimension and
        fraction of land (i.e. any missing points)
        """
        model, exp, source, variable, ratio = reader_arguments

        reader = Reader(model=model, exp=exp, source=source, regrid="r200",
                        fix=True, loglevel=loglevel)
        data = reader.retrieve()
        rgd = reader.regrid(data[variable])
        assert len(rgd.lon) == 180
        assert len(rgd.lat) == 90
        assert ratio == pytest.approx((rgd.isnull().sum()/rgd.size).values, rel=approx_rel)  # land fraction

    def test_recompute_weights_fesom2D(self):
        """
        Test interpolation on FESOM, at different grid rebuilding weights,
        checking output grid dimension and fraction of land
        (i.e. any missing points)
        """
        reader = Reader(model='FESOM', exp='test-pi', source='original_2d',
                        regrid='r100', rebuild=True, fix=False, loglevel=loglevel)
        data = reader.retrieve(var='sst')
        rgd = reader.regrid(data)

        ratio = rgd['sst'].isnull().sum()/rgd['sst'].size  # land fraction

        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.time) == 2
        assert 0.33 <= ratio <= 0.36

    def test_recompute_weights_ifs(self):
        """Test the case where no source grid path is specified in the regrid.yaml file
        and areas/weights are reconstructed from the file itself"""
        reader = Reader(model='IFS', exp='test-tco79', source='long',
                        regrid='r100', rebuild=True, loglevel=loglevel)
        data = reader.retrieve(var='ttr')
        rgd = reader.regrid(data)

        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.time) == 4728

    def test_recompute_weights_healpix(self):
        """Test Healpix and areas/weights are reconstructed from the file itself"""
        reader = Reader(model='ICON', exp='test-healpix', source='short',
                        regrid='r100', rebuild=True)
        data = reader.retrieve(var='t')
        rgd = reader.regrid(data)

        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.level_full) == 90
        assert len(rgd.time) == 2

    def test_recompute_weights_fesom3D(self):
        """
        Test interpolation on FESOM, at different grid rebuilding weights,
        checking output grid dimension and fraction of land
        (i.e. any missing points)
        """
        reader = Reader(model='FESOM', exp='test-pi', source='original_3d',
                        regrid='r100', rebuild=True, fix=False, loglevel=loglevel)
        data = reader.retrieve(var='temp')
        rgd = reader.regrid(data)

        ratio1 = rgd.temp.isel(nz1=0).isnull().sum()/rgd.temp.isel(nz1=0).size  # land fraction
        ratio2 = rgd.temp.isel(nz1=40).isnull().sum()/rgd.temp.isel(nz1=40).size  # land fraction
        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.time) == 2
        assert 0.33 <= ratio1 <= 0.36
        assert 0.75 <= ratio2 <= 0.79

    def test_recompute_weights_nemo3D(self):
        """
        Test interpolation on NEMO, at different grid rebuilding weights,
        checking output grid dimension and fraction of land
        (i.e. any missing points)
        """
        reader = Reader(model='NEMO', exp='test-eORCA1', source='short-3d',
                        regrid='r200', rebuild=True, fix=False, loglevel=loglevel)
        data = reader.retrieve(var='avg_so')
        rgd = reader.regrid(data)

        ratio1 = rgd.avg_so.isel(level=0).isnull().sum()/rgd.avg_so.isel(level=0).size  # land fraction
        ratio2 = rgd.avg_so.isel(level=40).isnull().sum()/rgd.avg_so.isel(level=40).size  # land fraction
        assert len(rgd.lon) == 180
        assert len(rgd.lat) == 90
        assert 0.27 <= ratio1 <= 0.30
        assert 0.35 <= ratio2 <= 0.39
    

# missing test for ICON-Healpix
