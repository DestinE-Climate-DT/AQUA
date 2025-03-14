"""Test regridding from Reader"""
import pytest
from aqua import Reader
from aqua.regridder.griddicthandler import GridDictHandler
from aqua.regridder import Regridder

LOGLEVEL = "DEBUG"
approx_rel = 1e-4

@pytest.fixture(
    params=[
        ("IFS", "test-tco79", "short", "2t", 0),
        ("IFS", "test-tco79", "short_nn", "2t", 0),
        ("IFS", "test-tco79", "long", "mtntrf", 0),
        ("ICON", "test-r2b0", "short", "2t", 0),
        ("ICON", "test-healpix", "short", "2t", 0),
        ("FESOM", "test-pi", "original_2d", "tos", 0.33925926),
        ("NEMO", "test-eORCA1", "long-2d", "tos", 0.28716)
    ]
)
def reader_arguments(request):
    return request.param

# fake dictionary for doing tests
cfg_dict = {
    "grids":{
        "r1000": "r36x18",
        "n128": "n128",
        "tragic": "tragic",
        "afternoon": { "path": { "2d": "r100x50" } },
        "doing": { "path": "noise.nc" },
        "wonderful": { "path": { '2d': "r360x180" } },
        "lovely": { "path": { '2d': "tests" } },
        "tests": { "path": { "pizza please!" } }
    }
}


@pytest.mark.aqua
class TestRegridder():
    """class for regridding test"""

    def test_grid_dict_handler(self):
        """Test the grid dictionary handler"""
        
        gdh = GridDictHandler(cfg_dict, loglevel=LOGLEVEL)

        # empty one
        assert gdh.normalize_grid_dict(None) == {}
        # standard
        assert gdh.normalize_grid_dict("r1000") == {"path": {"2d": "r36x18"}}
        # couple of CDO grids
        assert gdh.normalize_grid_dict("n256") == {"path": {"2d": "n256"}}
        assert gdh.normalize_grid_dict("r100x50") == {"path": {"2d": "r100x50"}}

        # test of errors
        with pytest.raises(ValueError, match="Grid name 'ciao' not found in the configuration."):
            gdh.normalize_grid_dict("ciao")
        with pytest.raises(ValueError, match="Grid name '20' is not a valid type."):
            gdh.normalize_grid_dict(20)
        with pytest.raises(ValueError, match="Grid name 'tragic' is not a valid CDO grid name."):
            gdh.normalize_grid_dict("tragic")

        # test on grid path
        assert gdh.normalize_grid_dict("afternoon")['path'] == {'2d': 'r100x50'}
        assert gdh.normalize_grid_dict("lovely")['path'] == {'2d': 'tests'}
        assert gdh.normalize_grid_dict("wonderful")['path'] == {'2d': 'r360x180'}
        
        # errors on grid path
        with pytest.raises(FileNotFoundError, match="Grid file 'noise.nc' does not exist."):
            gdh.normalize_grid_dict("doing")
        with pytest.raises(ValueError, match="Grid path '{'pizza please!'}' is not a valid type."):
            gdh.normalize_grid_dict("tests")

    def test_regridder(self):
        """Testing the regridder all in independent way"""

        reader = Reader(catalog='ci', model='IFS', exp='test-tco79', source='long', loglevel='ERROR')
        data = reader.retrieve()
        regridder = Regridder(data=data.isel(time=0), loglevel='debug')

        # Regrid the data
        regridder.weights(tgt_grid_name='r144x72', regrid_method="bil")
        out = regridder.regrid(data)

        assert len(out.lon) == 144
        assert len(out.lat) == 72

    def test_basic_interpolation(self, reader_arguments):
        """
        Test basic interpolation,
        checking output grid dimension and
        fraction of land (i.e. any missing points)
        """
        model, exp, source, variable, ratio = reader_arguments

        reader = Reader(model=model, exp=exp, source=source, regrid="r200",
                        fix=True, loglevel=LOGLEVEL)
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
                        regrid='r100', rebuild=True, fix=False, loglevel=LOGLEVEL)
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
                        regrid='r100', rebuild=True, loglevel=LOGLEVEL)
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
                        regrid='r100', rebuild=True, fix=False, loglevel=LOGLEVEL)
        data = reader.retrieve(var='temp')
        rgd = reader.regrid(data)

        subdata = rgd.temp.isel(nz1=0)
        ratio1 = subdata.isnull().sum()/subdata.size  # land fraction
        subdata = rgd.temp.isel(nz1=2)
        ratio2 = subdata.isnull().sum()/subdata.size  # land fraction
        assert len(rgd.lon) == 360
        assert len(rgd.lat) == 180
        assert len(rgd.time) == 2
        assert 0.33 <= ratio1 <= 0.36
        assert 0.43 <= ratio2 <= 0.46

    def test_recompute_weights_nemo3D(self):
        """
        Test interpolation on NEMO, at different grid rebuilding weights,
        checking output grid dimension and fraction of land
        (i.e. any missing points)
        """
        reader = Reader(model='NEMO', exp='test-eORCA1', source='short-3d',
                        regrid='r200', rebuild=True, fix=False, loglevel=LOGLEVEL)
        data = reader.retrieve(var='avg_so')
        rgd = reader.regrid(data)

        subdata = rgd.avg_so.isel(level=0)
        ratio1 = subdata.isnull().sum()/subdata.size  # land fraction
        subdata = rgd.avg_so.isel(level=6)
        ratio2 = subdata.isnull().sum()/subdata.size  # land fraction
        assert len(rgd.lon) == 180
        assert len(rgd.lat) == 90
        assert 0.27 <= ratio1 <= 0.30
        assert 0.44 <= ratio2 <= 0.46

    def test_levels_and_regrid(self):
        """
        Test regridding selected levels.
        """
        reader = Reader(model='FESOM', exp='test-pi', source='original_3d',
                        regrid='r100', loglevel=LOGLEVEL)
        data = reader.retrieve()

        layers = [0, 2]
        val = data.aqua.regrid().isel(time=1, nz=2, nz1=layers).wo.aqua.fldmean().values
        assert val == pytest.approx(8.6758228e-08)
        val = data.isel(time=1, nz=2, nz1=layers).aqua.regrid().wo.aqua.fldmean().values
        assert val == pytest.approx(8.6758228e-08)
        val = data.isel(time=1, nz=2, nz1=layers).wo.aqua.regrid().aqua.fldmean().values
        assert val == pytest.approx(8.6758228e-08)
        val = data.isel(time=1, nz=2, nz1=layers).aqua.regrid().thetao.isel(nz1=1).aqua.fldmean().values
        assert val == pytest.approx(274.9045)
        val = data.aqua.regrid().isel(time=1, nz=2, nz1=layers).thetao.isel(nz1=1).aqua.fldmean().values
        assert val == pytest.approx(274.9045)
        val = data.isel(time=1, nz=2, nz1=layers).thetao.aqua.regrid().isel(nz1=1).aqua.fldmean().values
        assert val == pytest.approx(274.9045)

        # test reading specific levels for first vertical coordinate (nz1)
        data = reader.retrieve(level=[2.5, 2275])
        val = data.isel(time=1).aqua.regrid().thetao.isel(nz1=1).aqua.fldmean().values
        assert val == pytest.approx(274.9045)


@pytest.mark.aqua
def test_non_latlon_interpolation():
    """
    Test interpolation to a non regular grid,
    checking appropriate logging message
    """
    reader = Reader(model="IFS", exp="test-tco79", source="short", regrid="F80",
                    fix=True, loglevel='DEBUG', rebuild=True)

    data = reader.retrieve(var='2t')['2t'].isel(time=0).aqua.regrid()

    assert data.shape == (160, 320)
    assert data.values[0, 0] == pytest.approx(246.71156470963325)

# missing test for ICON-Healpix
