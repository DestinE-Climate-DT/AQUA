"""Tests for the GridBuilder class."""
import pytest
from aqua import GridBuilder
from aqua import Reader


@pytest.mark.aqua
class TestGridBuilder:
    """Test the GridBuilder class."""

    def test_grid_healpix_polytope(self, tmp_path):
        """Test the GridBuilder class with a HEALPix grid."""
        reader = Reader(
            model="IFS-FESOM", exp="story-2017-control", source="hourly-hpz7-atm2d",
            engine="polytope", areas=False, chunks={'time': 'H'})
        data = reader.retrieve(var='2t')
        grid_builder = GridBuilder(outdir=tmp_path, model_name='IFS', original_resolution='tco1279')
        grid_builder.build(data, verify=True, create_yaml=False)

    @pytest.mark.parametrize("rebuild", [False, True])
    def test_grid_regular(self, tmp_path, rebuild):
        """Test the GridBuilder class with a regular grid."""
        reader = Reader(model='IFS', exp='test-tco79', source='long', loglevel='debug', areas=False, fix=False)
        data = reader.retrieve()
        grid_builder = GridBuilder(outdir=tmp_path, original_resolution='tco79')
        grid_builder.build(data, verify=True, rebuild=rebuild)
    
    def test_grid_curvilinear(self, tmp_path):
        """Test the GridBuilder class with a regular grid."""
        reader = Reader(model='ECE4-FAST', exp='test', source='monthly-oce', loglevel='debug', areas=False)
        data = reader.retrieve()
        grid_builder = GridBuilder(outdir=tmp_path, model_name='nemo', grid_name='ORCA2')
        grid_builder.build(data, verify=False) #set to False since it is very heavy

    def test_grid_unstructured(self, tmp_path):
        """Test the GridBuilder class with an unstructured grid."""
        reader = Reader(model='ECE4-FAST', exp='test', source='monthly-atm', loglevel='debug', areas=False)
        data = reader.retrieve()
        grid_builder = GridBuilder(outdir=tmp_path, model_name='ifs', grid_name='tl63')
        grid_builder.build(data, verify=True, create_yaml=False) # this is not working yet
    
    def test_grid_healpix(self, tmp_path):
        """Test the GridBuilder class with a HEALPix grid."""
        reader = Reader(model='ERA5', exp='era5-hpz3', source='monthly', loglevel='debug', areas=False)
        data = reader.retrieve()
        grid_builder = GridBuilder(outdir=tmp_path)
        grid_builder.build(data, verify=True, create_yaml=False)


