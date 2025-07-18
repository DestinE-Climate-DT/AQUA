"""Tests for the GridBuilder class."""
import subprocess
import os
import pytest
from aqua import GridBuilder
from aqua import Reader
from aqua.regridder.gridentry import GridEntryManager
from aqua.util import ConfigPath, load_yaml


@pytest.mark.aqua
class TestGridBuilder:
    """Test the GridBuilder class."""
    grid_dir = f'{ConfigPath().configdir}/grids'

    # def test_grid_healpix_polytope(self, tmp_path):
    #     """Test the GridBuilder class with a HEALPix grid."""
    #     reader = Reader(
    #         model="IFS-FESOM", exp="story-2017-control", source="hourly-hpz7-atm2d",
    #         engine="polytope", areas=False, chunks={'time': 'H'})
    #     data = reader.retrieve(var='2t')
    #     grid_builder = GridBuilder(outdir=tmp_path, model_name='IFS', original_resolution='tco1279')
    #     grid_builder.build(data, verify=True, create_yaml=False)

    @pytest.mark.parametrize("rebuild", [False, True])
    def test_grid_regular(self, tmp_path, rebuild):
        """Test the GridBuilder class with a regular grid."""
        reader = Reader(model='IFS', exp='test-tco79', source='long', loglevel='debug', areas=False, fix=False)
        data = reader.retrieve()
        grid_builder = GridBuilder(outdir=tmp_path, original_resolution='tco79')
        grid_builder.build(data, verify=True, rebuild=rebuild, create_yaml=True)
        assert os.path.exists(f'{self.grid_dir}/regular.yaml')
        os.remove(f'{self.grid_dir}/regular.yaml')
    
    def test_grid_curvilinear(self, tmp_path):
        """Test the GridBuilder class with a regular grid."""
        reader = Reader(model='ECE4-FAST', exp='test', source='monthly-oce', loglevel='debug', areas=False)
        data = reader.retrieve()
        grid_builder = GridBuilder(outdir=tmp_path, model_name='nemo', grid_name='ORCA2')
        grid_builder.build(data, verify=False, create_yaml=True) #set to False since it is very heavy
        assert os.path.exists(f'{self.grid_dir}/nemo-curvilinear.yaml')
        grid = load_yaml(f'{self.grid_dir}/nemo-curvilinear.yaml')
        assert set(grid['grids']['nemo-ORCA2-2d']['space_coord']) == set(['y', 'x'])
        assert grid['grids']['nemo-ORCA2-2d']['remap_method'] == 'bil'
        os.remove(f'{self.grid_dir}/nemo-curvilinear.yaml')

    def test_grid_unstructured(self, tmp_path):
        """Test the GridBuilder class with an unstructured grid."""
        reader = Reader(model='ECE4-FAST', exp='test', source='monthly-atm', loglevel='debug', areas=False)
        data = reader.retrieve()
        grid_builder = GridBuilder(outdir=tmp_path, model_name='ifs', grid_name='tl63')
        grid_builder.build(data, verify=True, create_yaml=True) # this is not working yet
        assert os.path.exists(f'{self.grid_dir}/ifs-unstructured.yaml')
        os.remove(f'{self.grid_dir}/ifs-unstructured.yaml')
        
    
    def test_grid_healpix(self, tmp_path):
        """Test the GridBuilder class with a HEALPix grid."""
        reader = Reader(model='ERA5', exp='era5-hpz3', source='monthly', loglevel='debug', areas=False)
        data = reader.retrieve()
        grid_builder = GridBuilder(outdir=tmp_path)
        grid_builder.build(data, verify=True, create_yaml=False)

    def test_cli_grid_healpix(self, tmp_path):
        """Test the GridBuilder class making use of the CLI."""
        command = [
            'aqua', 'grids', 'build', 
            '--model', 'ERA5', '--exp', 'era5-hpz3', '--source', 'monthly', 
            '--verify', '--outdir', tmp_path
        ]
        subprocess.run(command, check=True)


@pytest.mark.aqua
class TestGridEntryManager:
    """Test the GridEntryManager class."""
    @pytest.mark.parametrize(
        "model,aquagrid,cdogrid,original,kind", [
            ('IFS', 'hpz7-nested', 'hp32_nested', 'tco79', 'healpix'),
            ('IFS', 'tco79', None, None, 'unstructured'),
            ('IFS', 'r100', 'r180x90', 'tco79', 'regular'),
            ('NEMO','eORCA1', None, None, 'curvilinear'),
        ]
    )
    def test_gem_basic(self, model, aquagrid, cdogrid, original, kind):
        """Test the GridEntryManager class without mask"""
        gem = GridEntryManager(
            model_name=model, original_resolution=original, grid_name=aquagrid
        )
        filename = gem.get_gridfilename(cdogrid, kind)
        basename = gem.get_basename(aquagrid, cdogrid)
        if cdogrid:
            assert f'{kind}.yaml' == os.path.basename(filename)
            assert aquagrid == basename
        else:
            assert f'{model.lower()}-{kind}.yaml' == os.path.basename(filename)
            assert f'{model.lower()}_{aquagrid}' == basename
       
        entry = gem.create_grid_entry_name(aquagrid, cdogrid)
        assert basename.replace('_', '-') == entry

    def test_gem_curvilinear(self):
        gem = GridEntryManager(model_name='nemo', grid_name='ORCA2')

        filename = gem.get_gridfilename(cdogrid=None, gridkind='curvilinear')
        assert os.path.basename(filename) == 'nemo-curvilinear.yaml'
        basename = gem.get_basename(aquagrid='orca2', cdogrid=None, masked='oce', vert_coord='depth')
        assert basename == 'nemo_orca2_3d_depth'
        entry = gem.create_grid_entry_name(aquagrid='orca2', cdogrid=None, masked='oce', vert_coord='depth')
        assert 'nemo-orca2-3d-depth' == entry
    
        block = gem.create_grid_entry_block(
            path='orca2_oce_depth_v1.nc',
            horizontal_dims='cells',
            cdo_options='-f nc',
            remap_method='bil',
            vert_coord='depth'
        )
        assert block['space_coord'] == 'cells'
        assert block['vert_coord'] == 'depth'
        assert block['cdo_options'] == '-f nc'
        assert block['remap_method'] == 'bil'
        assert block['path']['depth'] == 'orca2_oce_depth_v1.nc'

        




