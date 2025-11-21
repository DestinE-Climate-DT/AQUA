import os
import shutil
import pytest
import xarray as xr
import pandas as pd
from aqua import Drop, Reader
from aqua.core.drop.output_path_builder import OutputPathBuilder
from aqua.core.drop.catalog_entry_builder import CatalogEntryBuilder   
from conftest import LOGLEVEL

DROP_PATH = 'ci/IFS/test-tco79/r1/r100/monthly/mean/global'
DROP_PATH_DAILY = 'ci/IFS/test-tco79/r1/r100/daily/mean/europe'

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
pytestmark = [
    pytest.mark.aqua,
    pytest.mark.console,
    pytest.mark.xdist_group(name="dask_operations")
]

@pytest.fixture(params=[{"model": "IFS", "exp": "test-tco79", "source": "long", "var": "2t", "outdir": "drop_test"}])
def drop_arguments(request):
    """Provides DROP arguments as a dictionary."""
    return request.param


class TestOutputPathBuilder:
    """Class containing tests for OutputPathBuilder."""

    expected = [
        None,
        'ci/IFS/test-tco79/r1/native/nostat/europe/2t_ci_IFS_test-tco79_r1_native_nostat_europe_202001.nc',
        None,
    ]

    @pytest.mark.parametrize("resolution, frequency, realization, region, stat, expected", [
        ('r100', 'monthly', 'r1', 'global', 'mean', expected[0]),
        (None, None, None, 'europe', None, expected[1]),
        ('r200', 'daily', 'r2', 'global', 'nostat', expected[2]),
    ])
    def test_build_path(self, drop_arguments, resolution, frequency, realization, region, stat, expected):
        """Test building output path."""
        builder = OutputPathBuilder(
            catalog='ci', model=drop_arguments["model"], exp=drop_arguments["exp"],
            resolution=resolution,
            frequency=frequency, realization=realization, stat=stat, region=region)
        path = builder.build_path(
            os.path.join(os.getcwd(), drop_arguments['outdir']),
            var=drop_arguments['var'], year=2020, month=1)

        if not expected:
            DROP_PATH = f'ci/IFS/test-tco79/{realization}/{resolution}/{frequency}/{stat}/{region}'
            expected = os.path.join(os.getcwd(), drop_arguments["outdir"], DROP_PATH,
                                         f"2t_ci_IFS_test-tco79_{realization}_{resolution}_{frequency}_{stat}_{region}_202001.nc")
        else:
            expected = os.path.join(os.getcwd(), drop_arguments["outdir"], expected)

        assert path == expected

class TestCatalogEntryBuilder:
    """Class containing tests for CatalogEntryBuilder."""

    @pytest.mark.parametrize("resolution, frequency, realization, region, stat, source_grid_name, chunks", [
        ('r100', 'monthly', 'r1', 'global', 'mean', 'lon-lat-r100', {'time': 12, 'lat': 180, 'lon': 360}),
        ('r200', 'daily', 'r1', 'global', 'mean', 'lon-lat', {'time': 365, 'lat': 90, 'lon': 180}),
        ('r050s', 'monthly', 'r4', 'europe', 'max', 'lon-lat', {'time': 12, 'lat': 361, 'lon': 720}),
      ])
    def test_create_entry_name(self, drop_arguments, resolution, frequency, realization, region, stat, source_grid_name, chunks):
        """Test creation of entry name."""
        builder = CatalogEntryBuilder(
            catalog='ci', **drop_arguments,
            resolution=resolution, frequency=frequency, realization=realization,
            stat=stat, region=region, loglevel=LOGLEVEL
        )
        entry_name = builder.create_entry_name()
        block = builder.create_entry_details(basedir=drop_arguments["outdir"], source_grid_name=source_grid_name)
        
        if resolution == 'r100' and frequency == 'monthly':
            expected_name = f'lra-{resolution}-{frequency}'
        else:
            expected_name = f'{resolution}-{frequency}'
        
        assert entry_name == expected_name
        assert block['driver'] == 'netcdf'
        assert block['parameters'].keys() == {'realization', 'stat', 'region'}

        builder2 = CatalogEntryBuilder(
            catalog='ci', **drop_arguments,
            resolution=resolution, frequency=frequency, realization='r2',
            stat=stat, region=region, loglevel=LOGLEVEL
        )
        newblock = builder2.create_entry_details(basedir=drop_arguments["outdir"], catblock=block, source_grid_name=source_grid_name)
        assert newblock['args']['urlpath'] == block['args']['urlpath']
        assert newblock['parameters']['realization']['allowed'] == [realization,'r2']
        assert newblock['args']['chunks'] == chunks



class TestDROP:
    """Class containing DROP tests."""

    def test_definitive_false(self, drop_arguments, tmp_path):
        """Test DROP with definitive=False."""
        test = Drop(
            catalog='ci', **drop_arguments, tmpdir=str(tmp_path),
            resolution='r100', frequency='monthly', loglevel=LOGLEVEL
        )

        test.retrieve()
        test.drop_generator()
        assert os.path.isdir(os.path.join(os.getcwd(), drop_arguments["outdir"], DROP_PATH))
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    @pytest.mark.parametrize("nworkers", [1, 2])
    def test_definitive_true(self, drop_arguments, tmp_path, nworkers):
        test = Drop(
            catalog='ci', **drop_arguments, tmpdir=str(tmp_path),
            nproc=nworkers, resolution='r100', frequency='monthly',
            definitive=True, loglevel=LOGLEVEL
        )

        test.retrieve()
        test.data = test.data.sel(time="2020-01")
        test.drop_generator()

        file_path = os.path.join(os.getcwd(), drop_arguments["outdir"], DROP_PATH, "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202001.nc")
        test.check_integrity(varname=drop_arguments["var"])
        assert os.path.isfile(file_path)

        file = xr.open_dataset(file_path)
        assert len(file.time) == 1
        assert pytest.approx(file['2t'][0, 1, 1].item()) == 248.0704
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_regional_subset(self, drop_arguments, tmp_path):
        """Test DROP with regional subset."""
        region = {'name': 'europe', 'lon': [-10, 30], 'lat': [35, 70]}

        test = Drop(
            catalog='ci', **drop_arguments, tmpdir=str(tmp_path),
            resolution='r100', frequency='daily', definitive=True,
            loglevel=LOGLEVEL, region=region
        )

        test.retrieve()
        test.data = test.data.sel(time="2020-01-20")
        test.drop_generator()

        file_path = os.path.join(os.getcwd(), drop_arguments["outdir"], DROP_PATH_DAILY, "2t_ci_IFS_test-tco79_r1_r100_daily_mean_europe_202001.nc")
        assert os.path.isfile(file_path), "File not found: {}".format(file_path)

        xfield = xr.open_dataset(file_path).where(lambda x: x.notnull(), drop=True)
        assert xfield.lat.min() > 35
        assert xfield.lat.max() < 70
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_zarr_entry(self, drop_arguments, tmp_path):
        """Test DROP with Zarr archive creation."""
        test = Drop(
            catalog='ci', **drop_arguments, tmpdir=str(tmp_path),
            resolution='r100', frequency='monthly', nproc=1,
            loglevel=LOGLEVEL, definitive=True,
            startdate="2020-01-01", enddate="2020-05-31"
        )

        test.retrieve()
        test.drop_generator()
        test.create_catalog_entry()
        test.create_zarr_entry()

        reader1 = Reader(model=drop_arguments["model"], exp=drop_arguments["exp"], source='lra-r100-monthly')
        reader2 = Reader(model=drop_arguments["model"], exp=drop_arguments["exp"], source='r100-monthly-zarr')

        data1 = reader1.retrieve()
        data2 = reader2.retrieve()
        assert data1.equals(data2)
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_dask_overwrite(self, drop_arguments, tmp_path):
        """Test DROP with overwrite=True and Dask initialization."""
        test = Drop(
            catalog='ci', **drop_arguments, tmpdir=str(tmp_path),
            resolution='r100', frequency='monthly', nproc=4,
            loglevel=LOGLEVEL, definitive=True, overwrite=True
        )

        test.retrieve()
        test.drop_generator()
        assert os.path.isdir(os.path.join(os.getcwd(), drop_arguments["outdir"], DROP_PATH))
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_exclude_incomplete(self, drop_arguments, tmp_path):
        """Test DROP's exclude_incomplete option."""
        test = Drop(
            catalog='ci', **drop_arguments, tmpdir=str(tmp_path),
            resolution='r100', frequency='monthly', definitive=True,
            loglevel=LOGLEVEL, exclude_incomplete=True
        )

        test.retrieve()
        test.drop_generator()

        missing_file = os.path.join(os.getcwd(), drop_arguments["outdir"], DROP_PATH, "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202008.nc")
        existing_file = os.path.join(os.getcwd(), drop_arguments["outdir"], DROP_PATH, "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202002.nc")

        assert not os.path.exists(missing_file)
        assert os.path.exists(existing_file)

        file = xr.open_dataset(existing_file)
        assert len(file.time) == 1
        test.check_integrity(varname=drop_arguments["var"])
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_concat_var_year(self, drop_arguments, tmp_path):
        """Test concatenation of monthly files into a single yearly file."""
        resolution = 'r100'
        frequency = 'monthly'
        year = 2022

        test = Drop(
            catalog='ci', **drop_arguments, tmpdir=str(tmp_path),
            resolution=resolution, frequency=frequency, loglevel=LOGLEVEL
        )

        for month in range(1, 13):
            mm = f'{month:02d}'
            filename = test.get_filename(drop_arguments["var"], year, month=mm)
            timeobj = pd.Timestamp(f'{year}-{mm}-01')
            ds = xr.Dataset({drop_arguments["var"]: xr.DataArray([0], dims=['time'], coords={'time': [timeobj]})})
            ds.to_netcdf(filename)

        test._concat_var_year(drop_arguments["var"], year)
        outfile = test.get_filename(drop_arguments["var"], year)

        assert os.path.exists(outfile)
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))

    def test_concat_var_year_cdo(self, drop_arguments, tmp_path):
        """Test concatenation of monthly files into a single yearly file using cdo."""
        resolution = 'r100'
        frequency = 'monthly'
        year = 2022

        test = Drop(
            catalog='ci', **drop_arguments, tmpdir=str(tmp_path), compact="cdo",
            resolution=resolution, frequency=frequency, loglevel=LOGLEVEL
        )

        for month in range(1, 13):
            mm = f'{month:02d}'
            filename = test.get_filename(drop_arguments["var"], year, month=mm)
            timeobj = pd.Timestamp(f'{year}-{mm}-01')
            ds = xr.Dataset({drop_arguments["var"]: xr.DataArray([0], dims=['time'], coords={'time': [timeobj]})})
            ds.to_netcdf(filename)

        test._concat_var_year(drop_arguments["var"], year)
        outfile = test.get_filename(drop_arguments["var"], year)

        assert os.path.exists(outfile)
        shutil.rmtree(os.path.join(drop_arguments["outdir"]))