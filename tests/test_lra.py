import os
import pytest
import shutil
import xarray as xr
import pandas as pd
from aqua import LRAgenerator, Reader
from aqua.lra_generator.output_path_builder import OutputPathBuilder
from aqua.lra_generator.catalog_entry_builder import CatalogEntryBuilder   

LOGLEVEL = "DEBUG"
LRAPATH = 'ci/IFS/test-tco79/r1/r100/monthly/mean/global'
LRAPATH_DAILY = 'ci/IFS/test-tco79/r1/r100/daily/mean/europe'


@pytest.fixture(params=[{"model": "IFS", "exp": "test-tco79", "source": "long", "var": "2t", "outdir": "lra_test"}])
def lra_arguments(request):
    """Provides LRA generator arguments as a dictionary."""
    return request.param


@pytest.mark.aqua
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
    def test_build_path(self, lra_arguments, resolution, frequency, realization, region, stat, expected):
        """Test building output path."""
        args = lra_arguments
        builder = OutputPathBuilder(
            catalog='ci', model=args["model"], exp=args["exp"],
            resolution=resolution,
            frequency=frequency, realization=realization, stat=stat, region=region)
        path = builder.build_path(
            os.path.join(os.getcwd(), args['outdir']),
            var=args['var'], year=2020, month=1)
        
        if not expected:
            lrapath = f'ci/IFS/test-tco79/{realization}/{resolution}/{frequency}/{stat}/{region}'
            expected = os.path.join(os.getcwd(), args["outdir"], lrapath,
                                         f"2t_ci_IFS_test-tco79_{realization}_{resolution}_{frequency}_{stat}_{region}_202001.nc")
        else:
            expected = os.path.join(os.getcwd(), args["outdir"], expected)
    
        assert path == expected

@pytest.mark.aqua
class TestCatalogEntryBuilder:
    """Class containing tests for CatalogEntryBuilder."""

    @pytest.mark.parametrize("resolution, frequency, realization, region, stat", [
        ('r100', 'monthly', 'r1', 'global', 'mean'),
        ('r200', 'daily', 'r1', 'global', 'mean'),
      ])
    def test_create_entry_name(self, lra_arguments, resolution, frequency, realization, region, stat):
        """Test creation of entry name."""
        args = lra_arguments
        builder = CatalogEntryBuilder(
            catalog='ci', model=args["model"], exp=args["exp"],
            resolution=resolution, frequency=frequency, realization=realization,
            stat=stat, region=region, loglevel=LOGLEVEL
        )
        entry_name = builder.create_entry_name()
        block = builder.create_entry_details(basedir=args["outdir"], source_grid_name='lon-lat')
        assert entry_name == f'lra-{resolution}-{frequency}'
        assert block['driver'] == 'netcdf'
        assert block['parameters'].keys() == {'realization', 'stat', 'region'}

        builder2 = CatalogEntryBuilder(
            catalog='ci', model=args["model"], exp=args["exp"],
            resolution=resolution, frequency=frequency, realization='r2',
            stat=stat, region=region, loglevel=LOGLEVEL
        )
        newblock = builder2.create_entry_details(basedir=args["outdir"], catblock=block, source_grid_name='lon-lat')
        assert newblock['args']['urlpath'] == block['args']['urlpath']
        assert newblock['parameters']['realization']['allowed'] == ['r1','r2']



@pytest.mark.aqua
class TestLRA:
    """Class containing LRA tests."""

    def test_definitive_false(self, lra_arguments, tmp_path):
        """Test LRA generator with definitive=False."""
        test = LRAgenerator(
            catalog='ci', **lra_arguments, tmpdir=str(tmp_path),
            resolution='r100', frequency='monthly', loglevel=LOGLEVEL
        )

        test.retrieve()
        test.generate_lra()
        assert os.path.isdir(os.path.join(os.getcwd(), lra_arguments["outdir"], LRAPATH))
        shutil.rmtree(os.path.join(lra_arguments["outdir"]))

    @pytest.mark.parametrize("nworkers", [1, 2])
    def test_definitive_true(self, lra_arguments, tmp_path, nworkers):
        test = LRAgenerator(
            catalog='ci', tmpdir=str(tmp_path), **lra_arguments, nproc=nworkers,
            resolution='r100', frequency='monthly', definitive=True,
            loglevel=LOGLEVEL
        )

        test.retrieve()
        test.data = test.data.sel(time="2020-01")
        test.generate_lra()

        file_path = os.path.join(os.getcwd(), lra_arguments["outdir"], LRAPATH, "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202001.nc")
        test.check_integrity(varname=lra_arguments["var"])
        assert os.path.isfile(file_path)

        file = xr.open_dataset(file_path)
        assert len(file.time) == 1
        assert pytest.approx(file['2t'][0, 1, 1].item()) == 248.0704
        shutil.rmtree(os.path.join(lra_arguments["outdir"]))

    def test_regional_subset(self, lra_arguments, tmp_path):
        """Test LRA generator with regional subset."""
        args = lra_arguments
        region = {'name': 'europe', 'lon': [-10, 30], 'lat': [35, 70]}

        test = LRAgenerator(
            catalog='ci', model=args["model"], exp=args["exp"], source=args["source"],
            var=args["var"], outdir=args["outdir"], tmpdir=str(tmp_path),
            resolution='r100', frequency='daily', definitive=True,
            loglevel=LOGLEVEL, region=region
        )

        test.retrieve()
        test.data = test.data.sel(time="2020-01-20")
        test.generate_lra()

        file_path = os.path.join(os.getcwd(), args["outdir"], LRAPATH_DAILY, "2t_ci_IFS_test-tco79_r1_r100_daily_mean_europe_202001.nc")
        assert os.path.isfile(file_path), "File not found: {}".format(file_path)

        xfield = xr.open_dataset(file_path).where(lambda x: x.notnull(), drop=True)
        assert xfield.lat.min() > 35
        assert xfield.lat.max() < 70
        shutil.rmtree(os.path.join(args["outdir"]))

    def test_zarr_entry(self, lra_arguments, tmp_path):
        """Test LRA generator with Zarr archive creation."""
        args = lra_arguments
        test = LRAgenerator(
            catalog='ci', model=args["model"], exp=args["exp"], source=args["source"],
            var=args["var"], outdir=args["outdir"], tmpdir=str(tmp_path),
            resolution='r100', frequency='monthly', nproc=1,
            loglevel=LOGLEVEL, definitive=True
        )

        test.retrieve()
        test.generate_lra()
        test.create_catalog_entry()
        test.create_zarr_entry()

        reader1 = Reader(model=args["model"], exp=args["exp"], source='lra-r100-monthly')
        reader2 = Reader(model=args["model"], exp=args["exp"], source='lra-r100-monthly-zarr')

        data1 = reader1.retrieve()
        data2 = reader2.retrieve()
        assert data1.equals(data2)
        shutil.rmtree(os.path.join(args["outdir"]))

    def test_dask_overwrite(self, lra_arguments, tmp_path):
        """Test LRA generator with overwrite=True and Dask initialization."""
        args = lra_arguments
        test = LRAgenerator(
            catalog='ci', model=args["model"], exp=args["exp"], source=args["source"],
            var=args["var"], outdir=args["outdir"], tmpdir=str(tmp_path),
            resolution='r100', frequency='monthly', nproc=4,
            loglevel=LOGLEVEL, definitive=True, overwrite=True
        )

        test.retrieve()
        test.generate_lra()
        assert os.path.isdir(os.path.join(os.getcwd(), args["outdir"], LRAPATH))
        shutil.rmtree(os.path.join(args["outdir"]))

    def test_exclude_incomplete(self, lra_arguments, tmp_path):
        """Test LRA generator's exclude_incomplete option."""
        args = lra_arguments
        test = LRAgenerator(
            catalog='ci', model=args["model"], exp=args["exp"], source=args["source"],
            var=args["var"], outdir=args["outdir"], tmpdir=str(tmp_path),
            resolution='r100', frequency='monthly', definitive=True,
            loglevel=LOGLEVEL, exclude_incomplete=True
        )

        test.retrieve()
        test.generate_lra()

        missing_file = os.path.join(os.getcwd(), args["outdir"], LRAPATH, "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202008.nc")
        existing_file = os.path.join(os.getcwd(), args["outdir"], LRAPATH, "2t_ci_IFS_test-tco79_r1_r100_monthly_mean_global_202002.nc")

        assert not os.path.exists(missing_file)
        assert os.path.exists(existing_file)

        file = xr.open_dataset(existing_file)
        assert len(file.time) == 1
        test.check_integrity(varname=args["var"])
        shutil.rmtree(os.path.join(args["outdir"]))

    def test_concat_var_year(self, lra_arguments, tmp_path):
        """Test concatenation of monthly files into a single yearly file."""
        args = lra_arguments
        resolution = 'r100'
        frequency = 'monthly'
        year = 2022

        test = LRAgenerator(
            catalog='ci', model=args["model"], exp=args["exp"], source=args["source"],
            var=args["var"], outdir=args["outdir"], tmpdir=str(tmp_path),
            resolution=resolution, frequency=frequency, loglevel=LOGLEVEL
        )

        for month in range(1, 13):
            mm = f'{month:02d}'
            filename = test.get_filename(args["var"], year, month=mm)
            timeobj = pd.Timestamp(f'{year}-{mm}-01')
            ds = xr.Dataset({args["var"]: xr.DataArray([0], dims=['time'], coords={'time': [timeobj]})})
            ds.to_netcdf(filename)

        test._concat_var_year(args["var"], year)
        outfile = test.get_filename(args["var"], year)

        assert os.path.exists(outfile)
        shutil.rmtree(os.path.join(args["outdir"]))

    def test_concat_var_year_cdo(self, lra_arguments, tmp_path):
        """Test concatenation of monthly files into a single yearly file using cdo."""
        args = lra_arguments
        resolution = 'r100'
        frequency = 'monthly'
        year = 2022

        test = LRAgenerator(
            catalog='ci', model=args["model"], exp=args["exp"], source=args["source"],
            var=args["var"], outdir=args["outdir"], tmpdir=str(tmp_path), compact="cdo",
            resolution=resolution, frequency=frequency, loglevel=LOGLEVEL
        )

        for month in range(1, 13):
            mm = f'{month:02d}'
            filename = test.get_filename(args["var"], year, month=mm)
            timeobj = pd.Timestamp(f'{year}-{mm}-01')
            ds = xr.Dataset({args["var"]: xr.DataArray([0], dims=['time'], coords={'time': [timeobj]})})
            ds.to_netcdf(filename)

        test._concat_var_year(args["var"], year)
        outfile = test.get_filename(args["var"], year)

        assert os.path.exists(outfile)
        shutil.rmtree(os.path.join(args["outdir"]))