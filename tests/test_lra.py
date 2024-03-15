import os
import shutil
import pytest
import xarray as xr
from aqua import LRAgenerator

loglevel = "DEBUG"

@pytest.fixture(
    params=[("IFS", "test-tco79", "long", "2t", "lra_test", "tmpdir")]
)
def lra_arguments(request):
    return request.param


@pytest.mark.aqua
class TestLRA():
    """Class for LRA Tests"""

    def test_definitive_false(self, lra_arguments):
        """Test the LRA generator with definitive = False"""
        model, exp, source, var, outdir, tmpdir = lra_arguments
        test = LRAgenerator(model=model, exp=exp, source=source, var=var,
                            outdir=outdir, tmpdir=tmpdir, resolution='r100',
                            frequency='monthly', loglevel=loglevel)
        test.retrieve()
        test.generate_lra()
        assert os.path.isdir(os.path.join(os.getcwd(), outdir,
                                          "IFS/test-tco79/r100/monthly"))

    def test_definitive_true(self, lra_arguments):
        """Test the LRA generator with definitive = True"""
        model, exp, source, var, outdir, tmpdir = lra_arguments
        test = LRAgenerator(model=model, exp=exp, source=source, var=var,
                            outdir=outdir, tmpdir=tmpdir, resolution='r100',
                            frequency='monthly', definitive=True,
                            loglevel=loglevel)
        test.retrieve()

        year = test.data.sel(time=test.data.time.dt.year == 2020)
        month = year.sel(time=year.time.dt.month == 1)

        test.data = month
        test.generate_lra()

        path = os.path.join(os.getcwd(), outdir,
                            "IFS/test-tco79/r100/monthly/2t_test-tco79_r100_monthly_202001.nc")
        test.check_integrity(varname=var)
        assert os.path.isfile(path)

        file = xr.open_dataset(path)
        assert len(file.time) == 1

        assert pytest.approx(file['2t'][0, 1, 1].item()) == 248.0704
        shutil.rmtree(os.path.join(os.getcwd(), outdir))

    # test with definitive = False but with dask init and catalog generator
    def test_dask_entry(self, lra_arguments):
        """
        Test the LRA generator with definitive = False
        but with dask init and catalog generator
        """
        model, exp, source, var, outdir, tmpdir = lra_arguments
        test = LRAgenerator(model=model, exp=exp, source=source, var=var,
                            outdir=outdir, tmpdir=tmpdir,
                            resolution='r100', frequency='monthly', nproc=2,
                            loglevel=loglevel)
        test.retrieve()
        test.generate_lra()
        test.create_catalog_entry()
        assert os.path.isdir(os.path.join(os.getcwd(), outdir,
                                          "IFS/test-tco79/r100/monthly"))

        shutil.rmtree(os.path.join(os.getcwd(), tmpdir))


    def test_exclude_incomplete(self, lra_arguments):
        """Test exclude_incomplete option"""
        model, exp, source, var, outdir, tmpdir = lra_arguments
        test = LRAgenerator(model=model, exp=exp, source=source, var=var,
                            outdir=outdir, tmpdir=tmpdir, resolution='r100',
                            frequency='monthly', definitive=True, exclude_incomplete=True,
                            loglevel=loglevel)
        test.retrieve()
        test.generate_lra()

        path = os.path.join(os.getcwd(), outdir,
                            "IFS/test-tco79/r100/monthly/2t_test-tco79_r100_monthly_202008.nc")
        assert not os.path.exists(path)

        path = os.path.join(os.getcwd(), outdir,
                            "IFS/test-tco79/r100/monthly/2t_test-tco79_r100_monthly_202002.nc")
        assert os.path.exists(path)
        file = xr.open_dataset(path)
        assert len(file.time) == 1

        test.check_integrity(varname=var)
        assert os.path.isfile(path)
        shutil.rmtree(os.path.join(os.getcwd(), outdir))
