import os, shutil
import pytest
import xarray as xr
from aqua import OPAgenerator, Reader

@pytest.fixture(
    params=[("IFS", "test-tco79", "long", "2t", "onepass_test", "onepass_test")]
)

def opa_arguments(request):
    return request.param

@pytest.mark.aqua
class TestOPA():
    """Class for OPA Tests"""

    def test_definitive_false(self, opa_arguments):
                model, exp, source, var, outdir, tmpdir = opa_arguments
                opaopa = OPAgenerator(model=model, exp=exp, source=source, var=var, frequency='daily',
                outdir=outdir, tmpdir=tmpdir, definitive=False)
                opaopa.retrieve()
                opaopa.generate_opa()
                assert os.path.isdir(os.path.join(os.getcwd(), outdir, "IFS/test-tco79/daily"))

    def test_definitive_true(self, opa_arguments):
                model, exp, source, var, outdir, tmpdir = opa_arguments
                opaopa = OPAgenerator(model=model, exp=exp, source=source, var=var, frequency='monthly',
                outdir=outdir, tmpdir=tmpdir, definitive=True)
                opaopa.retrieve()
                opaopa.generate_opa()

                path = os.path.join(os.getcwd(), outdir, "IFS/test-tco79/monthly/2020_03_2t_monthly_mean.nc")
                assert os.path.isfile(path)    
                file = xr.open_dataset(path)
                assert len(file.time) == 1
                assert pytest.approx(file['2t'][0,1,2].item()) == 262.79790
                
                opaopa.create_catalog_entry()
                assert Reader(model="IFS", exp="test-tco79", source="tmp-opa-monthly",areas=False)

                opaopa.clean()
                shutil.rmtree(os.path.join(os.getcwd(), outdir))
    
    