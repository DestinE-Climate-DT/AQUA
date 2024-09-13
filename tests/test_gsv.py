import pytest
import types
import xarray as xr

from dask.distributed import LocalCluster, Client

from aqua.gsv.intake_gsv import GSVSource, gsv_available
from aqua import Reader

if not gsv_available:
    pytest.skip('Skipping GSV tests: FDB5 libraries not available', allow_module_level=True)

"""Tests for GSV in AQUA. Requires FDB library installed and an FDB repository."""

# Used to create the ``GSVSource`` if no request provided.
DEFAULT_GSV_PARAMS = {'request': {
    'date': '20080101',
    'time': '1200',
    'step': "0",
    'param': '130',
    'levtype': 'pl',
    'levelist': '300'
}, 'data_start_date': '20080101T1200', 'data_end_date': '20080101T1200', 'timestep': 'h', 'timestyle': 'date'}

loglevel = 'DEBUG'


@pytest.fixture()
def gsv(request) -> GSVSource:
    """A fixture to create an instance of ``GSVSource``."""
    if not hasattr(request, 'param'):
        request = DEFAULT_GSV_PARAMS
    else:
        request = request.param
    return GSVSource(**request)


@pytest.mark.gsv
class TestGsv():
    """Pytest marked class to test GSV."""

    # Low-level tests

    def test_gsv_constructor(self) -> None:
        """Simplest test, to check that we can create it correctly."""
        print(DEFAULT_GSV_PARAMS['request'])
        source = GSVSource(DEFAULT_GSV_PARAMS['request'], "20080101", "20080101", timestep="h",
                           chunks="S", var='167', metadata=None)
        assert source is not None

    @pytest.mark.parametrize('gsv', [{'request': {
        'domain': 'g',
        'stream': 'oper',
        'class': 'ea',
        'type': 'an',
        'expver': '0001',
        'param': '130',
        'levtype': 'pl',
        'levelist': ['1000'],
        'date': '20080101',
        'time': '1200',
        'step': '0'
        },
        'data_start_date': '20080101T1200', 'data_end_date': '20080101T1200',
        'timestep': 'h', 'timestyle': 'date', 'var': 130}], indirect=True)
    def test_gsv_read_chunked(self, gsv: GSVSource) -> None:
        """Test that the ``GSVSource`` is able to read data from FDB."""
        data = gsv.read_chunked()
        dd = next(data)
        assert len(dd) > 0, 'GSVSource could not load data'
        assert dd.t.GRIB_paramId == 130, 'Wrong GRIB param in Dask data'

    # High-level, integrated test
    def test_reader(self) -> None:
        """Simple test, to check that catalog access works and reads correctly"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", chunks="D",
                        stream_generator=True, loglevel=loglevel)
        data = reader.retrieve(startdate='20080101T1200', enddate='20080101T1200', var='t')
        assert isinstance(data, types.GeneratorType), 'Reader does not return iterator'
        dd = next(data)
        assert dd.t.GRIB_paramId == 130, 'Wrong GRIB param in data'

    def test_reader_novar(self) -> None:
        """Simple test, to check that catalog access works and reads correctly, no var"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb",
                        stream_generator=True, loglevel=loglevel)
        data = reader.retrieve()
        dd = next(data)
        assert dd.t.GRIB_paramId == 130, 'Wrong GRIB param in data'

    def test_reader_xarray(self) -> None:
        """Reading directly into xarray"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=loglevel)
        data = reader.retrieve()
        assert isinstance(data, xr.Dataset), "Does not return a Dataset"
        assert data.t.mean().data == pytest.approx(279.3509), "Field values incorrect"

    def test_reader_paramid(self) -> None:
        """
        Reading with the variable paramid, we use '130' instead of 't'
        """

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=loglevel)
        data = reader.retrieve(var='130')
        assert isinstance(data, xr.Dataset), "Does not return a Dataset"
        assert data.t.mean().data == pytest.approx(279.3509), "Field values incorrect"
        data = reader.retrieve(var=130)  # test numeric argument
        assert data.t.mean().data == pytest.approx(279.3509), "Field values incorrect"

    def test_reader_3d(self) -> None:
        """Testing 3D access"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-levels", loglevel=loglevel)
        data = reader.retrieve()
        # coordinates read from levels key
        assert all(data.t.coords["plev"].data == [99999., 89999., 79999.]), "Wrong coordinates from levels metadata key"
        # can read second level
        assert data.t.isel(plev=1).mean().values == pytest.approx(274.79095), "Field values incorrect"

        data = reader.retrieve(level=[900, 800])  # Read only two levels
        assert data.t.isel(plev=1).mean().values == pytest.approx(271.2092), "Field values incorrect"

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-nolevels", loglevel=loglevel)
        data = reader.retrieve()
        # coordinates read from levels key
        assert all(data.t.coords["plev"].data == [100000, 90000, 80000]), "Wrong level info"
        # can read second level
        assert data.t.isel(plev=1).mean().values == pytest.approx(274.79095), "Field values incorrect"

    def test_reader_3d_chunks(self) -> None:
        """Testing 3D access with vertical chunking"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-levels-chunks", loglevel=loglevel)
        data = reader.retrieve()

        # can read second level
        assert data.t.isel(plev=1).mean().values == pytest.approx(274.79095), "Field values incorrect"

        data = reader.retrieve(level=[900, 800])  # Read only two levels
        assert data.t.isel(plev=1).mean().values == pytest.approx(271.2092), "Field values incorrect"

    def test_reader_auto(self) -> None:
        """
        Reading from a datasource using new operational schema and auto dates
        """

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-auto", loglevel=loglevel)
        data = reader.retrieve()
        # Test if the correct dates have been found
        assert "1990-01-01T00:00" in str(data.time[0].values)
        assert "1990-01-01T23:00" in str(data.time[-1].values)
        # Test if the data can actually be read and contain the expected values
        assert data.tcc.isel(time=0).values.mean() == pytest.approx(0.6530221138649116)
        assert data.tcc.isel(time=-1).values.mean() == pytest.approx(0.6679689864974151)

    def test_reader_dask(self) -> None:
        """
        Reading in parallel with a dask cluster
        """

        cluster = LocalCluster(threads_per_worker=1, n_workers=2)
        client = Client(cluster)

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-auto", loglevel=loglevel)
        data = reader.retrieve()
        # Test if the correct dates have been found
        assert "1990-01-01T00:00" in str(data.time[0].values)
        assert "1990-01-01T23:00" in str(data.time[-1].values)
        # Test if the data can actually be read and contain the expected values
        assert data.tcc.isel(time=0).values.mean() == pytest.approx(0.6530221138649116)
        assert data.tcc.isel(time=-1).values.mean() == pytest.approx(0.6679689864974151)

        client.shutdown()
        cluster.close()
