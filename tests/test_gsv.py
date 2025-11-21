import pytest
import xarray as xr

from dask.distributed import LocalCluster, Client

from aqua.core.gsv.intake_gsv import GSVSource, gsv_available
from aqua.core.configurer import ConfigPath
from aqua import Reader
from conftest import LOGLEVEL

if not gsv_available:
    pytest.skip('Skipping GSV tests: FDB5 libraries not available', allow_module_level=True)

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
pytestmark = [
    pytest.mark.gsv,
    pytest.mark.xdist_group(name="dask_operations")
]

"""Tests for GSV in AQUA. Requires FDB library installed and an FDB repository."""

# Used to create the ``GSVSource`` if no request provided.
DEFAULT_GSV_PARAMS = {
    'request': {
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
    'data_start_date': '20080101T1200', 
    'data_end_date': '20080101T1200', 
    'timestep': 'h', 
    'timestyle': 'date'
}

loglevel = LOGLEVEL
FDB_HOME = '/app'

# to enable for local testing on Lumi
if ConfigPath().machine == 'lumi':
    FDB_HOME = '/pfs/lustrep3/projappl/project_465000454/padavini/FDB-TEST'


@pytest.fixture()
def gsv(request) -> GSVSource:
    """A fixture to create an instance of ``GSVSource``."""
    if not hasattr(request, 'param'):
        request = DEFAULT_GSV_PARAMS
    else:
        request = request.param
    return GSVSource(**request, metadata={'fdb_home': FDB_HOME})


class TestGsv():
    """Pytest marked class to test GSV."""

    # Low-level tests
    def test_gsv_constructor(self) -> None:
        """Simplest test, to check that we can create it correctly."""
        print(DEFAULT_GSV_PARAMS['request'])
        source = GSVSource(DEFAULT_GSV_PARAMS['request'], "20080101", "20080101", timestep="h",
                           chunks="S", var='167', metadata={'fdb_home': FDB_HOME})
        assert source is not None

    def test_gsv_constructor_bridge(self) -> None:
        """Test bridge"""
        print(DEFAULT_GSV_PARAMS['request'])
        source = GSVSource(DEFAULT_GSV_PARAMS['request'], "20080101", "20080101", timestep="h",
                           chunks="S", var='167', bridge_end_date='complete',
                           metadata={'fdb_home_bridge': FDB_HOME})
        assert source is not None
            
    def test_gsv_constructor_raise(self) -> None:
        """Test raise for missing fdbhome"""
        print(DEFAULT_GSV_PARAMS['request'])
        with pytest.raises(ValueError):
            GSVSource(DEFAULT_GSV_PARAMS['request'], "20080101", "20080101", timestep="h",
                           chunks="S", var='167')
    
    def test_gsv_constructor_raise_bridge(self) -> None:
        """Test raise for missing fdbhome"""
        print(DEFAULT_GSV_PARAMS['request'])
        with pytest.raises(ValueError):
            GSVSource(DEFAULT_GSV_PARAMS['request'], "20080101", "20080101", timestep="h",
                           chunks="S", var='167', bridge_end_date='complete')

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

    # High-level, integrated test
    def test_reader(self) -> None:
        """Simple test, to check that catalog access works and reads correctly"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=loglevel)
        data = reader.retrieve(startdate='20080101T1200', enddate='20080101T1200', var='t')
        assert data.t.GRIB_paramId == 130, 'Wrong GRIB param in data'

    def test_reader_novar(self) -> None:
        """Simple test, to check that catalog access works and reads correctly, no var"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=loglevel)
        data = reader.retrieve()
        assert data.t.GRIB_paramId == 130, 'Wrong GRIB param in data'

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

    def test_reader_bridge(self) -> None:
        """
        Reading from a datasource using bridge
        """

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-bridge", loglevel=loglevel)
        data = reader.retrieve()
        # Test if the correct dates have been found
        assert "1990-01-01T00:00" in str(data.time[0].values)
        assert "1990-01-02T00:00" in str(data.time[-1].values)
        # Test if the data can actually be read and contain the expected values
        assert data.tcc.isel(time=0).values.mean() == pytest.approx(65.30221138649116)  # This is from HPC
        assert data.tcc.isel(time=15).values.mean() == pytest.approx(65.62109108718757)  # This is from the bridge
        assert data.tcc.isel(time=-1).values.mean() == pytest.approx(66.87973267265382)  # This is from HPC again

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
        assert data.tcc.isel(time=0).values.mean() == pytest.approx(65.30221138649116)
        assert data.tcc.isel(time=-1).values.mean() == pytest.approx(66.79689864974151)

    def test_reader_polytope(self) -> None:
        """
        Reading from a remote databridge using polytope
        """

        reader = Reader(catalog='climatedt-phase1', model="IFS-NEMO", exp="ssp370", source="hourly-hpz7-atm2d",
                        startdate="20210101T0000", enddate="20210101T2300", loglevel="debug", engine="polytope",
                        chunks='h')
        data = reader.retrieve(var='2t')
        assert data.isel(time=1)['2t'].mean().values == pytest.approx(285.8661045)

    def test_reader_stac_polytope(self) -> None:
       """
       Reading from a remote databridge using polytope
       """
    
       reader = Reader(catalog='climatedt-phase1', model="IFS-FESOM", exp="story-2017-control", source="hourly-hpz7-atm2d",
                       loglevel="debug", engine="polytope", areas=False)
       data = reader.retrieve(var='2t')
       assert data.isel(time=20)['2t'].mean().values == pytest.approx(285.52128)

    def test_fdb_from_file(self) -> None:
        """
        Reading fdb dates from a file.
        First test with a file that contains both data and bridge dates.
        Second test with a file that contains only data dates.
        """
        source = GSVSource(DEFAULT_GSV_PARAMS['request'],  "20080101", "20080101",
                           metadata={'fdb_home': FDB_HOME, 'fdb_home_bridge': FDB_HOME,
                                     'fdb_info_file': 'tests/catgen/fdb_info_file.yaml'},
                           loglevel=loglevel)

        assert source.data_start_date == '19900101T0000'
        assert source.data_end_date == '19900103T2300'
        assert source.bridge_start_date == '19900101T0000'
        assert source.bridge_end_date == '19900102T2300'

        source = GSVSource(DEFAULT_GSV_PARAMS['request'],  "20080101", "20080101",
                           metadata={'fdb_home': FDB_HOME, 'fdb_home_bridge': FDB_HOME,
                                     'fdb_info_file': 'tests/catgen/fdb_info_hpc-only.yaml'},
                            loglevel=loglevel)
        
        assert source.data_start_date == '19900101T0000'
        assert source.data_end_date == '19900103T2300'
    
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
        assert data.tcc.isel(time=0).values.mean() == pytest.approx(65.30221138649116)
        assert data.tcc.isel(time=-1).values.mean() == pytest.approx(66.79689864974151)

        client.shutdown()
        cluster.close()

# Additional tests for the GSVSource class
def test_fdb_home_bridge_logs(capsys):
    # Prepare test metadata ensuring we have fdbhome_bridge
    metadata = {
        'fdb_home_bridge': FDB_HOME,
        'fdb_home': FDB_HOME
    }

    source = GSVSource(DEFAULT_GSV_PARAMS['request'], data_start_date='20080101T1200', data_end_date='20080101T1200',
                        metadata=metadata, loglevel=loglevel)

    # No assert in the following because we cannot check the stderr logs. This is just for coverage.

    source.chk_type = [1]  # Force chunk type to be bridge
    source._get_partition(ii=0)

    source.chk_type = [0]
    source._get_partition(ii=0)

    metadata = {
        'fdb_path_bridge': FDB_HOME+'/etc/fdb/config.yaml',
        'fdb_path': FDB_HOME+'/etc/fdb/config.yaml'
    }
    source = GSVSource(DEFAULT_GSV_PARAMS['request'], data_start_date='20080101T1200', data_end_date='20080101T1200',
                        metadata=metadata, loglevel=loglevel)
    
    source.chk_type = [1]
    source._get_partition(ii=0)

    source.chk_type = [0]
    source._get_partition(ii=0)
