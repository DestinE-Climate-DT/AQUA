import pytest
import types

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
}, 'data_start_date': '20080101:1200', 'data_end_date': '20080101:1200', 'timestep': 'H', 'timestyle': 'date'}


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
        source = GSVSource(DEFAULT_GSV_PARAMS['request'], "20080101", "20080101", timestep="H", aggregation="S", var='167', metadata=None)
        assert source is not None

    def test_gsv_get_schema(self, gsv: GSVSource) -> None:
        """
        Test the initial schema.
        """
        schema = gsv._get_schema()
        assert schema.datashape is None
        assert schema.npartitions == 1

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
        'data_start_date': '20080101:1200', 'data_end_date': '20080101:1200',
        'timestep': 'H', 'timestyle': 'date', 'var': 130, 'verbose': True}], indirect=True)
    def test_gsv_read_chunked(self, gsv: GSVSource) -> None:
        """Test that the ``GSVSource`` is able to read data from FDB."""
        data = gsv.read()
        assert len(data) > 0, 'GSVSource could not load data'
        assert data.t.param == '130.128', 'Wrong GRIB param in Dask data'

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
        'data_start_date': '20080101:1200', 'data_end_date': '20080101:1200',
        'timestep': 'H', 'timestyle': 'date', 'var': '130',
        'startdate': '20080101:1200', 'enddate': '20080101:1200', 'verbose': True}], indirect=True)
    def test_gsv_read(self, gsv: GSVSource):
        """Test that the chunked data is returned correctly too.

        Uses ``startdate`` and ``enddate`` too.

        Note that we do not have enough data in the test FDB to do
        a real test with Dask.
        """
        dask_data = list(gsv.to_dask())
        assert len(dask_data) > 0, 'The dask data returned was empty'
        xarray_dataset = dask_data[0]
        assert xarray_dataset.t.param == '130.128', 'Wrong GRIB param in Dask data'

    # High-level, integrated test
    def test_reader(self) -> None:
        """Simple test, to check that catalog access works and reads correctly"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", aggregation="D")
        data = reader.retrieve(startdate='20080101:1200', enddate='20080101:1200', var='t')
        assert isinstance(data, types.GeneratorType), 'Reader does not return iterator'
        dd = next(data)
        assert dd.t.param == '130.128', 'Wrong GRIB param in data'

    def test_reader_novar(self) -> None:
        """Simple test, to check that catalog access works and reads correctly, no var"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb")
        data = reader.retrieve()
        dd = next(data)
        assert dd.t.param == '130.128', 'Wrong GRIB param in data'