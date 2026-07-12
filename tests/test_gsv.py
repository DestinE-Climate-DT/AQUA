import pickle

import numpy as np
import pytest
import xarray as xr
from conftest import LOGLEVEL
from dask.distributed import Client, LocalCluster

from aqua import Reader
from aqua.core.configurer import ConfigPath
from aqua.core.intake_drivers.fdb.openers.gsv_source import GSVSource, gsv_available

if not gsv_available:
    pytest.skip("Skipping GSV tests: FDB5 libraries not available", allow_module_level=True)

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
pytestmark = [pytest.mark.gsv, pytest.mark.xdist_group(name="dask_operations")]

"""Tests for GSV in AQUA. Requires FDB library installed and an FDB repository."""

# Used to create the ``GSVSource`` if no request provided.
DEFAULT_GSV_PARAMS = {
    "request": {
        "domain": "g",
        "stream": "oper",
        "class": "ea",
        "type": "an",
        "expver": "0001",
        "param": "130",
        "levtype": "pl",
        "levelist": ["1000"],
        "date": "20080101",
        "time": "1200",
        "step": "0",
    },
    "data_start_date": "20080101T1200",
    "data_end_date": "20080101T1200",
    "timestep": "h",
    "timestyle": "date",
}

loglevel = LOGLEVEL
FDB_HOME = "/app"

# to enable for local testing on Lumi
if ConfigPath().machine == "lumi":
    FDB_HOME = "/pfs/lustrep3/projappl/project_465000454/padavini/FDB-TEST"


@pytest.fixture()
def gsv(request) -> GSVSource:
    """A fixture to create an instance of ``GSVSource``."""
    if not hasattr(request, "param"):
        request = DEFAULT_GSV_PARAMS
    else:
        request = request.param
    return GSVSource(**request, metadata={"fdb_home": FDB_HOME})


class TestGsv:
    """Pytest marked class to test GSV."""

    # Low-level tests
    def test_gsv_constructor(self) -> None:
        """Simplest test, to check that we can create it correctly."""
        print(DEFAULT_GSV_PARAMS["request"])
        source = GSVSource(
            DEFAULT_GSV_PARAMS["request"],
            "20080101",
            "20080101",
            timestep="h",
            chunks="S",
            var="167",
            metadata={"fdb_home": FDB_HOME},
            engine="fdb",
        )
        assert source is not None

    def test_gsv_constructor_bridge(self) -> None:
        """Test bridge"""
        print(DEFAULT_GSV_PARAMS["request"])
        source = GSVSource(
            DEFAULT_GSV_PARAMS["request"],
            "20080101",
            "20080101",
            timestep="h",
            chunks="S",
            var="167",
            bridge_end_date="complete",
            metadata={"fdb_home_bridge": FDB_HOME},
            engine="fdb",
        )
        assert source is not None

    def test_gsv_constructor_raise(self) -> None:
        """Test raise for missing fdbhome"""
        print(DEFAULT_GSV_PARAMS["request"])
        with pytest.raises(ValueError):
            GSVSource(DEFAULT_GSV_PARAMS["request"], "20080101", "20080101", timestep="h", chunks="S", var="167", engine="fdb")

    def test_gsv_constructor_raise_bridge(self) -> None:
        """Test raise for missing fdbhome"""
        print(DEFAULT_GSV_PARAMS["request"])
        with pytest.raises(ValueError):
            GSVSource(
                DEFAULT_GSV_PARAMS["request"],
                "20080101",
                "20080101",
                timestep="h",
                chunks="S",
                var="167",
                bridge_end_date="complete",
                engine="fdb",
            )

    def test_gsv_constructor_raise_path_not_exists(self) -> None:
        """Test raise when fdbhome path is specified but does not exist"""
        print(DEFAULT_GSV_PARAMS["request"])
        with pytest.raises(FileNotFoundError, match="fdbhome path .* does not exist"):
            GSVSource(
                DEFAULT_GSV_PARAMS["request"],
                "20080101",
                "20080101",
                timestep="h",
                chunks="S",
                var="167",
                engine="fdb",
                metadata={"fdb_home": "/path/that/does/not/exist"},
            )

    def test_gsv_constructor_raise_bridge_path_not_exists(self) -> None:
        """Test raise when bridge path is specified but does not exist"""
        print(DEFAULT_GSV_PARAMS["request"])
        with pytest.raises(FileNotFoundError, match="fdbhome_bridge path .* does not exist"):
            GSVSource(
                DEFAULT_GSV_PARAMS["request"],
                "20080101",
                "20080101",
                timestep="h",
                chunks="S",
                var="167",
                bridge_end_date="complete",
                engine="fdb",
                metadata={"fdb_home_bridge": "/path/that/does/not/exist"},
            )

    @pytest.mark.parametrize(
        "gsv",
        [
            {
                "request": {
                    "domain": "g",
                    "stream": "oper",
                    "class": "ea",
                    "type": "an",
                    "expver": "0001",
                    "param": "130",
                    "levtype": "pl",
                    "levelist": ["1000"],
                    "date": "20080101",
                    "time": "1200",
                    "step": "0",
                },
                "data_start_date": "20080101T1200",
                "data_end_date": "20080101T1200",
                "timestep": "h",
                "timestyle": "date",
                "var": 130,
            }
        ],
        indirect=True,
    )
    def test_gsv_read_chunked(self, gsv: GSVSource) -> None:
        """Test that the ``GSVSource`` is able to read data from FDB."""
        data = gsv.read_chunked()
        dd = next(data)
        assert len(dd) > 0, "GSVSource could not load data"

    # High-level, integrated test

    # z3fdb does not use GRIB_paramId, so no need to run it with this test
    def test_reader(self) -> None:
        """Simple test, to check that catalog access works and reads correctly"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=loglevel)
        data = reader.retrieve(startdate="20080101T1200", enddate="20080101T1200", var="t")
        assert data.t.GRIB_paramId == 130, "Wrong GRIB param in data"

    def test_reader_novar(self) -> None:
        """Simple test, to check that catalog access works and reads correctly, no var"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=loglevel)
        data = reader.retrieve()
        assert data.t.GRIB_paramId == 130, "Wrong GRIB param in data"

    @pytest.mark.parametrize("engine", ["fdb", "z3fdb"])
    def test_reader_xarray(self, engine) -> None:
        """Reading directly into xarray"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=loglevel, engine=engine)
        data = reader.retrieve()
        assert isinstance(data, xr.Dataset), "Does not return a Dataset"
        assert data.t.mean().data == pytest.approx(279.3509), "Field values incorrect"

    def test_reader_paramid(self) -> None:
        """
        Reading with the variable paramid, we use '130' instead of 't'
        """

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=loglevel)
        data = reader.retrieve(var="130")
        assert isinstance(data, xr.Dataset), "Does not return a Dataset"
        assert data.t.mean().data == pytest.approx(279.3509), "Field values incorrect"
        data = reader.retrieve(var=130)  # test numeric argument
        assert data.t.mean().data == pytest.approx(279.3509), "Field values incorrect"

    # z3fdb does not convert codes to shortnames
    def test_reader_paramid_z3fdb(self) -> None:
        """
        Reading with the variable paramid, we use '130' instead of 't'
        """

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=loglevel, engine="z3fdb")
        data = reader.retrieve(var="130")
        assert isinstance(data, xr.Dataset), "Does not return a Dataset"
        assert data.var130.mean().data == pytest.approx(279.3509), "Field values incorrect"
        data = reader.retrieve(var=130)  # test numeric argument
        assert data.var130.mean().data == pytest.approx(279.3509), "Field values incorrect"

    @pytest.mark.parametrize("engine", ["fdb", "z3fdb"])
    def test_reader_3d(self, engine) -> None:
        """Testing 3D access"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-levels", loglevel=loglevel, engine=engine)
        data = reader.retrieve()
        # coordinates read from levels key
        assert all(data.t.coords["plev"].data == [99999.0, 89999.0, 79999.0]), "Wrong coordinates from levels metadata key"
        # can read second level
        assert data.t.isel(plev=1).mean().values == pytest.approx(274.79095), "Field values incorrect"

        data = reader.retrieve(level=[900, 800])  # Read only two levels
        assert data.t.isel(plev=1).mean().values == pytest.approx(271.2092), "Field values incorrect"

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-nolevels", loglevel=loglevel, engine=engine)
        data = reader.retrieve()
        # coordinates read from levels key
        assert all(data.t.coords["plev"].data == [100000, 90000, 80000]), "Wrong level info"
        # can read second level
        assert data.t.isel(plev=1).mean().values == pytest.approx(274.79095), "Field values incorrect"

    @pytest.mark.parametrize("engine", ["fdb", "z3fdb"])
    def test_reader_3d_chunks(self, engine) -> None:
        """Testing 3D access with vertical chunking"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-levels-chunks", loglevel=loglevel, engine=engine)
        data = reader.retrieve()

        # can read second level
        assert data.t.isel(plev=1).mean().values == pytest.approx(274.79095), "Field values incorrect"

        data = reader.retrieve(level=[900, 800])  # Read only two levels
        assert data.t.isel(plev=1).mean().values == pytest.approx(271.2092), "Field values incorrect"

    @pytest.mark.parametrize("engine", ["fdb", "z3fdb"])
    def test_reader_bridge(self, engine) -> None:
        """
        Reading from a datasource using bridge
        """

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-bridge", loglevel=loglevel, engine=engine)
        data = reader.retrieve()
        # Test if the correct dates have been found
        assert "1990-01-01T00:00" in str(data.time[0].values)
        assert "1990-01-02T00:00" in str(data.time[-1].values)
        # Test if the data can actually be read and contain the expected values
        assert data.tcc.isel(time=0).values.mean() == pytest.approx(65.30221138649116)  # This is from HPC
        assert data.tcc.isel(time=15).values.mean() == pytest.approx(65.62109108718757)  # This is from the bridge
        assert data.tcc.isel(time=-1).values.mean() == pytest.approx(66.87973267265382)  # This is from HPC again

    # The auto option is not implemented for z3fdb
    def test_reader_auto(self) -> None:
        """
        Reading from a datasource using new operational schema and auto dates
        """

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-auto", loglevel=loglevel, engine="fdb")
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

        reader = Reader(
            catalog="climatedt-phase1",
            model="IFS-NEMO",
            exp="ssp370",
            source="hourly-hpz7-atm2d",
            startdate="20210101T0000",
            enddate="20210101T2300",
            loglevel="debug",
            engine="polytope",
            chunks="h",
        )
        data = reader.retrieve(var="2t")
        assert data.isel(time=1)["2t"].mean().values == pytest.approx(285.8661045)

    def test_reader_stac_polytope(self) -> None:
        """
        Reading from a remote databridge using polytope
        """
        reader = Reader(
            catalog="climatedt-phase1",
            model="IFS-FESOM",
            exp="story-2017-control",
            source="hourly-hpz7-atm2d",
            loglevel="debug",
            engine="polytope",
            areas=False,
        )
        data = reader.retrieve(var="2t")
        assert data.isel(time=20)["2t"].mean().values == pytest.approx(285.52128)

    def test_reader_polytope_mn5(self) -> None:
        """
        Reading from mn5 databridge using polytope
        """
        reader = Reader(
            catalog="climatedt-o25.1",
            model="IFS-NEMO",
            exp="historical-1990",
            source="hourly-hpz7-atm2d",
            startdate="19900101T0000",
            enddate="19910101T0025",
            loglevel="debug",
            engine="polytope",
            areas=False,
        )
        data = reader.retrieve(var="2t")
        assert "databridge" in reader.backend.kwargs
        assert reader.backend.kwargs["databridge"] == "mn5"
        assert data.isel(time=20)["2t"].values[0] == pytest.approx(301.0878448486328)

    def test_fdb_from_file(self) -> None:
        """
        Reading fdb dates from a file.
        First test with a file that contains both data and bridge dates.
        Second test with a file that contains only data dates.
        """
        source = GSVSource(
            DEFAULT_GSV_PARAMS["request"],
            "20080101",
            "20080101",
            metadata={"fdb_home": FDB_HOME, "fdb_home_bridge": FDB_HOME, "fdb_info_file": "tests/catgen/fdb_info_file.yaml"},
            engine="fdb",
            loglevel=loglevel,
        )

        assert source.data_start_date == "19900101T0000"
        assert source.data_end_date == "19900103T2300"
        assert source.bridge_start_date == "19900101T0000"
        assert source.bridge_end_date == "19900102T2300"

        source = GSVSource(
            DEFAULT_GSV_PARAMS["request"],
            "20080101",
            "20080101",
            metadata={
                "fdb_home": FDB_HOME,
                "fdb_home_bridge": FDB_HOME,
                "fdb_info_file": "tests/catgen/fdb_info_hpc-only.yaml",
            },
            engine="fdb",
            loglevel=loglevel,
        )

        assert source.data_start_date == "19900101T0000"
        assert source.data_end_date == "19900103T2300"

    @pytest.mark.parametrize("engine", ["fdb", "z3fdb"])
    def test_reader_dask(self, engine) -> None:
        """
        Reading in parallel with a dask cluster
        LocalCluster is created with dashboard_address=None to avoid dashboard port conflicts under pytest-xdist
        """
        with LocalCluster(threads_per_worker=1, n_workers=2, dashboard_address=None) as cluster:
            with Client(cluster):
                reader = Reader(model="IFS", exp="test-fdb", source="fdb-bridge", loglevel=loglevel, engine=engine)
                data = reader.retrieve()
                # Test if the correct dates have been found
                assert "1990-01-01T00:00" in str(data.time[0].values)
                assert "1990-01-02T00:00" in str(data.time[-1].values)
                # Test if the data can actually be read and contain the expected values
                assert data.tcc.isel(time=0).mean().compute().item() == pytest.approx(65.30221138649115)
                assert data.tcc.isel(time=-1).mean().compute().item() == pytest.approx(66.8797378540039)


# Additional tests for the GSVSource class
def test_fdb_home_bridge_logs(capsys):
    # Prepare test metadata ensuring we have fdbhome_bridge
    metadata = {"fdb_home_bridge": FDB_HOME, "fdb_home": FDB_HOME}

    source = GSVSource(
        DEFAULT_GSV_PARAMS["request"],
        data_start_date="20080101T1200",
        data_end_date="20080101T1200",
        metadata=metadata,
        loglevel=loglevel,
    )

    # No assert in the following because we cannot check the stderr logs. This is just for coverage.

    source.chk_type = [1]  # Force chunk type to be bridge
    source._get_partition(ii=0)

    source.chk_type = [0]
    source._get_partition(ii=0)

    metadata = {"fdb_path_bridge": FDB_HOME + "/etc/fdb/config.yaml", "fdb_path": FDB_HOME + "/etc/fdb/config.yaml"}
    source = GSVSource(
        DEFAULT_GSV_PARAMS["request"],
        data_start_date="20080101T1200",
        data_end_date="20080101T1200",
        metadata=metadata,
        loglevel=loglevel,
    )

    source.chk_type = [1]
    source._get_partition(ii=0)

    source.chk_type = [0]
    source._get_partition(ii=0)


CMIP6_MARS_REQ = (
    "class=d1,dataset=climate-dt,activity=CMIP6,experiment=hist,generation=1,"
    "model=IFS-NEMO,realization=1,resolution=standard,expver=a0h3,type=fc,"
    "stream=clte,date=19900101,time=0000,param=164,levtype=sfc"
)


def test_z3fdb_store_pickling() -> None:
    """Test FdbZarrStore custom pickling and unpickling."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import z3fdb_available

    if not z3fdb_available:
        pytest.skip("z3fdb not available")

    from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

    builder = SimpleStoreBuilder(None)
    axes = [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)]
    builder.add_part(CMIP6_MARS_REQ, axes, ExtractorType.GRIB)
    store = builder.build()

    # Attach serialization attributes to verify pickling
    store._config = None
    store._mars = [CMIP6_MARS_REQ]
    store._serialized_axes = [(axis.keys, axis.chunking.name) for axis in axes]
    store._extractor_type_str = ExtractorType.GRIB.name

    pickled = pickle.dumps(store)
    unpickled = pickle.loads(pickled)
    assert unpickled is not None


def test_z3fdb_rebuild_parameters() -> None:
    """Test rebuild_fdb_zarr_store parameters and path logic."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import rebuild_fdb_zarr_store, z3fdb_available

    if not z3fdb_available:
        pytest.skip("z3fdb not available")

    serialized_axes = [(["date", "time"], "SINGLE_VALUE")]
    # Test with mars as string
    store1 = rebuild_fdb_zarr_store(
        config=None, mars=CMIP6_MARS_REQ, serialized_axes=serialized_axes, extractor_type_str="GRIB"
    )
    assert store1 is not None

    # Test with config path and mars as list
    store2 = rebuild_fdb_zarr_store(
        config=FDB_HOME, mars=[CMIP6_MARS_REQ], serialized_axes=serialized_axes, extractor_type_str="GRIB"
    )
    assert store2 is not None


def test_z3fdb_missing_attrs_raise() -> None:
    """Test reduce_fdb_zarr_store raises TypeError on missing attrs."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import reduce_fdb_zarr_store, z3fdb_available

    if not z3fdb_available:
        pytest.skip("z3fdb not available")

    from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

    builder = SimpleStoreBuilder(None)
    axes = [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)]
    builder.add_part(CMIP6_MARS_REQ, axes, ExtractorType.GRIB)
    store = builder.build()

    with pytest.raises(TypeError, match="Cannot pickle FdbZarrStore: missing serialization attributes"):
        reduce_fdb_zarr_store(store)


def test_z3fdb_healpix_unstructured_coordinate_checking() -> None:
    """Test healpix unstructured detection logic."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import add_coordinates

    # ds with cell dimension of 48 (12 * 2^2) -> valid healpix count
    ds_valid = xr.Dataset(coords={"cell": np.arange(48)})
    try:
        ds_out = add_coordinates(ds_valid, grid_type="healpix_unstructured")
        assert "lon" in ds_out.coords
    except (ImportError, ModuleNotFoundError):
        pass

    # ds with invalid cell count
    ds_invalid = xr.Dataset(coords={"cell": np.arange(50)})
    ds_out = add_coordinates(ds_invalid, grid_type="healpix_unstructured")
    assert "lon" not in ds_out.coords


def test_z3fdb_lonlat_mismatch_aborts() -> None:
    """Test add_lonlat_coordinates aborts on mismatching shape."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import add_lonlat_coordinates

    ds = xr.Dataset(coords={"cell": np.arange(5)})
    ds_out = add_lonlat_coordinates(ds)
    assert ds_out.equals(ds)


def test_z3fdb_reader_before_bridge_period() -> None:
    """Test Z3FDBDatasetReader period splitting with a start date before the bridge."""
    from aqua.core.intake_drivers.fdb.datatypes import Z3FDB
    from aqua.core.intake_drivers.fdb.readers import Z3FDBDatasetReader

    metadata = {
        "fdb_home": FDB_HOME,
        "fdb_home_bridge": FDB_HOME,
    }
    data = Z3FDB(
        request={
            "class": "d1",
            "dataset": "climate-dt",
            "activity": "CMIP6",
            "experiment": "hist",
            "generation": 1,
            "model": "IFS-NEMO",
            "realization": 1,
            "resolution": "standard",
            "expver": "a0h3",
            "type": "fc",
            "stream": "clte",
            "date": 19900101,
            "time": "0000",
            "param": 164,
            "levtype": "sfc",
        },
        metadata=metadata,
        data_start_date="19900101T0000",
        data_end_date="19900103T2300",
        bridge_start_date="19900102T0000",
        bridge_end_date="19900102T2300",
        startdate="19900101T0000",
        enddate="19900102T2300",
        var=["tcc"],
        level=None,
        engine="z3fdb",
    )
    reader = Z3FDBDatasetReader(data)
    ds = reader._read(data)
    assert "time" in ds.dims
