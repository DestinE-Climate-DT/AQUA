import os

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from dask.distributed import Client, LocalCluster
from utils_tests import LOGLEVEL

from aqua import Reader
from aqua.core.configurer import ConfigContext
from aqua.core.intake_drivers.fdb.datatypes import Z3FDB
from aqua.core.intake_drivers.fdb.openers import open_gsv
from aqua.core.intake_drivers.fdb.openers.gsv_source import GSVSource, gsv_available
from aqua.core.intake_drivers.fdb.readers import Z3FDBDatasetReader

if not gsv_available:
    pytest.skip("Skipping GSV tests: FDB5 libraries not available", allow_module_level=True)

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
pytestmark = [pytest.mark.fdb, pytest.mark.xdist_group(name="dask_operations")]

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

FDB_HOME = "/app"
FDB_INFO_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "catgen")

# request of the d1 test data in the FDB: hourly tcc from 19900101T0000 to 19900102T0000
D1_REQUEST = {
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
}
# global mean of tcc (fraction) in the d1 test data, computed with eccodes from sample_test_data_d1.grib
D1_TCC_MEAN = {"1990-01-01T00": 0.6530221138649116, "1990-01-01T15": 0.6562109108708682, "1990-01-02T00": 0.6687973267265382}

# to enable for local testing on Lumi
if ConfigContext().get_machine() == "lumi":
    FDB_HOME = "/pfs/lustrep3/projappl/project_465000454/padavini/FDB-TEST"


@pytest.fixture(autouse=True)
def restore_fdb_environment(monkeypatch):
    """Reading from FDB sets FDB_HOME / FDB_CONFIG_FILE for the whole process: restore them after each test."""
    for name in ("FDB_HOME", "FDB_CONFIG_FILE"):
        if name in os.environ:
            monkeypatch.setenv(name, os.environ[name])
        else:
            monkeypatch.delenv(name, raising=False)


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
    @pytest.mark.parametrize(
        "bridge_end_date, metadata_key, chunk_type",
        [(None, "fdb_home", 0), ("complete", "fdb_home_bridge", 1)],
        ids=["hpc", "bridge"],
    )
    def test_gsv_constructor(self, bridge_end_date, metadata_key, chunk_type) -> None:
        """A source on HPC or entirely on the bridge has floored dates and the matching FDB home and chunk type."""
        source = GSVSource(
            DEFAULT_GSV_PARAMS["request"],
            "20080101",
            "20080101",
            timestep="h",
            chunks="S",
            var="167",
            bridge_end_date=bridge_end_date,
            metadata={metadata_key: FDB_HOME},
            engine="gsv",
        )
        assert (source.data_start_date, source.data_end_date) == ("20080101T0000", "20080101T0000")
        assert getattr(source, metadata_key.replace("fdb_home", "fdbhome")) == FDB_HOME
        assert np.all(source.chk_type == chunk_type)

    @pytest.mark.parametrize(
        "bridge_end_date, match",
        [
            (None, "Some data is on HPC but no local FDB path or FDB home"),
            ("complete", "Some data is on bridge but no bridge FDB path or FDB home"),
        ],
        ids=["hpc", "bridge"],
    )
    def test_gsv_constructor_raise(self, bridge_end_date, match) -> None:
        """Test raise for missing fdbhome"""
        with pytest.raises(ValueError, match=match):
            open_gsv(
                DEFAULT_GSV_PARAMS["request"],
                data_start_date="20080101",
                data_end_date="20080101",
                timestep="h",
                chunks="S",
                var="167",
                bridge_end_date=bridge_end_date,
                engine="gsv",
            )

    def test_gsv_constructor_raise_path_not_exists(self) -> None:
        """Test raise when fdbhome path is specified but does not exist"""
        with pytest.raises(FileNotFoundError, match="fdbhome path .* does not exist"):
            open_gsv(
                DEFAULT_GSV_PARAMS["request"],
                data_start_date="20080101",
                data_end_date="20080101",
                timestep="h",
                chunks="S",
                var="167",
                engine="gsv",
                metadata={"fdb_home": "/path/that/does/not/exist"},
            )

    def test_gsv_constructor_raise_bridge_path_not_exists(self) -> None:
        """Test raise when bridge path is specified but does not exist"""
        with pytest.raises(FileNotFoundError, match="fdbhome_bridge path .* does not exist"):
            open_gsv(
                DEFAULT_GSV_PARAMS["request"],
                data_start_date="20080101",
                data_end_date="20080101",
                timestep="h",
                chunks="S",
                var="167",
                bridge_end_date="complete",
                engine="gsv",
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
        assert list(dd.data_vars) == ["t"]
        assert dd["t"].GRIB_paramId == 130
        assert dd["t"].mean().values == pytest.approx(279.3509)

    # High-level, integrated test

    # z3fdb does not use GRIB_paramId, so no need to run it with this test
    def test_reader(self) -> None:
        """Simple test, to check that catalog access works and reads correctly"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=LOGLEVEL)
        data = reader.retrieve(startdate="20080101T1200", enddate="20080101T1200", var="t")
        assert data.t.GRIB_paramId == 130, "Wrong GRIB param in data"

    def test_reader_novar(self) -> None:
        """Simple test, to check that catalog access works and reads correctly, no var"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=LOGLEVEL)
        data = reader.retrieve()
        assert data.t.GRIB_paramId == 130, "Wrong GRIB param in data"

    @pytest.mark.parametrize("engine", ["gsv", "z3fdb"])
    def test_reader_xarray(self, engine) -> None:
        """Reading directly into xarray"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=LOGLEVEL, engine=engine)
        data = reader.retrieve()
        assert isinstance(data, xr.Dataset), "Does not return a Dataset"
        assert data.t.mean().data == pytest.approx(279.3509), "Field values incorrect"

    @pytest.mark.parametrize("engine", ["gsv", "z3fdb"])
    @pytest.mark.parametrize("config_fdb_type", ["dir", "file"])
    def test_reader_config_fdb(self, engine, config_fdb_type) -> None:
        """Test reading data with config_fdb passed to Reader as a directory or config file."""
        config = FDB_HOME if config_fdb_type == "dir" else f"{FDB_HOME}/etc/fdb/config.yaml"
        reader = Reader(model="IFS", exp="test-fdb", source="fdb", config_fdb=config, loglevel=LOGLEVEL, engine=engine)
        assert reader.backend.kwargs.get("config_fdb") == config
        data = reader.retrieve()
        assert isinstance(data, xr.Dataset), "Does not return a Dataset"
        assert data.t.mean().data == pytest.approx(279.3509), "Field values incorrect"

    def test_reader_paramid(self) -> None:
        """
        Reading with the variable paramid, we use '130' instead of 't'
        """

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=LOGLEVEL)
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

        reader = Reader(model="IFS", exp="test-fdb", source="fdb", loglevel=LOGLEVEL, engine="z3fdb")
        data = reader.retrieve(var="130")
        assert isinstance(data, xr.Dataset), "Does not return a Dataset"
        assert data.var130.mean().data == pytest.approx(279.3509), "Field values incorrect"
        data = reader.retrieve(var=130)  # test numeric argument
        assert data.var130.mean().data == pytest.approx(279.3509), "Field values incorrect"

    @pytest.mark.parametrize("engine", ["gsv", "z3fdb"])
    def test_reader_3d(self, engine) -> None:
        """Testing 3D access"""

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-levels", loglevel=LOGLEVEL, engine=engine)
        data = reader.retrieve()
        # coordinates read from levels key
        assert all(data.t.coords["plev"].data == [99999.0, 89999.0, 79999.0]), "Wrong coordinates from levels metadata key"
        # can read second level
        assert data.t.isel(plev=1).mean().values == pytest.approx(274.79095), "Field values incorrect"

        data = reader.retrieve(level=[900, 800])  # Read only two levels
        assert data.t.isel(plev=1).mean().values == pytest.approx(271.2092), "Field values incorrect"

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-nolevels", loglevel=LOGLEVEL, engine=engine)
        data = reader.retrieve()
        # coordinates read from levels key
        assert all(data.t.coords["plev"].data == [100000, 90000, 80000]), "Wrong level info"
        # can read second level
        assert data.t.isel(plev=1).mean().values == pytest.approx(274.79095), "Field values incorrect"

    @pytest.mark.parametrize("chunk_key", ["vertical", "level"])
    @pytest.mark.parametrize("engine", ["gsv", "z3fdb"])
    def test_reader_3d_chunks(self, engine, chunk_key) -> None:
        """Testing 3D access with vertical chunking"""

        reader = Reader(
            model="IFS",
            exp="test-fdb",
            source="fdb-levels",
            chunks={"time": "h", chunk_key: 1},
            loglevel=LOGLEVEL,
            engine=engine,
        )
        data = reader.retrieve()
        assert data.t.chunksizes["plev"] == (1, 1, 1), "Vertical dimension is not chunked correctly"

        # can read second level
        assert data.t.isel(plev=1).mean().values == pytest.approx(274.79095), "Field values incorrect"

        data = reader.retrieve(level=[900, 800])  # Read only two levels
        assert data.t.chunksizes["plev"] == (1, 1), "Subsetted vertical dimension is not chunked correctly"
        assert data.t.isel(plev=1).mean().values == pytest.approx(271.2092), "Field values incorrect"

        # Test custom chunk size with FixedSizeChunk (z3fdb) or vertical chunk size (gsv)
        reader2 = Reader(
            model="IFS",
            exp="test-fdb",
            source="fdb-levels",
            chunks={"time": "h", chunk_key: 2},
            loglevel=LOGLEVEL,
            engine=engine,
        )
        data_chunk2 = reader2.retrieve(level=[900, 800])
        assert data_chunk2.t.chunksizes["plev"] == (2,), "Fixed size vertical chunking is incorrect"

    @pytest.mark.parametrize(
        "engine, config_fdb_type, environment",
        [
            ("gsv", None, ("FDB_HOME", FDB_HOME)),
            ("z3fdb", None, None),  # z3fdb passes the config file of the FDB home, it sets no variable
            ("gsv", "dir", ("FDB_HOME", FDB_HOME)),
            ("gsv", "file", ("FDB_CONFIG_FILE", f"{FDB_HOME}/etc/fdb/config.yaml")),
        ],
    )
    def test_reader_bridge(self, monkeypatch, engine, config_fdb_type, environment) -> None:
        """
        Reading from a datasource using bridge: HPC and bridge chunks are read from the FDB set in the
        catalog metadata or in config_fdb, which is exported to the environment of the process.
        """
        monkeypatch.delenv("FDB_HOME", raising=False)
        monkeypatch.delenv("FDB_CONFIG_FILE", raising=False)
        config = {None: None, "dir": FDB_HOME, "file": f"{FDB_HOME}/etc/fdb/config.yaml"}[config_fdb_type]

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-bridge", config_fdb=config, loglevel=LOGLEVEL, engine=engine)
        data = reader.retrieve()
        # Test if the correct dates have been found
        assert "1990-01-01T00:00" in str(data.time[0].values)
        assert "1990-01-02T00:00" in str(data.time[-1].values)
        # Test if the data can actually be read and contain the expected values
        assert data.tcc.isel(time=0).values.mean() == pytest.approx(65.30221138649116)  # This is from HPC
        assert data.tcc.isel(time=15).values.mean() == pytest.approx(65.62109108718757)  # This is from the bridge
        assert data.tcc.isel(time=-1).values.mean() == pytest.approx(66.87973267265382)  # This is from HPC again
        if environment is None:
            assert "FDB_HOME" not in os.environ and "FDB_CONFIG_FILE" not in os.environ
        else:
            name, value = environment
            assert os.environ[name] == value

    # The auto option is not implemented for z3fdb
    def test_reader_auto(self) -> None:
        """
        Reading from a datasource using new operational schema and auto dates
        """

        reader = Reader(model="IFS", exp="test-fdb", source="fdb-auto", loglevel=LOGLEVEL, engine="gsv")
        data = reader.retrieve()
        # Test if the correct dates have been found
        assert "1990-01-01T00:00" in str(data.time[0].values)
        assert "1990-01-01T23:00" in str(data.time[-1].values)
        # Test if the data can actually be read and contain the expected values
        assert data.tcc.isel(time=0).values.mean() == pytest.approx(65.30221138649116)
        assert data.tcc.isel(time=-1).values.mean() == pytest.approx(66.79689864974151)

    def test_fdb_from_file(self) -> None:
        """
        Reading fdb dates from a file.
        First test with a file that contains both data and bridge dates.
        Second test with a file that contains only data dates.
        """

        # test the dummy_run situation for intake_gsv
        source = GSVSource(
            DEFAULT_GSV_PARAMS["request"],
            "20080101",
            "20080101",
            metadata={"fdb_info_file": os.path.join(FDB_INFO_DIR, "fdb_info_file.yaml")},
            loglevel=LOGLEVEL,
        )

        assert source.data_start_date == "19900101T0000"

        source = GSVSource(
            DEFAULT_GSV_PARAMS["request"],
            "20080101",
            "20080101",
            metadata={
                "fdb_home": FDB_HOME,
                "fdb_home_bridge": FDB_HOME,
                "fdb_info_file": os.path.join(FDB_INFO_DIR, "fdb_info_file.yaml"),
            },
            engine="gsv",
            loglevel=LOGLEVEL,
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
                "fdb_info_file": os.path.join(FDB_INFO_DIR, "fdb_info_hpc-only.yaml"),
            },
            engine="gsv",
            loglevel=LOGLEVEL,
        )

        assert source.data_start_date == "19900101T0000"
        assert source.data_end_date == "19900103T2300"
        assert (source.bridge_start_date, source.bridge_end_date) == (None, None)
        assert np.all(source.chk_type == 0)

    @pytest.mark.parametrize("engine", ["gsv", "z3fdb"])
    def test_reader_dask(self, engine) -> None:
        """
        Reading in parallel with a dask cluster
        LocalCluster is created with dashboard_address=None to avoid dashboard port conflicts under pytest-xdist
        """
        with LocalCluster(threads_per_worker=1, n_workers=2, dashboard_address=None) as cluster:
            with Client(cluster):
                reader = Reader(model="IFS", exp="test-fdb", source="fdb-bridge", loglevel=LOGLEVEL, engine=engine)
                data = reader.retrieve()
                # Test if the correct dates have been found
                assert "1990-01-01T00:00" in str(data.time[0].values)
                assert "1990-01-02T00:00" in str(data.time[-1].values)
                # Test if the data can actually be read and contain the expected values
                assert data.tcc.isel(time=0).mean().compute().item() == pytest.approx(65.30221138649115)
                assert data.tcc.isel(time=-1).mean().compute().item() == pytest.approx(66.8797378540039)


@pytest.mark.parametrize("bridge_end_date", ["19900101T1800", "19900102T0000"], ids=["hpc-bridge-hpc", "hpc-bridge-to-end"])
def test_z3fdb_reader_bridge_periods(bridge_end_date) -> None:
    """The z3fdb reader splits the request in HPC and bridge periods and joins them without gaps or duplicates."""
    data = Z3FDB(
        request=D1_REQUEST,
        metadata={"fdb_home": FDB_HOME, "fdb_home_bridge": FDB_HOME},
        data_start_date="19900101T0000",
        data_end_date="19900102T0000",
        bridge_start_date="19900101T1200",
        bridge_end_date=bridge_end_date,
        startdate="19900101T0000",
        enddate="19900102T0000",
        var=["tcc"],
        level=None,
        engine="z3fdb",
    )
    ds = Z3FDBDatasetReader(data).read()

    assert list(ds.data_vars) == ["tcc"]
    assert ds.time.values.tolist() == pd.date_range("1990-01-01T00", "1990-01-02T00", freq="h").values.tolist()
    for time, mean in D1_TCC_MEAN.items():
        assert ds["tcc"].sel(time=time).mean().values == pytest.approx(mean)


@pytest.mark.parametrize(
    "bridge_end_date, metadata_key, variable",
    [
        (None, "fdb_path", "FDB_CONFIG_FILE"),
        ("complete", "fdb_path_bridge", "FDB_CONFIG_FILE"),
        ("complete", "fdb_home_bridge", "FDB_HOME"),
    ],
    ids=["hpc-config-file", "bridge-config-file", "bridge-home"],
)
def test_gsv_read_exports_fdb_environment(monkeypatch, bridge_end_date, metadata_key, variable) -> None:
    """HPC and bridge reads export the FDB of the metadata (home or config file) and only that one to the process."""
    monkeypatch.delenv("FDB_HOME", raising=False)
    monkeypatch.delenv("FDB_CONFIG_FILE", raising=False)
    value = f"{FDB_HOME}/etc/fdb/config.yaml" if variable == "FDB_CONFIG_FILE" else FDB_HOME
    source = GSVSource(
        **{**DEFAULT_GSV_PARAMS, "bridge_end_date": bridge_end_date},
        metadata={metadata_key: value},
        engine="gsv",
        loglevel=LOGLEVEL,
    )
    data = source.read()

    assert data["t"].mean().values == pytest.approx(279.3509)
    assert os.environ[variable] == value
    assert ({"FDB_HOME", "FDB_CONFIG_FILE"} - {variable}).isdisjoint(os.environ)
