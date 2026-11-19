"""
Shared fixtures for AQUA test suite.
These fixtures use scope="session" to retrieve data once and share across all tests.
Reference: https://docs.pytest.org/en/stable/reference/fixtures.html
"""

import glob
import os
import shutil
import subprocess
import sys
import tempfile
import warnings
from functools import cache
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock

import matplotlib
import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr
from utils_tests import LOGLEVEL, make_private_config

from aqua import Reader
from aqua.core.backend.backend import Backend
from aqua.core.intake_drivers.fdb.openers import gsv_source
from aqua.core.intake_drivers.fdb.openers import z3fdb_opener as z
from aqua.core.lock import SafeFileLock
from aqua.core.util import dump_yaml

matplotlib.use("Agg")  # Non-interactive backend

import matplotlib.pyplot as plt

plt.ioff()  # Turn off interactive mode explicitly

STAC_CATALOGUE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "stac_catalogue")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

# entries a test run may legitimately create in the repository root (reports, caches)
ALLOWED_NEW_ROOT_ENTRIES = {".coverage", "junit.xml", "coverage.xml", "aqua_pytest.log", ".pytest_cache", "__pycache__"}


def _ensure_stac_catalogue():
    """Generate the example STAC catalogue if it is missing or older than the generator or the NetCDF fixtures.

    It runs on the controller before collection, because the catalog tests browse the `stac` sources at import.
    The lock and the atomic writes of the generator keep concurrent pytest invocations safe.
    """
    models = os.path.join(REPO_ROOT, "AQUA_tests", "models")
    if not os.path.isdir(models):
        return
    catalog = os.path.join(STAC_CATALOGUE_DIR, "catalog.json")
    generator = os.path.join(STAC_CATALOGUE_DIR, "generate_catalog.py")
    with SafeFileLock(os.path.join(STAC_CATALOGUE_DIR, ".generation.lock"), loglevel=LOGLEVEL):
        inputs = [generator, *glob.glob(os.path.join(models, "**", "*.nc"), recursive=True)]
        if os.path.exists(catalog) and os.path.getmtime(catalog) >= max(os.path.getmtime(path) for path in inputs):
            return
        if shutil.which("ncdump") is None:
            warnings.warn("ncdump not found: the STAC test catalogue is not generated", stacklevel=2)
            return
        subprocess.run([sys.executable, generator], check=True)


@pytest.fixture(scope="session")
def stac_catalogue_dir():
    """Return the example STAC catalogue prepared before test collection."""
    if not os.path.exists(os.path.join(STAC_CATALOGUE_DIR, "catalog.json")):
        pytest.skip("STAC catalogue not generated: AQUA_tests/models or ncdump is not available")
    return STAC_CATALOGUE_DIR


@pytest.fixture
def aqua_caplog(caplog):
    """`caplog` for the AQUA loggers.

    They do not propagate to the root logger, where pytest collects the records, so the capture handler is
    attached to the loggers passed to the returned function, for the duration of the test.
    """
    attached = []

    def capture(*loggers):
        for logger in loggers:
            if caplog.handler not in logger.handlers:
                logger.addHandler(caplog.handler)
                attached.append(logger)
        return caplog

    yield capture
    for logger in attached:
        logger.removeHandler(caplog.handler)


@pytest.fixture(autouse=True)
def _close_figures():
    """Close the matplotlib figures left open by a test."""
    yield
    plt.close("all")


class DummyBackend(Backend):
    """Dummy backend implementing abstract methods for testing."""

    def retrieve(self, *args, **kwargs):
        pass

    def retrieve_plain(self, *args, **kwargs):
        pass

    def log_history(self, data, *args, **kwargs):
        return data


@pytest.fixture(scope="session")
def dummy_backend():
    """Return a minimal Backend instance to test the shared Backend methods."""
    return DummyBackend()


# ======================================================================
# Session set-up: private AQUA configuration, controller-only checks
# ======================================================================
def pytest_configure(config):
    """
    Runs once at session start on controller and workers with xdist.
    The controller builds a private AQUA configuration (see utils_tests.config) and every process
    exports it as AQUA_CONFIG before collection, so that no test touches the installed one.
    Generate STAC JSON on the controller before catalog discovery during collection.
    Set per-worker TMPDIR to avoid CDO/temp contention in parallel runs.
    """
    workerinput = getattr(config, "workerinput", None)

    if workerinput is None:
        try:
            config._aqua_private_config = make_private_config()
        except (FileNotFoundError, KeyError):
            config._aqua_private_config = None  # no installed AQUA: tests will fail on their own
        config._root_entries = set(os.listdir(REPO_ROOT))
    else:
        config._aqua_private_config = workerinput.get("aqua_private_config")

    if config._aqua_private_config:
        os.environ["AQUA_CONFIG"] = config._aqua_private_config

    if workerinput is None:
        _ensure_stac_catalogue()  # on the controller, before xdist starts the workers

    if workerinput is not None:
        worker_id = workerinput.get("workerid", "master")
        # macOS default tmpdir is long: keep it short to fit the AF_UNIX socket path limit (~103 chars)
        base = "/tmp" if sys.platform == "darwin" else None
        os.environ["TMPDIR"] = tempfile.mkdtemp(prefix=f"aqua_pytest_{worker_id}_", dir=base)


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node):
    """xdist controller hook: give the workers the private configuration built by the controller."""
    node.workerinput["aqua_private_config"] = node.config._aqua_private_config


def pytest_sessionfinish(session, exitstatus):
    """
    Runs once at the end of the entire test session, on the controller only (after all workers
    have finished, so no file that other workers still need is deleted).
    Remove the private AQUA configuration and fail the session if tests left new files in the
    repository root (e.g. weights, areas or output directories written in the current directory).
    """
    config = session.config
    if getattr(config, "workerinput", None) is not None:
        return

    if config._aqua_private_config:
        shutil.rmtree(config._aqua_private_config, ignore_errors=True)

    leftovers = sorted(
        entry
        for entry in set(os.listdir(REPO_ROOT)) - config._root_entries
        if entry not in ALLOWED_NEW_ROOT_ENTRIES and not entry.startswith(".coverage.")
    )
    if leftovers:
        config.get_terminal_writer().line(f"\nTests left new files in the repository root: {leftovers}", red=True)
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


# ======================================================================
# xdist: log worker -> test mapping (per test start/finish) for hang debugging
# Log file: .pytest_cache/xdist_worker_tests.log
# ======================================================================


def _get_xdist_log_path(config):
    """Resolve the path for the xdist worker log file."""
    root = getattr(config, "rootdir", None)
    if not root:
        return None
    # Handle pytest's distinct path objects if necessary
    root_path = root.path if hasattr(root, "path") else root
    return os.path.join(str(root_path), ".pytest_cache", "xdist_worker_tests.log")


def _log_xdist_event(config, event, nodeid, outcome=None):
    """Append a structured log entry for xdist workers."""
    worker_input = getattr(config, "workerinput", None)
    if not worker_input:
        return

    worker_id = worker_input.get("workerid", "unknown")
    log_path = _get_xdist_log_path(config)

    if not log_path:
        return

    # Construct the log line
    line = f"{worker_id}\t{event}\t{nodeid}"
    if outcome:
        line += f"\t{outcome}"

    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        # Append line with flush to ensure it's written immediately
        with open(log_path, "a") as f:
            f.write(f"{line}\n")
            f.flush()
    except OSError:
        pass  # Best effort logging


def pytest_sessionstart(session):
    """Clear the xdist worker log on the controller node."""
    if getattr(session.config, "workerinput", None) is not None:
        return  # only the controller clears; workers append
    log_path = _get_xdist_log_path(session.config)
    if log_path and os.path.exists(log_path):
        try:
            os.remove(log_path)
        except OSError:
            pass


def pytest_runtest_setup(item):
    """Log test start event."""
    _log_xdist_event(item.config, "started", item.nodeid)


def pytest_runtest_makereport(item, call):
    """Log test finish event with outcome, including errors in fixture setup and teardown."""
    if call.when == "call" or call.excinfo:
        outcome = "failed" if call.excinfo else "passed"
        _log_xdist_event(item.config, "finished" if call.when == "call" else call.when, item.nodeid, outcome)


# ===================== FDB boundaries =====================
@pytest.fixture(scope="session")
def fdb_grib_message():
    """Encode grid metadata in memory with ecCodes; no test-data files are written."""
    ecc = pytest.importorskip("eccodes")

    @cache
    def encode(grid_type="healpix", **keys):
        handle = ecc.codes_grib_new_from_samples("regular_ll_sfc_grib2")
        try:
            ecc.codes_set(handle, "gridType", grid_type)
            if grid_type == "healpix":
                keys = {"Nside": 1, "ordering": 0, "longitudeOfFirstGridPointInDegrees": 45, **keys}
            for key, value in keys.items():
                ecc.codes_set(handle, key, value)
            if grid_type == "healpix":
                ecc.codes_set_values(handle, np.arange(12 * keys["Nside"] ** 2))
            elif grid_type == "regular_ll":
                ecc.codes_set_values(handle, np.arange(ecc.codes_get(handle, "Ni") * ecc.codes_get(handle, "Nj")))
            return ecc.codes_get_message(handle)
        finally:
            ecc.codes_release(handle)

    return encode


@pytest.fixture
def z3fdb_store(monkeypatch, tmp_path, fdb_grib_message):
    """Replace only native FDB access; run real AQUA helpers and real Zarr/Dask I/O."""
    if not z.z3fdb_available:
        pytest.skip("z3fdb and pyfdb are required for the native axis definitions")
    monkeypatch.setenv("FDB_HOME", str(tmp_path))
    monkeypatch.delenv("FDB_CONFIG_FILE", raising=False)

    def install(shapes, *, message=None, values=None):
        shapes = shapes if isinstance(shapes, list) else [shapes]
        fdb = SimpleNamespace(calls=[], streams=[], stores=[], configs={})
        for name in ("hpc", "bridge"):
            config = os.path.join(tmp_path, name, "etc/fdb/config.yaml")
            os.makedirs(os.path.dirname(config), exist_ok=True)
            dump_yaml(config, {"type": "local", "name": name})
            fdb.configs[name] = config
        fdb.metadata = {"fdb_path": fdb.configs["hpc"], "fdb_path_bridge": fdb.configs["bridge"]}

        def make_builder(config):
            builder = Mock()
            builder.add_part.side_effect = lambda request, axes, extractor: fdb.calls.append(
                SimpleNamespace(request=request.copy(), axes=axes, config=config)
            )

            def build():
                shape = shapes[len(fdb.stores) % len(shapes)]
                axes = builder.add_part.call_args.args[1]
                chunks = [
                    size if axis.chunking == z.Chunking.WHOLE_AXIS else getattr(axis.chunking, "chunkShape", 1)
                    for axis, size in zip(axes, shape)
                ]
                chunks += list(shape[len(chunks) :])
                content = {"shape": shape, "dtype": "float32", "fill_value": 0} if values is None else {"data": values}
                array = zarr.create_array(store=zarr.storage.MemoryStore(), chunks=tuple(chunks), **content)
                fdb.stores.append(array.store)
                return array.store

            builder.build.side_effect = build
            return builder

        def retrieve(request):
            stream = BytesIO(fdb_grib_message() if message is None else message)
            fdb.streams.append(stream)
            return stream

        fdb.retrieve = Mock(side_effect=retrieve)
        monkeypatch.setattr(z, "SimpleStoreBuilder", Mock(side_effect=make_builder))
        monkeypatch.setattr(z.pyfdb, "FDB", Mock(return_value=Mock(retrieve=fdb.retrieve)))
        return fdb

    return install


@pytest.fixture
def gsv_boundary(monkeypatch):
    """Replace the external GSV retriever while retaining AQUA's real GSV planning."""
    if not gsv_source.gsv_available:
        pytest.skip("GSV is required to check shared time-request compatibility")
    sample = xr.Dataset(
        {"t": (("time", "ncells"), np.ones((1, 12)), {"GRIB_paramId": 130})}, coords={"time": [pd.Timestamp("20200101")]}
    )
    retrieve = Mock(return_value=sample)
    monkeypatch.setattr(gsv_source, "GSVRetriever", Mock(return_value=Mock(request_data=retrieve)))
    return retrieve


# ===================== Reader and Retrieve fixtures ===================
# ======================================================================
# IFS fixtures
# ======================================================================
@pytest.fixture(scope="session")
def ifs_tco79_short_reader():
    return Reader(model="IFS", exp="test-tco79", source="short", loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def ifs_tco79_short_data_2t(ifs_tco79_short_reader):
    return ifs_tco79_short_reader.retrieve(var="2t")


@pytest.fixture(scope="session")
def ifs_tco79_short_r100_reader():
    return Reader(model="IFS", exp="test-tco79", source="short", loglevel=LOGLEVEL, regrid="r100")


@pytest.fixture(scope="session")
def ifs_tco79_short_r100_data(ifs_tco79_short_r100_reader):
    return ifs_tco79_short_r100_reader.retrieve()


@pytest.fixture(scope="session")
def ifs_tco79_short_r200_reader():
    return Reader(model="IFS", exp="test-tco79", source="short", loglevel=LOGLEVEL, regrid="r200")


@pytest.fixture(scope="session")
def ifs_tco79_short_r200_data(ifs_tco79_short_r200_reader):
    return ifs_tco79_short_r200_reader.retrieve()


@pytest.fixture(scope="session")
def ifs_tco79_long_fixfalse_reader():
    return Reader(model="IFS", exp="test-tco79", source="long", fix=False, loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def ifs_tco79_long_areasfalse_fixfalse_reader():
    return Reader(model="IFS", exp="test-tco79", source="long", areas=False, fix=False, loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def ifs_tco79_long400_fixfalse_reader():
    # used through request.getfixturevalue in test_timstat.py
    return Reader(model="IFS", exp="test-tco79", source="long400", fix=False, loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def ifs_tco79_long_fixfalse_data(ifs_tco79_long_fixfalse_reader):
    return ifs_tco79_long_fixfalse_reader.retrieve(var=["2t", "ttr"])


@pytest.fixture(scope="session")
def ifs_tco79_long400_fixfalse_data(ifs_tco79_long400_fixfalse_reader):
    # used through request.getfixturevalue in test_timstat.py
    return ifs_tco79_long400_fixfalse_reader.retrieve(var=["2t", "ttr"])


@pytest.fixture(scope="session")
def ifs_tco79_long_reader():
    return Reader(model="IFS", exp="test-tco79", source="long", loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def ifs_tco79_long_data(ifs_tco79_long_reader):
    return ifs_tco79_long_reader.retrieve()


@pytest.fixture(scope="session")
def ifs_tco79_long_r100_reader():
    return Reader(model="IFS", exp="test-tco79", source="long", loglevel=LOGLEVEL, regrid="r100")


# ======================================================================
# FESOM fixtures
# ======================================================================
@pytest.fixture(scope="session")
def fesom_test_pi_original_2d_reader():
    return Reader(model="FESOM", exp="test-pi", source="original_2d", loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def fesom_test_pi_original_2d_data(fesom_test_pi_original_2d_reader):
    return fesom_test_pi_original_2d_reader.retrieve(var="tos")


@pytest.fixture(scope="session")
def fesom_test_pi_original_2d_r200_fixfalse_reader():
    return Reader(model="FESOM", exp="test-pi", source="original_2d", regrid="r200", fix=False, loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def fesom_test_pi_original_2d_r200_fixfalse_data(fesom_test_pi_original_2d_r200_fixfalse_reader):
    return fesom_test_pi_original_2d_r200_fixfalse_reader.retrieve()


# ======================================================================
# ICON fixtures
# ======================================================================
@pytest.fixture(scope="session")
def icon_test_healpix_short_reader():
    return Reader(model="ICON", exp="test-healpix", source="short", loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def icon_test_healpix_short_data(icon_test_healpix_short_reader):
    return icon_test_healpix_short_reader.retrieve(var="2t")


@pytest.fixture(scope="session")
def icon_test_r2b0_short_reader():
    return Reader(model="ICON", exp="test-r2b0", source="short", loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def icon_test_r2b0_short_data(icon_test_r2b0_short_reader):
    return icon_test_r2b0_short_reader.retrieve(var="t")


# ======================================================================
# NEMO fixtures
# ======================================================================
@pytest.fixture(scope="session")
def nemo_test_e_orca1_long_2d_reader():
    return Reader(model="NEMO", exp="test-eORCA1", source="long-2d", loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def nemo_test_e_orca1_long_2d_data(nemo_test_e_orca1_long_2d_reader):
    return nemo_test_e_orca1_long_2d_reader.retrieve(var="tos")


@pytest.fixture(scope="session")
def nemo_test_e_orca1_short_3d_reader():
    return Reader(model="NEMO", exp="test-eORCA1", source="short-3d", loglevel=LOGLEVEL, fix=False)


@pytest.fixture(scope="session")
def nemo_test_e_orca1_short_3d_data(nemo_test_e_orca1_short_3d_reader):
    return nemo_test_e_orca1_short_3d_reader.retrieve(var="avg_so")


# ======================================================================
# ERA5 fixtures
# ======================================================================
@pytest.fixture(scope="session")
def era5_hpz3_monthly_reader():
    return Reader(model="ERA5", exp="era5-hpz3", source="monthly", loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def era5_hpz3_monthly_data(era5_hpz3_monthly_reader):
    return era5_hpz3_monthly_reader.retrieve(var=["2t", "tprate", "q"])


@pytest.fixture(scope="session")
def era5_hpz3_monthly_r100_reader():
    return Reader(model="ERA5", exp="era5-hpz3", source="monthly", regrid="r100", loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def era5_hpz3_monthly_r100_data(era5_hpz3_monthly_r100_reader):
    return era5_hpz3_monthly_r100_reader.retrieve(var=["q"])
