"""Tests for remote access to the databridge through Polytope. They need the gsv package and network access."""

import pytest
from utils_tests import LOGLEVEL

from aqua import Reader
from aqua.core.intake_drivers.fdb.openers.polytope_source import gsv_available

if not gsv_available:
    pytest.skip("Skipping Polytope tests: gsv and polytope cannot be imported", allow_module_level=True)

pytestmark = [pytest.mark.polytope, pytest.mark.network]


def test_reader_polytope() -> None:
    """
    Reading from a remote databridge using polytope
    """

    reader = Reader(
        catalog="climatedt-gen2",
        model="IFS-NEMO-5km",
        exp="projections-ssp370",
        source="hourly-hpz7-sfc",
        startdate="20210101T0000",
        enddate="20210101T2300",
        loglevel=LOGLEVEL,
        engine="polytope",
        chunks="h",
    )
    data = reader.retrieve(var="2t")
    assert data.isel(time=1)["2t"].mean().values == pytest.approx(285.1543)


def test_reader_stac_polytope() -> None:
    """
    Reading from a remote databridge using polytope
    """
    reader = Reader(
        catalog="climatedt-gen2",
        model="IFS-FESOM-10km",
        exp="story-nudging-Tplus2K",
        source="hourly-hpz7-sfc",
        loglevel=LOGLEVEL,
        engine="polytope",
        areas=False,
    )
    data = reader.retrieve(var="2t")
    assert data.isel(time=20)["2t"].mean().values == pytest.approx(286.9244)


def test_reader_polytope_mn5() -> None:
    """
    Reading from mn5 databridge using polytope
    """
    reader = Reader(
        catalog="climatedt-gen2",
        model="IFS-NEMO-5km",
        exp="baseline-hist",
        source="hourly-hpz7-sfc",
        startdate="19900101T0000",
        enddate="19910101T0025",
        loglevel=LOGLEVEL,
        engine="polytope",
        areas=False,
    )
    data = reader.retrieve(var="2t")
    assert "databridge" in reader.backend.kwargs
    assert reader.backend.kwargs["databridge"] == "mn5"
    assert data.isel(time=20)["2t"].values[0] == pytest.approx(301.0878448486328)
