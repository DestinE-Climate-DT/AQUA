"""Network- and data-backed console workflow tests."""

import os

import pytest

from aqua.core.util import dump_yaml

MACHINE = "github"

pytestmark = [pytest.mark.aqua, pytest.mark.console]


def test_console_drop(tmp_path, set_home, run_aqua, run_aqua_console_with_input):
    """Exercise DROP through the console with a remotely activated catalog."""
    mydir = str(tmp_path)
    set_home(mydir)

    run_aqua(["install", MACHINE])
    run_aqua(["add", "ci", "--repository", "DestinE-Climate-DT/Climate-DT-catalog"])

    drop_test = os.path.join(mydir, "faketrip.yaml")
    dump_yaml(
        drop_test,
        {
            "target": {"resolution": "r200", "frequency": "monthly", "catalog": "ci"},
            "paths": {"outdir": os.path.join(mydir, "drop_test"), "tmpdir": os.path.join(mydir, "tmp")},
            "options": {"loglevel": "INFO"},
            "data": {"IFS": {"test-tco79": {"long": {"vars": "2t"}}}},
        },
    )

    # run DROP and verify that at least one file exist
    # fmt: off
    run_aqua(
        [
            "drop", "--config", drop_test, "-w", "1", "-d",
            "--rebuild",
            "--startdate", "2020-01-01",
            "--enddate", "2020-03-31",
            "--driver", "netcdf",
        ]
    )
    # fmt: on
    path = os.path.join(
        os.path.join(mydir, "drop_test"),
        "ci/IFS/test-tco79/r1/r200/monthly/mean/global/2t_ci_IFS_test-tco79_r1_r200_monthly_mean_global_202002.nc",
    )
    assert os.path.isfile(path), f"File not found: {path}"

    run_aqua(
        [
            "drop",
            "--config",
            drop_test,
            "-w",
            "1",
            "-d",
            "--startdate",
            "2020-01-01",
            "--enddate",
            "2020-03-31",
            "--rebuild",
            "--stat",
            "min",
            "--driver",
            "zarr",
        ]
    )

    monthly_path = os.path.join(
        os.path.join(mydir, "drop_test"),
        "ci/IFS/test-tco79/r1/r200/monthly/min/global/2t_ci_IFS_test-tco79_r1_r200_monthly_min_global_202002.zarr",
    )
    assert os.path.exists(monthly_path), f"Monthly Zarr store not found: {monthly_path}"

    outdir_monitoring = os.path.join(mydir, "drop_test")
    run_aqua(
        [
            "drop",
            "--config",
            drop_test,
            "-w",
            "1",
            "-d",
            "--monitoring",
            "--startdate",
            "2020-01-01",
            "--enddate",
            "2020-03-31",
            "--rebuild",
            "--regrid_first",
        ]
    )

    stats_files = [f for f in os.listdir(outdir_monitoring) if f.startswith("drop_stats_") and f.endswith(".txt")]
    assert len(stats_files) >= 1, f"Expected at least one stats file in {outdir_monitoring}, found: {stats_files}"

    run_aqua(
        [
            "drop",
            "--config",
            drop_test,
            "-w",
            "1",
            "-d",
            "--startdate",
            "2020-01-01",
            "--enddate",
            "2020-01-31",
            "--catalog-entry",
            "no",
        ]
    )

    run_aqua(
        [
            "drop",
            "--config",
            drop_test,
            "-w",
            "1",
            "-d",
            "--startdate",
            "2020-01-01",
            "--enddate",
            "2020-01-31",
            "--catalog-entry",
            "only",
        ]
    )

    run_aqua(
        [
            "drop",
            "--outdir",
            os.path.join(mydir, "drop_test"),
            "-w",
            "1",
            "-d",
            "--catalog",
            "ci",
            "--model",
            "IFS",
            "--exp",
            "test-tco79",
            "--source",
            "long",
            "--var",
            "2t",
            "--resolution",
            "r200",
            "--frequency",
            "monthly",
            "--startdate",
            "2020-01-01",
            "--enddate",
            "2020-01-31",
            "--catalog-entry",
            "no",
        ]
    )

    run_aqua_console_with_input(["uninstall"], "yes")
