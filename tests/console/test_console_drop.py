"""Data-backed console workflow tests."""

import glob
import os

import pytest

from aqua.core.util import dump_yaml, load_yaml

MACHINE = "github"

pytestmark = pytest.mark.console


def test_console_drop(tmp_path, set_home, run_aqua, run_aqua_console_with_input, fake_github):
    """Exercise DROP through the console with a catalog added from the (local stand-in of the) GitHub repository."""
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

    outdir = os.path.join(mydir, "drop_test")

    def stats_files():
        return [f for f in os.listdir(outdir) if f.startswith("drop_stats_") and f.endswith(".txt")]

    def data_files(*parts):
        pattern = os.path.join(outdir, *parts, "**")
        return sorted(path for path in glob.glob(pattern, recursive=True) if os.path.isfile(path))

    stats_before = len(stats_files())  # the two runs above wrote their own stats files
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
    assert len(stats_files()) == stats_before + 1, "The monitoring run must write one more stats file"

    # --catalog-entry controls the catalog entry only: `no` generates the data but not the entry, `only` the opposite
    catalog_file = os.path.join(mydir, ".aqua", "catalogs", "ci", "catalog", "IFS", "test-tco79.yaml")
    daily = [
        "drop",
        "--config",
        drop_test,
        "-w",
        "1",
        "-d",
        "--frequency",
        "daily",
        "--startdate",
        "2020-02-01",
        "--enddate",
        "2020-02-03",
    ]
    assert "r200-daily" not in load_yaml(catalog_file)["sources"]

    run_aqua(daily + ["--catalog-entry", "no"])
    generated = data_files("ci", "IFS", "test-tco79", "r1", "r200", "daily")
    assert generated, "--catalog-entry no must generate the data"
    assert "r200-daily" not in load_yaml(catalog_file)["sources"]

    for path in generated:
        os.remove(path)  # so that a run which wrote the data again could not pass unnoticed
    run_aqua(daily + ["--catalog-entry", "only"])
    assert "r200-daily" in load_yaml(catalog_file)["sources"]
    assert data_files("ci", "IFS", "test-tco79", "r1", "r200", "daily") == []  # `only` generates no data

    # CLI-only mode, without a configuration file: the statistic that no run has produced yet is generated
    run_aqua(
        [
            "drop",
            "--outdir",
            outdir,
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
            "--stat",
            "max",
            "--startdate",
            "2020-02-01",
            "--enddate",
            "2020-02-29",
            "--catalog-entry",
            "no",
        ]
    )
    assert os.path.isfile(
        os.path.join(
            outdir,
            "ci/IFS/test-tco79/r1/r200/monthly/max/global/2t_ci_IFS_test-tco79_r1_r200_monthly_max_global_202002.nc",
        )
    )

    run_aqua_console_with_input(["uninstall"], "yes")
